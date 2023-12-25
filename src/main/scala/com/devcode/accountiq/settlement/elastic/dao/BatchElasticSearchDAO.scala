package com.devcode.accountiq.settlement.elastic.dao

import com.devcode.accountiq.settlement.elastic.reports.batch.{BatchSalesToPayoutPaidOutReportRow, BatchSalesToPayoutReportRow, BatchSalesToPayoutReportSummaryRow}
import com.sksamuel.elastic4s.ElasticApi.{boolQuery, search, termQuery}
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import zio._
import zio.json._
import zio.json.ast.Json
import com.sksamuel.elastic4s.zio.instances._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.ElasticDsl._

import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}


class BatchElasticSearchDAO(esClient: ElasticClient)
  extends ElasticSearchDAO[BatchSalesToPayoutReportRow] {

  val client: ElasticClient = esClient
  val indexName = "batch_settlement_merchant_reports"
  val mapping: MappingDefinition = BatchSalesToPayoutReportRow.mapping
  val reader: HitReader[BatchSalesToPayoutReportRow] = IndexableHitReader
  val indexable: Indexable[BatchSalesToPayoutReportRow] = BatchSalesToPayoutReportRow.formatter
  val dateFieldName: String = "payoutDate"
  val refIdFieldName: String = ""

  implicit object IndexableHitReader extends HitReader[BatchSalesToPayoutReportRow] {
    override def read(hit: Hit): Try[BatchSalesToPayoutReportRow] =
      if (hit.isSourceEmpty) {
        Failure(new IllegalArgumentException(s"doc (id:${hit.id}) src is empty"))
      } else {
        val jsonVal = for {
          entityJson <- hit.sourceAsString.fromJson[Json]
          infoJson <- s"""{"_id": "${Some(hit.id)}", "version": {"_seq_no": ${hit.seqNo}, "_primary_term": ${hit.primaryTerm} } }""".fromJson[Json]
        } yield entityJson.merge(infoJson)

        jsonVal.flatMap { j =>
          j.asObject.flatMap(_.fields.find { case (k, _) => k == "status" }).flatMap(_._2.asString) match {
            case Some("summary") => j.as[BatchSalesToPayoutReportSummaryRow]
            case Some("paidOut") => j.as[BatchSalesToPayoutPaidOutReportRow]
            case Some(v) => Left(s"Not expected value `${v}` for status")
            case None => Left(s"Could not find status in json")
          }
        } match {
          case Right(j) => Success(j)
          case Left(e) => Failure(new IllegalArgumentException(
            s"failed to parse src ${hit.sourceAsString} errors: " + e
          ))
        }
      }
  }

  override def addBulk(jsonEntities: List[BatchSalesToPayoutReportRow]) = {
    // skipping the summary fields, looks like they do not needed for settlement reconciliation
    val paidOutReports = jsonEntities.collect {
      case r: BatchSalesToPayoutPaidOutReportRow => r
    }

    for {
      _ <- ZIO.logInfo("[BatchElasticSearchDAO] Executing bulk add")
      existingEntitiesRes <- this.findDuplicates(paidOutReports.map(r => (r.payoutDate, r.paymentMethod)))
      existingEntitiesSeq <- ZIO.attempt(existingEntitiesRes.result.map {
        case Success(v) => v
      })
      existingRefIds = existingEntitiesSeq.collect {
        case r: BatchSalesToPayoutPaidOutReportRow => (r.payoutDate, r.paymentMethod)
      }
      (existingEntities, newEntities) = paidOutReports.partition(e => existingRefIds.contains((e.payoutDate, e.paymentMethod)))
      _ <- ZIO.foreachDiscard(existingEntities) { ee =>
        ZIO.logInfo(s"[BatchElasticSearchDAO] Skipping... Batch report row with payoutDate `${ee.payoutDate}` and paymentMethod `${ee.paymentMethod}` already exists")
      }
      res <- super.addBulk(newEntities)
      _ <- logEntries(newEntities, (ee: BatchSalesToPayoutPaidOutReportRow) => s"[BatchElasticSearchDAO] Adding batch report row with payoutDate `${ee.payoutDate}` and paymentMethod `${ee.paymentMethod}`")
    } yield res
  }

  //TODO: combine with findByRefIds and refIdFieldName variable
  def findDuplicates(refIds: Seq[(LocalDateTime, String)]) = {
    client.execute(
      search(indexName)
        .query {
          boolQuery()
            .should(refIds.map(ref => termQuery("payoutDate", ref._1)))
            .should(refIds.map(ref => termQuery("paymentMethod", ref._2)))
            .minimumShouldMatch(1)
        }
        .seqNoPrimaryTerm(true)
        .limit(1500)
    ).map(r => r.map(v => v.safeTo[BatchSalesToPayoutReportRow](reader)))
  }

}

object BatchElasticSearchDAO {
  val live: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[BatchSalesToPayoutReportRow]] =
    ZLayer.fromFunction(createBatchSalesToPayout _)
  private def createBatchSalesToPayout(client: ElasticClient): ElasticSearchDAO[BatchSalesToPayoutReportRow] =
    new BatchElasticSearchDAO(client)
}
