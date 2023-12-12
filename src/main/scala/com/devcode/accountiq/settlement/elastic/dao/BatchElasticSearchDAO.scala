package com.devcode.accountiq.settlement.elastic.dao

import com.devcode.accountiq.settlement.elastic.reports.batch.{BatchSalesToPayoutPaidOutReportRow, BatchSalesToPayoutReportRow, BatchSalesToPayoutReportSummaryRow}
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import zio._
import zio.json._
import zio.json.ast.Json
import com.sksamuel.elastic4s._

import scala.util.{Failure, Success, Try}


class BatchElasticSearchDAO(esClient: ElasticClient)
  extends ElasticSearchDAO[BatchSalesToPayoutReportRow] {

  val client: ElasticClient = esClient
  val indexName = "batch_settlement_merchant_reports"
  val mapping: MappingDefinition = BatchSalesToPayoutReportRow.mapping
  val reader: HitReader[BatchSalesToPayoutReportRow] = IndexableHitReader
  val indexable: Indexable[BatchSalesToPayoutReportRow] = BatchSalesToPayoutReportRow.formatter
  val dateFieldName: String = "payoutDate"
  val refIdFieldName: String = "refId"

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

}

object BatchElasticSearchDAO {
  val live: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[BatchSalesToPayoutReportRow]] =
    ZLayer.fromFunction(createBatchSalesToPayout _)
  private def createBatchSalesToPayout(client: ElasticClient): ElasticSearchDAO[BatchSalesToPayoutReportRow] =
    new BatchElasticSearchDAO(client)
}
