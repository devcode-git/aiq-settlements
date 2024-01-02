package com.devcode.accountiq.settlement.elastic.dao

import com.devcode.accountiq.settlement.elastic.reports.settlement.SettlementDetailReportRow
import com.sksamuel.elastic4s.ElasticApi.{boolQuery, termQuery}
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import zio._
import zio.json._
import zio.json.ast.Json
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.requests.searches.queries.compound.BoolQuery

import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}


class SettlementElasticSearchDAO(esClient: ElasticClient)
  extends ElasticSearchDAO[SettlementDetailReportRow] {

  val client: ElasticClient = esClient
  val indexName = "detail_settlement_merchant_reports"
  val mapping: MappingDefinition = SettlementDetailReportRow.mapping
  val reader: HitReader[SettlementDetailReportRow] = IndexableHitReader
  val indexable: Indexable[SettlementDetailReportRow] = SettlementDetailReportRow.formatter
  val dateFieldName: String = "creationDate"
  val refIdFieldName: String = "merchantReference"

  implicit object IndexableHitReader extends HitReader[SettlementDetailReportRow] {
    override def read(hit: Hit): Try[SettlementDetailReportRow] =
      if (hit.isSourceEmpty) {
        Failure(new IllegalArgumentException(s"doc (id:${hit.id}) src is empty"))
      } else {
        val jsonVal = for {
          entityJson <- hit.sourceAsString.fromJson[Json]
          infoJson <- s"""{"_id": "${Some(hit.id)}", "version": {"_seq_no": ${hit.seqNo}, "_primary_term": ${hit.primaryTerm} } }""".fromJson[Json]
        } yield entityJson.merge(infoJson)

        jsonVal.flatMap { j =>
          j.as[SettlementDetailReportRow]
        } match {
          case Right(j) => Success(j)
          case Left(e) => Failure(new IllegalArgumentException(
            s"failed to parse src ${hit.sourceAsString} errors: " + e
          ))
        }
      }
  }

  override def addBulk(jsonEntities: List[SettlementDetailReportRow]) = {
    val settlementReports: List[SettlementDetailReportRow] = jsonEntities.collect { case r: SettlementDetailReportRow => r }

    for {
      _ <- ZIO.logInfo("[SettlementElasticSearchDAO] Executing bulk addBulk")

      newSettlementReports <- removeSettlementDuplicates(settlementReports)
      newReports = newSettlementReports

      res <- super.addBulk(newReports)

      _ <- logEntries(newSettlementReports, (ee: SettlementDetailReportRow) => s"[SettlementElasticSearchDAO] Adding settlement report entry with refId `${ee.merchantReference}`")

    }  yield res
  }

  private def removeSettlementDuplicates(settlementReports: List[SettlementDetailReportRow]): Task[List[SettlementDetailReportRow]] = {
    val findQuery: BoolQuery = boolQuery()
      .should(settlementReports.map(_.merchantReference).map(id => termQuery(refIdFieldName, id)))
      .minimumShouldMatch(1)
    for {
      existingEntitiesRes <- this.findByQuery(findQuery)
      existingRefIds <- ZIO.attempt(existingEntitiesRes.collect {
        case v if v.isInstanceOf[SettlementDetailReportRow] => v.asInstanceOf[SettlementDetailReportRow].merchantReference
      })
      (existingEntities, newEntities) = settlementReports.partition(e => existingRefIds.contains(e.merchantReference))
      _ <- logEntries(existingEntities, (ee: SettlementDetailReportRow) => s"[SettlementElasticSearchDAO] Skipping... Settlement entry SettlementDetailReportRow with refId `${ee.merchantReference}` already exists")
    } yield newEntities
  }

  private def typeCreationDateQuery(tuples: List[(String, LocalDateTime)]): BoolQuery = {
    boolQuery().should(tuples.map { case (reportType, creationDate) =>
      boolQuery().should(boolQuery().should(termQuery("type", reportType)), boolQuery().should(termQuery("creationDate", creationDate))).minimumShouldMatch(1)
    })
  }

}

object SettlementElasticSearchDAO {
  val live: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[SettlementDetailReportRow]] =
    ZLayer.fromFunction(createSettlementDetail _)
  private def createSettlementDetail(client: ElasticClient): ElasticSearchDAO[SettlementDetailReportRow] =
    new SettlementElasticSearchDAO(client)
}
