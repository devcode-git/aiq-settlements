package com.devcode.accountiq.settlement.elastic.dao

import com.devcode.accountiq.settlement.elastic.reports.settlement.SettlementDetailReportRow
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import zio._
import zio.json._
import zio.json.ast.Json
import com.sksamuel.elastic4s._

import scala.util.{Failure, Success, Try}


class SettlementElasticSearchDAO(esClient: ElasticClient)
  extends ElasticSearchDAO[SettlementDetailReportRow] {

  val client: ElasticClient = esClient
  val indexName = "detail_settlement_merchant_reports"
  val mapping: MappingDefinition = SettlementDetailReportRow.mapping
  val reader: HitReader[SettlementDetailReportRow] = IndexableHitReader
  val indexable: Indexable[SettlementDetailReportRow] = SettlementDetailReportRow.formatter
  val dateFieldName: String = "creationDate"
  val refIdFieldName: String = "refId"

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

}

object SettlementElasticSearchDAO {
  val live: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[SettlementDetailReportRow]] =
    ZLayer.fromFunction(createSettlementDetail _)
  private def createSettlementDetail(client: ElasticClient): ElasticSearchDAO[SettlementDetailReportRow] =
    new SettlementElasticSearchDAO(client)
}
