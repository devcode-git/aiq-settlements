package com.devcode.accountiq.settlement.elastic.dao

import com.devcode.accountiq.settlement.elastic.reports.merchant.MerchantPaymentTransactionsReportRow
import zio._
import zio.json._
import zio.json.ast.Json
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.requests.bulk.BulkResponse
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition

import scala.util.{Failure, Success, Try}


class MerchantElasticSearchDAO(esClient: ElasticClient)
  extends ElasticSearchDAO[MerchantPaymentTransactionsReportRow] {

  val client: ElasticClient = esClient
  val indexName: String = "merchant_payment_transactions_reports"
  val mapping: MappingDefinition = MerchantPaymentTransactionsReportRow.mapping
  val reader: HitReader[MerchantPaymentTransactionsReportRow] = IndexableHitReader
  val indexable: Indexable[MerchantPaymentTransactionsReportRow] = MerchantPaymentTransactionsReportRow.formatter
  val dateFieldName: String = "booked"
  val refIdFieldName: String = "txRef"

  implicit object IndexableHitReader extends HitReader[MerchantPaymentTransactionsReportRow] {
    override def read(hit: Hit): Try[MerchantPaymentTransactionsReportRow] =
      if (hit.isSourceEmpty) {
        Failure(new IllegalArgumentException(s"doc (id:${hit.id}) src is empty"))
      } else {
        val jsonVal = for {
          entityJson <- hit.sourceAsString.fromJson[Json]
          infoJson <- s"""{"_id": "${Some(hit.id)}", "version": {"_seq_no": ${hit.seqNo}, "_primary_term": ${hit.primaryTerm} } }""".fromJson[Json]
        } yield entityJson.merge(infoJson)

        jsonVal.flatMap { j =>
          j.as[MerchantPaymentTransactionsReportRow]
        } match {
          case Right(j) => Success(j)
          case Left(e) => Failure(new IllegalArgumentException(
            s"failed to parse src ${hit.sourceAsString} errors: " + e
          ))
        }
      }
  }

  override def addBulk(jsonEntities: List[MerchantPaymentTransactionsReportRow]): Task[Response[BulkResponse]] = {
    val refIds = jsonEntities.map(_.txRef)
    for {
      _ <- ZIO.logInfo("[MerchantElasticSearchDAO] Executing bulk add")
      existingEntitiesRes <- this.findByRefIds(refIds)
      existingEntitiesSeq <- ZIO.attempt(existingEntitiesRes.result.map {
        case Success(v) => v
      })
      existingRefIds = existingEntitiesSeq.map(_.txRef)
      (existingEntities, newEntities) = jsonEntities.partition(e => existingRefIds.contains(e.txRef))
      _ <- ZIO.foreachDiscard(existingEntities) { ee =>
        ZIO.logInfo(s"[MerchantElasticSearchDAO] Skipping... Merchant report row with txRef `${ee.txRef}` already exists")
      }
      res <- super.addBulk(newEntities)
      _ <- logEntries(newEntities, (ee: MerchantPaymentTransactionsReportRow) => s"[MerchantElasticSearchDAO] Adding merchant report row with txRef `${ee.txRef}`")
    } yield res
  }

}

object MerchantElasticSearchDAO {
  val live: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[MerchantPaymentTransactionsReportRow]] =
    ZLayer.fromFunction(createMerchantPaymentTransactions _)
  private def createMerchantPaymentTransactions(client: ElasticClient): ElasticSearchDAO[MerchantPaymentTransactionsReportRow] =
    new MerchantElasticSearchDAO(client)
}
