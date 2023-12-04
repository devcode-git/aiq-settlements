package com.devcode.accountiq.settlement.elastic

import com.devcode.accountiq.settlement.elastic.reports.AIQField
import com.devcode.accountiq.settlement.elastic.reports.batch.{BatchSalesToPayoutReportRow, BatchSalesToPayoutReportSummaryRow}
import com.devcode.accountiq.settlement.elastic.reports.merchant.MerchantPaymentTransactionsReportRow
import com.devcode.accountiq.settlement.elastic.reports.settlement.SettlementDetailReportRow
import com.devcode.accountiq.settlement.util.DateUtil.LocalDateTimeConverter
import com.sksamuel.elastic4s.{ElasticClient, ElasticDate, Hit, HitReader, Indexable, Response}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.zio.instances._
import zio._
import zio.json._

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.indexes.admin.DeleteIndexResponse
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import zio.json.ast.Json

import scala.util.{Failure, Success, Try}
import java.time.LocalDateTime


class ElasticSearchDAO[T: Indexable](client: ElasticClient, indexName: String, reader: HitReader[T], mapping: MappingDefinition, dateField: String) {

  implicit object IndexableHitreader extends HitReader[Json] {
    override def read(hit: Hit): Try[Json] =
      if (hit.isSourceEmpty) {
        Failure(new IllegalArgumentException(s"doc (id:${hit.id}) src is empty"))
      } else {
        val jsonVal = for {
          entityJson <- hit.sourceAsString.fromJson[Json] // fromJson[BatchSalesToPayoutReportSummaryRow]
          infoJson <- s"""{"_id": "${Some(hit.id)}", "version": {"_seq_no": ${hit.seqNo}, "_primary_term": ${hit.primaryTerm} } }""".fromJson[Json]
        } yield entityJson.merge(infoJson)

        jsonVal match {
          case Right(j) => Success(j)
          case Left(e) => Failure(new IllegalArgumentException(
            s"failed to parse src ${hit.sourceAsString} errors: " + e
          ))
        }
      }
  }

//  implicit lazy val global = ExecutionContext.fromExecutorService(
//    Executors.newWorkStealingPool(10)
//  )

  def findAll() = {
    val limit = 100
    client.execute(
      search(indexName)
        .query(matchAllQuery())
        .limit(limit)
        .seqNoPrimaryTerm(true)
    )
  }


  def find(from: LocalDateTime,
                                     to: LocalDateTime,
                                     merchant: String,
                                     provider: String): ZIO[Any, Throwable, Response[IndexedSeq[Try[T]]]] = {
    client.execute(
      search(indexName)
        .query(
          boolQuery()
            .must(
              rangeQuery(dateField).gte(from.toElasticDate).lt(to.toElasticDate),
              termQuery(AIQField.merchant.toString, merchant),
              termQuery(AIQField.provider.toString, provider)
            )
        )
        .limit(1000)
        .seqNoPrimaryTerm(true)
    ).map(r => r.map(v => v.safeTo[T](reader)))
  }

  def recreateIndex() = for {
    _ <- client.execute(deleteIndex(this.indexName))
    _ <- client.execute(createIndex(indexName).mapping(mapping))
  } yield ()

  def addBulk(jsonEntities: List[T]) =
    client.execute {
      bulk {
        jsonEntities.map { dm =>
          indexInto(indexName).doc(dm)
        }
      }
    }

}

object ElasticSearchDAO {

  val liveBatchSalesToPayout: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[BatchSalesToPayoutReportRow]] = ZLayer.fromFunction(createBatchSalesToPayout _)

  private def createBatchSalesToPayout(client: ElasticClient): ElasticSearchDAO[BatchSalesToPayoutReportRow] = {
    val indexName = "batch_settlement_merchant_reports"
    new ElasticSearchDAO[BatchSalesToPayoutReportRow](client, indexName, BatchSalesToPayoutReportRow.IndexableHitreader, BatchSalesToPayoutReportRow.mapping, "payoutDate")
  }

  val liveSettlementDetail: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[SettlementDetailReportRow]] = ZLayer.fromFunction(createSettlementDetail _)

  private def createSettlementDetail(client: ElasticClient): ElasticSearchDAO[SettlementDetailReportRow] = {
    val indexName = "detail_settlement_merchant_reports"
    new ElasticSearchDAO[SettlementDetailReportRow](client, indexName, SettlementDetailReportRow.IndexableHitreader, SettlementDetailReportRow.mapping, "creationDate")
  }

  val liveMerchantPaymentTransactions: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[MerchantPaymentTransactionsReportRow]] = ZLayer.fromFunction(createMerchantPaymentTransactions _)

  private def createMerchantPaymentTransactions(client: ElasticClient): ElasticSearchDAO[MerchantPaymentTransactionsReportRow] = {
    val indexName = "merchant_payment_transactions_reports"
    new ElasticSearchDAO[MerchantPaymentTransactionsReportRow](client, indexName, MerchantPaymentTransactionsReportRow.IndexableHitreader, MerchantPaymentTransactionsReportRow.mapping, "booked")
  }

}
