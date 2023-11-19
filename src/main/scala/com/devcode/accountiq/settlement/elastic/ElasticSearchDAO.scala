package com.devcode.accountiq.settlement.elastic

import com.devcode.accountiq.settlement.elastic.reports.batch.BatchSalesToPayoutReportRow
import com.devcode.accountiq.settlement.elastic.reports.merchant.MerchantPaymentTransactionsReportRow
import com.devcode.accountiq.settlement.elastic.reports.settlement.SettlementDetailReportRow
import com.sksamuel.elastic4s.{ElasticClient, Indexable}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.zio.instances._
import zio._
import zio.json._

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import com.sksamuel.elastic4s.ElasticDsl._


class ElasticSearchDAO[T: Indexable](client: ElasticClient, indexName: String) {

  implicit lazy val global = ExecutionContext.fromExecutorService(
    Executors.newWorkStealingPool(10)
  )

  def findAll() = {
    val limit = 100
    client.execute(
      search(indexName)
        .query(matchAllQuery())
        .limit(limit)
        .seqNoPrimaryTerm(true)
    )
  }

  def createIndices() = client.execute {
    createIndex(indexName)
  }

  def add(jsonEntity: T) = client.execute {
    indexInto(indexName)
      .doc(jsonEntity)
  }

  def addBulk(jsonEntities: List[T]) =
    client.execute {
      bulk {
        jsonEntities.map { dm =>
          indexInto(indexName).doc(dm)
        }
      }
    }

}

case class ESDoc(doc: Map[String, String])

object ESDoc {
  implicit val formatter: Indexable[ESDoc] = (t: ESDoc) => t.doc.toJson
  val fileIdField = "AIQ_Filename"

  def parseESDocs(rows: List[List[String]], fileId: String): List[ESDoc] = {
    rows match {
      case _ :: Nil => List()
      case Nil => List()
      case headerRow :: dataRows => dataRows.map(row => headerRow.zip(row)).map(_.toMap).map(_ + (fileIdField -> fileId)).map(ESDoc(_))
    }
  }
}

object ElasticSearchDAO {

  val liveESDoc: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[ESDoc]] = ZLayer.fromFunction(create _)

  def create(client: ElasticClient): ElasticSearchDAO[ESDoc] = {
    val indexName = "raw_settlement_merchant_reports"
    new ElasticSearchDAO[ESDoc](client, indexName)
  }

  val liveBatchSalesToPayout: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[BatchSalesToPayoutReportRow]] = ZLayer.fromFunction(createBatchSalesToPayout _)

  def createBatchSalesToPayout(client: ElasticClient): ElasticSearchDAO[BatchSalesToPayoutReportRow] = {
    val indexName = "batch_settlement_merchant_reports"
    new ElasticSearchDAO[BatchSalesToPayoutReportRow](client, indexName)
  }

  val liveSettlementDetail: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[SettlementDetailReportRow]] = ZLayer.fromFunction(createSettlementDetail _)

  def createSettlementDetail(client: ElasticClient): ElasticSearchDAO[SettlementDetailReportRow] = {
    val indexName = "detail_settlement_merchant_reports"
    new ElasticSearchDAO[SettlementDetailReportRow](client, indexName)
  }

  val liveMerchantPaymentTransactions: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[MerchantPaymentTransactionsReportRow]] = ZLayer.fromFunction(createMerchantPaymentTransactions _)

  def createMerchantPaymentTransactions(client: ElasticClient): ElasticSearchDAO[MerchantPaymentTransactionsReportRow] = {
    val indexName = "merchant_payment_transactions_reports"
    new ElasticSearchDAO[MerchantPaymentTransactionsReportRow](client, indexName)
  }

}
