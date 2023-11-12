package com.devcode.accountiq.settlement.elastic

import com.sksamuel.elastic4s.{ElasticClient, Indexable}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.zio.instances._
import zio._
import zio.json._

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import com.sksamuel.elastic4s.ElasticDsl._

class ElasticSearchDAO[T: Indexable](client: ElasticClient) {

  implicit lazy val global = ExecutionContext.fromExecutorService(
    Executors.newWorkStealingPool(10)
  )
  val indexName = "test_settlement_merchant_reports"

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

object EntercashField extends Enumeration {
  type EntercashField = Value
  val String = Value("STRING")
  val Double = Value("DOUBLE")
  val Long = Value("LONG")
  val Date = Value("DATE")
}

val batchSalesToPayoutReportMapping = Map(
  "Status" -> EntercashField.String,
  "Sales" -> EntercashField.Long,
  "Refunds" -> EntercashField.Double,
  "Sales - Refunds" -> EntercashField.Long,
  "Pending" -> EntercashField.Long, //might be double, always zero
  "Payout Date" -> EntercashField.Date, //example: 03/08/2023
  "Payment Method" -> EntercashField.String,
  "Payment Method Description" -> EntercashField.String,
  "Sales Count" -> EntercashField.Long,
  "Refund Count" -> EntercashField.Long,
)

val settlementDetailReportMapping = Map(
  "Company Account" -> EntercashField.String,
  "Merchant Account" -> EntercashField.String,
  "Psp Reference" -> EntercashField.String,
  "Merchant Reference" -> EntercashField.Long,
  "Payment Method" -> EntercashField.String,
  "Creation Date" -> EntercashField.Date,
  "TimeZone" -> EntercashField.String,
  "Type" -> EntercashField.String,
  "Modification Reference" -> EntercashField.String,
  "Gross Currency" -> EntercashField.String,
  "Gross Debit (GC)" -> EntercashField.Double,
  "Gross Credit (GC)" -> EntercashField.Double,
  "Exchange Rate" -> EntercashField.Double,
  "Net Currency" -> EntercashField.String,
  "Net Debit (NC)" -> EntercashField.Double,
  "Net Credit (NC)" -> EntercashField.Double,
  "Commission (NC)" -> EntercashField.Double,
  "Markup (NC)" -> EntercashField.String, //not found
  "Scheme Fees (NC)" -> EntercashField.String, //not found
  "Interchange (NC)" -> EntercashField.String, //not found
  "Payment Method Variant" -> EntercashField.String,
  "Modification Merchant Reference" -> EntercashField.Long,
  "Batch Number" -> EntercashField.Long, //can be Int ?
  "Reserved4" -> EntercashField.String, //not found
  "Reserved5" -> EntercashField.String, //not found
  "Reserved6" -> EntercashField.String, //not found
  "Reserved7" -> EntercashField.String, //not found
  "Reserved8" -> EntercashField.String, //not found
  "Reserved9" -> EntercashField.String, //not found
  "Reserved10" -> EntercashField.String, //not found
  "Reserved10" -> EntercashField.String, //not found
  "Shopper Reference" -> EntercashField.Long
)

object ESDoc {
  implicit val formatter: Indexable[ESDoc] = (t: ESDoc) => t.doc.toJson
  val fileIdField = "processedFileId"

  def parseESDocs(rows: List[List[String]], fileId: String): List[ESDoc] = {
    rows match {
      case _ :: Nil => List()
      case Nil => List()
      case headerRow :: dataRows => dataRows.map(row => headerRow.zip(row)).map(_.toMap).map(_ + (fileIdField -> fileId)).map(ESDoc(_))
    }
  }
}

object ElasticSearchDAO {

  val live: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[ESDoc]] = ZLayer.fromFunction(create _)

  def create(client: ElasticClient): ElasticSearchDAO[ESDoc] = {
    new ElasticSearchDAO[ESDoc](client)
  }

}
