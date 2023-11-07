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

object ESDoc {
  implicit val formatter: Indexable[ESDoc] = (t: ESDoc) => t.doc.toJson
}

object ElasticSearchDAO {

  val live: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[ESDoc]] = ZLayer.fromFunction(create _)

  def create(client: ElasticClient): ElasticSearchDAO[ESDoc] = {
    new ElasticSearchDAO[ESDoc](client)
  }

}
