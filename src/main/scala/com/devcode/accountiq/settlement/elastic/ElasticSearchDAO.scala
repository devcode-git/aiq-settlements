package com.devcode.accountiq.settlement.elastic

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.zio.instances._
import zio._
import zio.json.ast.Json

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

class ElasticSearchDAO(client: ElasticClient) {

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

  def createIndices() =
    client.execute {
      createIndex(indexName)
    }

  def add(jsonEntity: String) = {
    client.execute {
      indexInto(indexName).doc(jsonEntity)
    }
  }

  def addBulk(jsonEntities: List[String]) =
    client.execute {
      bulk {
        jsonEntities.map { dm =>
          indexInto(indexName).doc(dm)
        }
      }
    }

}

object ElasticSearchDAO {

  val live: ZLayer[ElasticClient, Nothing, ElasticSearchDAO] = ZLayer.fromFunction(create _)

  def create(client: ElasticClient): ElasticSearchDAO = {
    new ElasticSearchDAO(client)
  }

}
