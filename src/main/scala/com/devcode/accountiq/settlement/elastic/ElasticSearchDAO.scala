package com.devcode.accountiq.settlement.elastic

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.zio.instances._
import zio._

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

class ElasticSearchDAO(client: ElasticClient) {

  implicit lazy val global = ExecutionContext.fromExecutorService(
    Executors.newWorkStealingPool(10)
  )

  val indexName = "betsson_journal_entries"

  def findAll() = {
    val limit = 100
    client.execute(
      search(indexName)
        .query(matchAllQuery())
        .limit(limit)
        .seqNoPrimaryTerm(true)
    )
  }

}

object ElasticSearchDAO {

  val live: ZLayer[ElasticClient, Nothing, ElasticSearchDAO] = ZLayer.fromFunction(create _)

  def create(client: ElasticClient): ElasticSearchDAO = {
    new ElasticSearchDAO(client)
  }

}
