package com.devcode.accountiq.settlement.elastic.dao

import com.devcode.accountiq.settlement.elastic.TransactionRow
import com.sksamuel.elastic4s.ElasticApi.{boolQuery, termQuery}
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import zio._
import zio.json._
import zio.json.ast.Json
import com.sksamuel.elastic4s._

import scala.util.{Failure, Success, Try}


class ReconcileElasticSearchDAO(esClient: ElasticClient)
  extends ElasticSearchDAO[TransactionRow] {

  val client: ElasticClient = esClient
  val indexName = "settlement_reconcile_transactions"
  val mapping: MappingDefinition = TransactionRow.mapping
  val reader: HitReader[TransactionRow] = IndexableHitReader
  val indexable: Indexable[TransactionRow] = TransactionRow.formatter
  val dateFieldName: String = "payoutDate"
  val refIdFieldName: String = ""

  implicit object IndexableHitReader extends HitReader[TransactionRow] {
    override def read(hit: Hit): Try[TransactionRow] =
      if (hit.isSourceEmpty) {
        Failure(new IllegalArgumentException(s"doc (id:${hit.id}) src is empty"))
      } else {
        val jsonVal = for {
          entityJson <- hit.sourceAsString.fromJson[Json]
          infoJson <- s"""{"_id": "${Some(hit.id)}", "version": {"_seq_no": ${hit.seqNo}, "_primary_term": ${hit.primaryTerm} } }""".fromJson[Json]
        } yield entityJson.merge(infoJson)

        jsonVal.flatMap { j =>
          j.as[TransactionRow]
        } match {
          case Right(j) => Success(j)
          case Left(e) => Failure(new IllegalArgumentException(
            s"failed to parse src ${hit.sourceAsString} errors: " + e
          ))
        }
      }
  }

}

object ReconcileElasticSearchDAO {
  val live: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[TransactionRow]] =
    ZLayer.fromFunction(create _)
  private def create(client: ElasticClient): ElasticSearchDAO[TransactionRow] =
    new ReconcileElasticSearchDAO(client)
}
