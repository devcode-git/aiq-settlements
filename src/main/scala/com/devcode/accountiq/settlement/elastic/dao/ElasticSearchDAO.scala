package com.devcode.accountiq.settlement.elastic.dao

import zio._
import zio.json._
import zio.json.ast.Json
import com.devcode.accountiq.settlement.elastic.reports.AIQField
import com.devcode.accountiq.settlement.util.DateUtil.LocalDateTimeConverter
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.requests.bulk.BulkResponse
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import com.sksamuel.elastic4s.requests.searches.queries.compound.BoolQuery
import com.sksamuel.elastic4s.zio.instances._

import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}


trait ElasticSearchDAO[T] {

  val client: ElasticClient
  val indexName: String
  val mapping: MappingDefinition
  val reader: HitReader[T]
  val indexable: Indexable[T]
  val dateFieldName: String
  val refIdFieldName: String

  implicit object JsonReader extends HitReader[Json] {

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

  def recreateIndex(): Task[Unit] = for {
    _ <- client.execute(deleteIndex(this.indexName))
    _ <- client.execute(createIndex(indexName).mapping(mapping))
  } yield ()

  def find(from: LocalDateTime,
           to: LocalDateTime,
           merchant: String,
           provider: String): Task[Response[IndexedSeq[Try[T]]]] = client.execute(
    search(indexName)
      .query(
        boolQuery()
          .must(
            rangeQuery(dateFieldName).gte(from.toElasticDate).lt(to.toElasticDate),
            termQuery(AIQField.merchant.toString, merchant),
            termQuery(AIQField.provider.toString, provider)
          )
      )
      .limit(1000)
      .seqNoPrimaryTerm(true)
  ).map(r => r.map(v => v.safeTo[T](reader)))

  def addBulk(jsonEntities: List[T]): Task[Response[BulkResponse]] =
    client.execute {
      bulk {
        jsonEntities.map { dm =>
          indexInto(indexName).doc(dm)(indexable)
        }
      }
    }

  def findByQuery(query: BoolQuery): ZIO[Any, Throwable, IndexedSeq[T]] = {
    client.execute(
      search(indexName)
        .query(query)
        .seqNoPrimaryTerm(true)
        .limit(1500)
    ).map(r => r.map(v => v.safeTo[T](reader)))
      .map(_.result)
      .flatMap { entities =>
        val (successJsons, failureJsons) = entities.partition(_.isSuccess)
        val failureMsg = failureJsons.collect {
          case Failure(exception) => exception.getMessage
        }
        ZIO.when(failureMsg.nonEmpty)(ZIO.logError(s"Could not parse entity: $failureMsg")) *>
          ZIO.succeed(successJsons.collect { case Success(v) => v })
      }
  }

  protected def logEntries[R](entities: List[R], message: R => String): ZIO[Any, Nothing, Unit] = {
    for {
      _ <- ZIO.foreachDiscard(entities) { ee => ZIO.logInfo(message(ee)) }
    } yield ()
  }

}

object ElasticSearchDAO {





}
