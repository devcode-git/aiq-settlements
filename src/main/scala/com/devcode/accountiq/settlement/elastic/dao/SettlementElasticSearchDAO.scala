package com.devcode.accountiq.settlement.elastic.dao

import com.devcode.accountiq.settlement.elastic.reports.settlement.{SettlementDetailReport, SettlementDetailReportFeeRow, SettlementDetailReportMerchantPayoutRow, SettlementDetailReportRow}
import com.sksamuel.elastic4s.ElasticApi.{boolQuery, termQuery}
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import zio._
import zio.json._
import zio.json.ast.Json
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.requests.searches.queries.compound.BoolQuery

import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}


class SettlementElasticSearchDAO(esClient: ElasticClient)
  extends ElasticSearchDAO[SettlementDetailReport] {

  val client: ElasticClient = esClient
  val indexName = "detail_settlement_merchant_reports"
  val mapping: MappingDefinition = SettlementDetailReport.mapping
  val reader: HitReader[SettlementDetailReport] = IndexableHitReader
  val indexable: Indexable[SettlementDetailReport] = SettlementDetailReport.formatter
  val dateFieldName: String = "creationDate"
  val refIdFieldName: String = "merchantReference"

  implicit object IndexableHitReader extends HitReader[SettlementDetailReport] {
    override def read(hit: Hit): Try[SettlementDetailReport] =
      if (hit.isSourceEmpty) {
        Failure(new IllegalArgumentException(s"doc (id:${hit.id}) src is empty"))
      } else {
        val jsonVal = for {
          entityJson <- hit.sourceAsString.fromJson[Json]
          infoJson <- s"""{"_id": "${Some(hit.id)}", "version": {"_seq_no": ${hit.seqNo}, "_primary_term": ${hit.primaryTerm} } }""".fromJson[Json]
        } yield entityJson.merge(infoJson)

        jsonVal.flatMap { j =>
          j.as[SettlementDetailReport]
        } match {
          case Right(j) => Success(j)
          case Left(e) => Failure(new IllegalArgumentException(
            s"failed to parse src ${hit.sourceAsString} errors: " + e
          ))
        }
      }
  }

  override def addBulk(jsonEntities: List[SettlementDetailReport]) = {
    // SettlementDetailReportMerchantPayoutRow
    // SettlementDetailReportFeeRow
    val settlementReports: List[SettlementDetailReportRow] = jsonEntities.collect {
      case r: SettlementDetailReportRow => r
    }
    val settlementPayoutReports: List[SettlementDetailReportMerchantPayoutRow] = jsonEntities.collect {
      case r: SettlementDetailReportMerchantPayoutRow => r
    }
    val settlementFeeReports: List[SettlementDetailReportFeeRow] = jsonEntities.collect {
      case r: SettlementDetailReportFeeRow => r
    }

    for {
      _ <- ZIO.logInfo("Executing bulk add")
      newSettlementReports <- removeSettlementDuplicates(settlementReports)
      newSettlementPayoutReports <- removeSettlementPayoutDuplicates(settlementPayoutReports)
      newSettlementFeeReports <- removeSettlementFeeDuplicates(settlementFeeReports)
      newReports = newSettlementReports ++ newSettlementPayoutReports ++ newSettlementFeeReports

      res <- super.addBulk(newReports)
      _ <- ZIO.foreachDiscard(newSettlementReports) { ee =>
        ZIO.logInfo(s"Adding settlement report entry with refId `${ee.merchantReference}`")
      }
      _ <- ZIO.foreachDiscard(newSettlementPayoutReports) { ee =>
        ZIO.logInfo(s"Adding settlement payout report entry with type `${ee.`type`}` and date ${ee.creationDate}")
      }
      _ <- ZIO.foreachDiscard(newSettlementFeeReports) { ee =>
        ZIO.logInfo(s"Adding settlement fee report entry with type `${ee.`type`}` and date ${ee.creationDate}")
      }
    }  yield res
  }

  private def findEntriesByQuery(query: BoolQuery): ZIO[Any, Throwable, IndexedSeq[SettlementDetailReport]] = {
    for {
      jsons <- super.findByQuery(query)
      (successJsons, failureJsons) = jsons.result.partition(_.isSuccess)
      failureMsg = failureJsons.collect {
        case Failure(exception) => exception.getMessage
      }
      _ <- ZIO.when(failureMsg.nonEmpty)(ZIO.logError(s"Could not parse settlement detail reports: $failureMsg"))

      reports = successJsons.collect {
        case Success(v) => v
      }.map { r =>
        (r.asInstanceOf[Json.Obj].get("type").flatMap(_.asString) match {
          case Some("Fee") => r.as[SettlementDetailReportFeeRow]
          case Some("MerchantPayout") => r.as[SettlementDetailReportMerchantPayoutRow]
          case Some(_) => r.as[SettlementDetailReportRow]
        }) match {
          case Right(v) => v
        }
      }
    } yield reports
  }

  private def removeSettlementDuplicates(settlementReports: List[SettlementDetailReportRow]): Task[List[SettlementDetailReportRow]] = {
    val findQuery: BoolQuery = boolQuery()
      .should(settlementReports.map(_.merchantReference).map(id => termQuery(refIdFieldName, id)))
      .minimumShouldMatch(1)
    for {
      existingEntitiesRes <- this.findEntriesByQuery(findQuery)
      existingRefIds <- ZIO.attempt(existingEntitiesRes.collect {
        case v if v.isInstanceOf[SettlementDetailReportRow] => v.asInstanceOf[SettlementDetailReportRow].merchantReference
      })
      (existingEntities, newEntities) = settlementReports.partition(e => existingRefIds.contains(e.merchantReference))
      _ <- ZIO.foreachDiscard(existingEntities) { ee =>
        ZIO.logInfo(s"Skipping... Settlement entry [SettlementDetailReportRow] with refId `${ee.merchantReference}` already exists")
      }
    } yield newEntities
  }

  private def removeSettlementPayoutDuplicates(settlementReports: List[SettlementDetailReportMerchantPayoutRow]): Task[List[SettlementDetailReportMerchantPayoutRow]] = {
    val findQuery = typeCreationDateQuery(settlementReports.map(r => (r.`type`, r.creationDate)))
    for {
      existingEntitiesRes <- this.findEntriesByQuery(findQuery)
      existingData <- ZIO.attempt(existingEntitiesRes.collect {
        case v if v.isInstanceOf[SettlementDetailReportMerchantPayoutRow] =>
          (v.asInstanceOf[SettlementDetailReportMerchantPayoutRow].`type`, v.asInstanceOf[SettlementDetailReportMerchantPayoutRow].creationDate)
      })
      (existingEntities, newEntities) = settlementReports.partition(r => existingData.contains((r.`type`, r.creationDate)))
      _ <- ZIO.foreachDiscard(existingEntities) { ee =>
        ZIO.logInfo(s"Skipping... Settlement entry [SettlementDetailReportMerchantPayoutRow] with type `${ee.`type`}` and creation date ${ee.creationDate.toString} already exists")
      }
    } yield newEntities
  }

  private def removeSettlementFeeDuplicates(settlementReports: List[SettlementDetailReportFeeRow]): Task[List[SettlementDetailReportFeeRow]] = {
    val findQuery = typeCreationDateQuery(settlementReports.map(r => (r.`type`, r.creationDate)))
    for {
      existingEntitiesRes <- this.findEntriesByQuery(findQuery)
      existingData <- ZIO.attempt(existingEntitiesRes.collect {
        case v if v.isInstanceOf[SettlementDetailReportFeeRow] =>
          (v.asInstanceOf[SettlementDetailReportFeeRow].`type`, v.asInstanceOf[SettlementDetailReportFeeRow].creationDate)
      })
      (existingEntities, newEntities) = settlementReports.partition(r => existingData.contains((r.`type`, r.creationDate)))
      _ <- ZIO.foreachDiscard(existingEntities) { ee =>
        ZIO.logInfo(s"Skipping... Settlement entry [SettlementDetailReportFeeRow] with type `${ee.`type`}` and creation date ${ee.creationDate.toString} already exists")
      }
    } yield newEntities
  }

  private def typeCreationDateQuery(tuples: List[(String, LocalDateTime)]): BoolQuery = {
    boolQuery().should(tuples.map { case (reportType, creationDate) =>
      boolQuery().should(boolQuery().should(termQuery("type", reportType)), boolQuery().should(termQuery("creationDate", creationDate))).minimumShouldMatch(1)
    })
  }

//  private def logEntries[T](entities: List[T], message:T => String) = {
//    for {
//      _ <- ZIO.foreachDiscard(entities) { ee =>
//        ZIO.logInfo(s"Skipping... Settlement entry [SettlementDetailReportFeeRow] with type `${ee.`type`}` and creation date ${ee.creationDate.toString} already exists")
//      }
//    } yield ()
//  }

}

object SettlementElasticSearchDAO {
  val live: ZLayer[ElasticClient, Nothing, ElasticSearchDAO[SettlementDetailReport]] =
    ZLayer.fromFunction(createSettlementDetail _)
  private def createSettlementDetail(client: ElasticClient): ElasticSearchDAO[SettlementDetailReport] =
    new SettlementElasticSearchDAO(client)
}
