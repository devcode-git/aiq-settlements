package com.devcode.accountiq.settlement.http

import com.devcode.accountiq.settlement.elastic.dao.ElasticSearchDAO
import com.devcode.accountiq.settlement.elastic.reports.batch.BatchSalesToPayoutReportRow
import com.devcode.accountiq.settlement.elastic.reports.merchant.MerchantPaymentTransactionsReportRow
import com.devcode.accountiq.settlement.elastic.reports.settlement.SettlementDetailReportRow
import com.devcode.accountiq.settlement.recoonciliation.{ReconTimeFrame, ReconcileCmd}
import com.devcode.accountiq.settlement.services.ReconciliationService
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.ztapir._
import zio._
import zio.json.EncoderOps

class Routes(batchDAO: ElasticSearchDAO[BatchSalesToPayoutReportRow],
             merchantDAO: ElasticSearchDAO[MerchantPaymentTransactionsReportRow],
             settlementDAO: ElasticSearchDAO[SettlementDetailReportRow]) {

  private val healthCheckEndpoint =
    endpoint.get
      .in("live")
      .out(plainBody[String])

  private val healthCheckServerEndpoint =
    healthCheckEndpoint.zServerLogic(_ => ZIO.succeed("hello world!"))

  private val batchReports =
    endpoint.post
      .in("batchReports" / path[String] / path[String] / query[Option[Int]]("days") / stringJsonBody)
      .out(stringJsonBody)
      .zServerLogic { case (merchant, provider, days, timeFrame) =>
      for {
        dateRange <- getTimeFrame(days, timeFrame)
        cmd = ReconcileCmd(dateRange, merchant, provider)
        res <- ReconciliationService.findBatchSalesToPayoutReports(cmd).provide(ZLayer.succeed(batchDAO)).orDie
      } yield (res.toJson)
    }

  private val merchantReports =
    endpoint.post
      .in("merchantReports" / path[String] / path[String] / query[Option[Int]]("days") / stringJsonBody)
      .out(stringJsonBody)
      .zServerLogic { case (merchant, provider, days, timeFrame) =>
      for {
        dateRange <- getTimeFrame(days, timeFrame)
        cmd = ReconcileCmd(dateRange, merchant, provider)
        res <- ReconciliationService.findMerchantPaymentTransactionsReports(cmd).provide(ZLayer.succeed(merchantDAO)).orDie
      } yield (res.toJson)
    }

  private val settlementReports =
    endpoint.post
      .in("settlementReports" / path[String] / path[String] / query[Option[Int]]("days") / stringJsonBody)
      .out(stringJsonBody)
      .zServerLogic { case (merchant, provider, days, timeFrame) =>
      for {
        dateRange <- getTimeFrame(days, timeFrame)
        cmd = ReconcileCmd(dateRange, merchant, provider)
        res <- ReconciliationService.findSettlementDetailReportRow(cmd).provide(ZLayer.succeed(settlementDAO)).orDie
      } yield (res.toJson)
    }

  private def getTimeFrame(days: Option[RuntimeFlags], timeFrame: String) = {
    ReconTimeFrame.decoder.decodeJson(timeFrame) match {
      case Right(tf) => ZIO.succeed(tf)
      case Left(e) => ZIO.logWarning(s"Could not parse ReconTimeFrame, message: $e") *> ZIO.succeed(ReconTimeFrame.forDaysBack(days.getOrElse(100)))
    }
  }

  private val endpoints = List(healthCheckServerEndpoint, batchReports, merchantReports, settlementReports)

  val serverEndpoints = ZioHttpInterpreter()
    .toHttp(endpoints)
    .catchAllCauseZIO(err => ZIO.dieMessage(err.failures.toString()))

}

object Routes {

  def create(batchDAO: ElasticSearchDAO[BatchSalesToPayoutReportRow],
             merchantDAO: ElasticSearchDAO[MerchantPaymentTransactionsReportRow],
             settlementDAO: ElasticSearchDAO[SettlementDetailReportRow]): Routes = new Routes(batchDAO, merchantDAO, settlementDAO)

  val live: ZLayer[ElasticSearchDAO[BatchSalesToPayoutReportRow] with ElasticSearchDAO[MerchantPaymentTransactionsReportRow] with ElasticSearchDAO[SettlementDetailReportRow], Nothing, Routes] = ZLayer.fromFunction(create _)

}