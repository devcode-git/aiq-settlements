package com.devcode.accountiq.settlement

import com.devcode.accountiq.settlement.config.AppConfig
import com.devcode.accountiq.settlement.elastic.reports.batch.BatchSalesToPayoutReportRow
import com.devcode.accountiq.settlement.elastic.reports.merchant.MerchantPaymentTransactionsReportRow
import com.devcode.accountiq.settlement.elastic.reports.settlement.SettlementDetailReportRow
import com.devcode.accountiq.settlement.elastic.ElasticSearchClient
import com.devcode.accountiq.settlement.elastic.dao.{BatchElasticSearchDAO, ElasticSearchDAO, MerchantElasticSearchDAO, SettlementElasticSearchDAO}
import com.devcode.accountiq.settlement.elastic.reports.ESDoc
import com.devcode.accountiq.settlement.services.TransformService
import com.devcode.accountiq.settlement.sftp.SftpDownloader
import com.devcode.accountiq.settlement.sftp.SftpDownloader.SFTPAccount
import com.devcode.accountiq.settlement.transformer.{CSVParser, XLSXParser}
import com.devcode.accountiq.settlement.util.FileUtil
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import com.sksamuel.elastic4s.{ElasticClient => ESClient}
import zio.json.ast.Json
import zio.json._
import com.devcode.accountiq.settlement.http.{Routes, ServerDependencies}
import zio.http.Server

import java.io.File
import java.nio.file.Path

object Main extends ZIOAppDefault {

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.setConfigProvider(
      TypesafeConfigProvider
        .fromResourcePath()
    )

//  private val esDocsElasticDAO: ZLayer[Any, Config.Error, ElasticSearchDAO[ESDoc]] = ZLayer.make[ElasticSearchDAO[ESDoc]](
//    ElasticSearchClient.live,
//    ElasticSearchDAO.liveESDoc,
//    ElasticConfig.live
//  )

  private val batchReportsElasticDAO: ZLayer[Any, Config.Error, ElasticSearchDAO[BatchSalesToPayoutReportRow]] = ZLayer.make[ElasticSearchDAO[BatchSalesToPayoutReportRow]](
    ElasticSearchClient.live,
    BatchElasticSearchDAO.live,
    ElasticConfig.live
  )

  private val settlementReportsElasticDAO: ZLayer[Any, Config.Error, ElasticSearchDAO[SettlementDetailReportRow]] = ZLayer.make[ElasticSearchDAO[SettlementDetailReportRow]](
    ElasticSearchClient.live,
    SettlementElasticSearchDAO.live,
    ElasticConfig.live
  )

  private val merchantPaymentTransactionsReportsElasticDAO: ZLayer[Any, Config.Error, ElasticSearchDAO[MerchantPaymentTransactionsReportRow]] = ZLayer.make[ElasticSearchDAO[MerchantPaymentTransactionsReportRow]](
    ElasticSearchClient.live,
    MerchantElasticSearchDAO.live,
    ElasticConfig.live
  )

  private val sftpAccount: ZLayer[Any, Nothing, SFTPAccount] = ZLayer.succeed(SFTPAccount(
    "s-050dbb81072240e89.server.transfer.eu-west-1.amazonaws.com",
    22,
    "fastpay_aiq",
    "b5dd33d9995cc68c4908910488869636",
    "home/upload",
    "/Users/white/Desktop/win"))

  private def recreateIndexes(): ZIO[ElasticSearchDAO[MerchantPaymentTransactionsReportRow] with ElasticSearchDAO[SettlementDetailReportRow] with ElasticSearchDAO[BatchSalesToPayoutReportRow], Throwable, Unit] = for {
    batchSalesToPayoutReportRow <- ZIO.service[ElasticSearchDAO[BatchSalesToPayoutReportRow]]
    _ <- batchSalesToPayoutReportRow.recreateIndex()
    settlementDetailReportRow <- ZIO.service[ElasticSearchDAO[SettlementDetailReportRow]]
    _ <- settlementDetailReportRow.recreateIndex()
    merchantPaymentTransactionsReportRow <- ZIO.service[ElasticSearchDAO[MerchantPaymentTransactionsReportRow]]
    _ <- merchantPaymentTransactionsReportRow.recreateIndex()
  } yield ()

  def app =
    ZIO.scoped(for {
      _          <- ZIO.logInfo("Starting Application")
//      routes     <- ZIO.service[Routes]
//      _          <- ZIO.logInfo("Starting HTTP server")
//      _          <- Server.serve(routes.serverEndpoints)

//      _ <- SftpDownloader.downloadAccount().provideLayer(sftpAccount)
//      _ <- createIndex().provide(elasticDAO)

      _ <- recreateIndexes()

      batchPath = new File("/Users/white/IdeaProjects/aiq-settlement-reconciliation/src/main/resources/Belgium salestopayout_sales_2023_08_01_2023_08_07_EUR.csv")
      _ <- TransformService.saveReports[BatchSalesToPayoutReportRow](batchPath, "adyen", "kindred", BatchSalesToPayoutReportRow.fromESDocRaw)
        .provide(batchReportsElasticDAO)

      settlementPath = new File("/Users/white/IdeaProjects/aiq-settlement-reconciliation/src/main/resources/Belgium settlement_detail_report_batch_297.csv")
      _ <- TransformService.saveReports[SettlementDetailReportRow](settlementPath, "adyen", "kindred", SettlementDetailReportRow.fromESDocRaw)
        .provide(settlementReportsElasticDAO)

      merchantPaymentTransactions = new File("/Users/white/IdeaProjects/aiq-settlement-reconciliation/src/main/resources/test-payment_transactions_04082023.csv")
      _ <- TransformService.saveReports[MerchantPaymentTransactionsReportRow](merchantPaymentTransactions, "adyen", "kindred", MerchantPaymentTransactionsReportRow.fromESDocRaw)
        .provide(merchantPaymentTransactionsReportsElasticDAO)

    } yield ())



  override def run: ZIO[Any, Throwable, Unit] = {
    app.provide(
      AppConfig.live,
      ServerDependencies.live,
      Routes.live,
      batchReportsElasticDAO,
      settlementReportsElasticDAO,
      merchantPaymentTransactionsReportsElasticDAO
    )
  }

}