package com.devcode.accountiq.settlement

import com.devcode.accountiq.settlement.elastic.reports.batch.BatchSalesToPayoutReportRow
import com.devcode.accountiq.settlement.elastic.reports.settlement.SettlementDetailReportRow
import com.devcode.accountiq.settlement.elastic.{ESDoc, ElasticSearchClient, ElasticSearchDAO}
import com.devcode.accountiq.settlement.sftp.SftpDownloader
import com.devcode.accountiq.settlement.sftp.SftpDownloader.SFTPAccount
import com.devcode.accountiq.settlement.transformer.{CSVParser, XLSXParser}
import com.devcode.accountiq.settlement.util.FileUtil
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import com.sksamuel.elastic4s.{ElasticClient => ESClient}
import zio.json.ast.Json
import zio.json._

import java.io.File
import java.nio.file.Path

object Main extends ZIOAppDefault {

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.setConfigProvider(
      TypesafeConfigProvider
        .fromResourcePath()
    )

  private val elasticDAO: ZLayer[Any, Config.Error, ElasticSearchDAO[ESDoc]] = ZLayer.make[ElasticSearchDAO[ESDoc]](
    ElasticSearchClient.live,
    ElasticSearchDAO.liveESDoc,
    ElasticConfig.live
  )

  private val elasticBatchDAO: ZLayer[Any, Config.Error, ElasticSearchDAO[BatchSalesToPayoutReportRow]] = ZLayer.make[ElasticSearchDAO[BatchSalesToPayoutReportRow]](
    ElasticSearchClient.live,
    ElasticSearchDAO.liveBatchSalesToPayout,
    ElasticConfig.live
  )

  private val elasticSettlementDAO: ZLayer[Any, Config.Error, ElasticSearchDAO[SettlementDetailReportRow]] = ZLayer.make[ElasticSearchDAO[SettlementDetailReportRow]](
    ElasticSearchClient.live,
    ElasticSearchDAO.liveSettlementDetail,
    ElasticConfig.live
  )

  private val sftpAccount: ZLayer[Any, Nothing, SFTPAccount] = ZLayer.succeed(SFTPAccount(
    "s-050dbb81072240e89.server.transfer.eu-west-1.amazonaws.com",
    22,
    "fastpay_aiq",
    "b5dd33d9995cc68c4908910488869636",
    "home/upload",
    "/Users/white/Desktop/win"))

  private def createIndex() = for {
    dao <- ZIO.service[ElasticSearchDAO[ESDoc]]
    response <- dao.createIndices()
    _ <- ZIO.logInfo(response.toString)
  } yield ()

  private def addESDocs(json: List[ESDoc]): ZIO[ElasticSearchDAO[ESDoc], Throwable, Unit] = for {
    dao <- ZIO.service[ElasticSearchDAO[ESDoc]]
    response <- dao.addBulk(json)
    _ <- ZIO.logInfo(response.toString)
  } yield ()

  private def addSettlementDetailReports(json: List[SettlementDetailReportRow]): ZIO[ElasticSearchDAO[SettlementDetailReportRow], Throwable, Unit] = for {
    dao <- ZIO.service[ElasticSearchDAO[SettlementDetailReportRow]]
    response <- dao.addBulk(json)
    _ <- ZIO.logInfo(response.toString)
  } yield ()

  private def addBatchSalesToPayoutReports(json: List[BatchSalesToPayoutReportRow]): ZIO[ElasticSearchDAO[BatchSalesToPayoutReportRow], Throwable, Unit] = for {
    dao <- ZIO.service[ElasticSearchDAO[BatchSalesToPayoutReportRow]]
    response <- dao.addBulk(json)
    _ <- ZIO.logInfo(response.toString)
  } yield ()

  def app: ZIO[Any, Throwable, Unit] =
    ZIO.scoped(for {
      _          <- ZIO.logInfo("Starting Application")
//      routes     <- ZIO.service[Routes]
//      _          <- ZIO.logInfo("Starting HTTP server")
//      _          <- Server.serve(routes.serverEndpoints)
//      _ <- SftpDownloader.downloadAccount().provideLayer(sftpAccount)
//      _ <- createIndex().provide(elasticDAO)

//      STEP3: PARSE SETTLEMENT REPORT FILE
//      path = new File("/Users/white/Desktop/test-SettlementDetailReport.xlsx").toPath
//      fileId <- FileUtil.getFileNamePart(path.toString)
//      rowsXLSX <- XLSXParser.parse(path).tap(rows => ZIO.foreach(rows)(row=> ZIO.logInfo(row.mkString(","))))
//      docs = ESDoc.parseESDocs(rowsXLSX, fileId).map(SettlementDetailReport.fromESDocRaw)
//      _ <- ZIO.logInfo(docs.mkString(","))


//      STEP3: PARSE SETTLEMENT REPORT FILE
//      settlementPath = new File("/Users/white/IdeaProjects/aiq-settlement-reconciliation/src/main/resources/Belgium settlement_detail_report_batch_297.csv").toPath
//      fileId <- FileUtil.getFileNamePart(settlementPath.toString)
//      rows <- CSVParser.parse(settlementPath)
//      esdocs = ESDoc.parseESDocs(rows, fileId)
//      esdocsAIQ = esdocs.map(SettlementDetailReportRow.fromESDocRaw)
//      _ <- addSettlementDetailReports(esdocsAIQ).provide(elasticSettlementDAO)
//      _ <- ZIO.logInfo(esdocsAIQ.mkString(","))

//        STEP3: PARSE BATCH REPORT FILE
      batchPath = new File("/Users/white/IdeaProjects/aiq-settlement-reconciliation/src/main/resources/Belgium salestopayout_sales_2023_08_01_2023_08_07_EUR.csv").toPath
      fileId <- FileUtil.getFileNamePart(batchPath.toString)
      rows <- CSVParser.parse(batchPath)
      esdocs = ESDoc.parseESDocs(rows, fileId)
      esdocsAIQ = esdocs.map(BatchSalesToPayoutReportRow.fromESDocRaw)
      _ <- addBatchSalesToPayoutReports(esdocsAIQ).provide(elasticBatchDAO)
      _ <- ZIO.logInfo(esdocsAIQ.mkString(","))
  //      esdocsAIQ = esdocs.map(BatchSalesToPayoutReportRow.fromESDocRaw)

//      STEP4: ADD TO ELASTIC SEARCH
//      _ <- addESDocs(esdocs).provide(elasticDAO)

    } yield ())



  override def run: ZIO[Any, Throwable, Unit] = {
    app
  }

}