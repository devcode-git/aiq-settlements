package com.devcode.accountiq.settlement

import com.devcode.accountiq.settlement.elastic.reports.batch.BatchSalesToPayoutReport
import com.devcode.accountiq.settlement.elastic.reports.settlement.SettlementDetailReport
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
    ElasticSearchDAO.live,
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

  private def addDocs(json: List[ESDoc]) = for {
    dao <- ZIO.service[ElasticSearchDAO[ESDoc]]
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

//    STEP3: PARSE BATCH REPORT FILE
      path = new File("/Users/white/Desktop/reports/test-BatchSalesToPayout.xlsx").toPath
      fileId <- FileUtil.getFileNamePart(path.toString)
      rowsXLSX <- XLSXParser.parse(path).tap(rows => ZIO.foreach(rows)(row => ZIO.logInfo(row.mkString(","))))
      docs = ESDoc.parseESDocs(rowsXLSX, fileId)
//      docs = ESDoc.parseESDocs(rowsXLSX, fileId).map(BatchSalesToPayoutReport.fromESDocRaw)
//      _ <- ZIO.logInfo(docs.mkString(","))

//      STEP4: ADD TO ELASTIC SEARCH
      _ <- addDocs(docs).provide(elasticDAO)

    } yield ())



  override def run: ZIO[Any, Throwable, Unit] = {
    app
  }

}