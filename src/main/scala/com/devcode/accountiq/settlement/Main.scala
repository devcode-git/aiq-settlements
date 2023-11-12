package com.devcode.accountiq.settlement
import com.devcode.accountiq.settlement.elastic.{ESDoc, ElasticSearchClient, ElasticSearchDAO, SettlementDetailReport}
import com.devcode.accountiq.settlement.sftp.SftpDownloader
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

  private val elasticConfig: ZLayer[Any, Config.Error, ElasticConfig] =
    ZLayer
      .fromZIO(
        ZIO.config[ElasticConfig](ElasticConfig.config)
      )

  private val elasticClient: ZLayer[Any, Config.Error, ESClient] = elasticConfig >>> ElasticSearchClient.live
  private val elasticDAO: ZLayer[Any, Config.Error, ElasticSearchDAO[ESDoc]] = elasticClient >>> ElasticSearchDAO.live

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
//      _ <- SftpDownloader.downloadAccount()
//      rowsCSV <- CSVParser.parse(new java.io.File("/Users/white/IdeaProjects/aiq-settlement-reconciliation/src/main/resources/test.csv").toPath)
//      docs = ESDoc.parseESDocs(rowsCSV)
//      _ <- addDocs(docs).provide(elasticDAO)

//      _ <- createIndex().provide(elasticDAO)
      path = new File("/Users/white/Desktop/test2.xlsx").toPath
      fileId <- FileUtil.getFileNamePart(path.toString)
      rowsXLSX <- XLSXParser.parse(path).tap(rows => ZIO.foreach(rows)(row=> ZIO.logInfo(row.mkString(","))))
      docs = ESDoc.parseESDocs(rowsXLSX, fileId).map(SettlementDetailReport.fromESDocRaw)
      _ <- ZIO.logInfo(docs.mkString(","))
//      _ <- addDocs(docs).provide(elasticDAO)

    } yield ())



  override def run: ZIO[Any, Throwable, Unit] = {
    app
  }

}