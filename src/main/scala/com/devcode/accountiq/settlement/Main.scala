package com.devcode.accountiq.settlement
import com.devcode.accountiq.settlement.sftp.SftpDownloader
import com.devcode.accountiq.settlement.transformer.{CSVParser, XLSXParser}
import zio._

import java.io.File
import java.nio.file.Path

object Main extends ZIOAppDefault {

  def app: ZIO[Any, Throwable, Unit] =
    ZIO.scoped(for {
      _          <- ZIO.logInfo("Starting Application")
//      routes     <- ZIO.service[Routes]
//      _          <- ZIO.logInfo("Starting HTTP server")
//      _          <- Server.serve(routes.serverEndpoints)
//      _ <- SftpDownloader.downloadAccount()
      chunks <- CSVParser.parse(new File("/Users/white/Desktop/Winbet_Deposit_31032023.csv").toPath)
//      xlsx <- XLSXParser.parse(new File("/Users/white/Desktop/Settlement_Reconciliation_Belgium.xlsx").toPath).tap(rows => ZIO.foreach(rows)(row=> ZIO.logInfo(row.mkString(","))))
    } yield ())

  override def run: ZIO[Any, Throwable, Unit] = {
    app
  }

}