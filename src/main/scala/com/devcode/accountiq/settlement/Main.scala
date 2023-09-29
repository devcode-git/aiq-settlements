package com.devcode.accountiq.settlement
import com.devcode.accountiq.settlement.config.AppConfig
import com.devcode.accountiq.settlement.http.{Routes, ServerDependencies}
import zio.http.Server
import zio.{ZIO, ZIOAppDefault}

object Main extends ZIOAppDefault {

  def app =
    ZIO.scoped(for {
      _          <- ZIO.logInfo("Starting Application")
      routes     <- ZIO.service[Routes]
      _          <- ZIO.logInfo("Starting HTTP server")
      _          <- Server.serve(routes.serverEndpoints)
    } yield ())

  override def run =
    app.provide(
      AppConfig.live,
      ServerDependencies.live,
      Routes.live
    )

}