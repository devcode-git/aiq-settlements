package com.devcode.accountiq.settlement.http


import com.devcode.accountiq.settlement.config.AppConfig
import zio.ZLayer
import zio.http.Server

object ServerDependencies {
  def create(config: AppConfig): ZLayer[Any, Throwable, Server] = Server.defaultWithPort(config.httpConfig.port)

  val live: ZLayer[AppConfig, Throwable, Server] = ZLayer.fromFunction(create _).flatten
}
