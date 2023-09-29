package com.devcode.accountiq.settlement.http

import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.ztapir._
import zio._

class Routes() {

  private val healthCheckEndpoint =
    endpoint.get
      .in("live")
      .out(plainBody[String])

  private val healthCheckServerEndpoint =
    healthCheckEndpoint.zServerLogic(_ => ZIO.succeed("hello world!"))

  private val endpoints = List(healthCheckServerEndpoint)

  val serverEndpoints = ZioHttpInterpreter()
    .toHttp(endpoints)
    .catchAllCauseZIO(err => ZIO.dieMessage(err.failures.toString()))

}

object Routes {

  def create(): Routes = new Routes()

  val live: ZLayer[Any, Throwable, Routes] = ZLayer.fromFunction(create _)

}