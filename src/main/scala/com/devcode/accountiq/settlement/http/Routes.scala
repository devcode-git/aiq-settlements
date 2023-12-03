package com.devcode.accountiq.settlement.http

import com.devcode.accountiq.settlement.recoonciliation.{ReconTimeFrame, ReconcileCmd}
import com.devcode.accountiq.settlement.services.ReconciliationService
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

  private val reconcileEndpoint =
    endpoint.post
      .in("reconcile" / path[String] / path[String] / query[Option[Int]]("days") / stringJsonBody)
      .out(plainBody[String])

  private val reconcileServerEndpoint =
    reconcileEndpoint.zServerLogic { case (merchant, provider, days, timeFrame) =>
      for {
        dateRange <- getTimeFrame(days, timeFrame)
        cmd = ReconcileCmd(dateRange, Some(merchant), Some(provider))
        res <- ReconciliationService.reconcile(cmd)
      } yield (res)
    }

  private def getTimeFrame(days: Option[RuntimeFlags], timeFrame: String) = {
    ReconTimeFrame.decoder.decodeJson(timeFrame) match {
      case Right(tf) => ZIO.succeed(tf)
      case Left(e) => ZIO.logWarning(s"Could not parse ReconTimeFrame, message: $e") *> ZIO.succeed(ReconTimeFrame.forDaysBack(days.getOrElse(100)))
    }
  }

  private val endpoints = List(healthCheckServerEndpoint, reconcileServerEndpoint)

  val serverEndpoints = ZioHttpInterpreter()
    .toHttp(endpoints)
    .catchAllCauseZIO(err => ZIO.dieMessage(err.failures.toString()))

}

object Routes {

  def create(): Routes = new Routes()

  val live: ZLayer[Any, Throwable, Routes] = ZLayer.fromFunction(create _)

}