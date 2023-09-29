package com.devcode.accountiq.settlement

import zio.{Config, ConfigProvider, ZIO, ZLayer}

import scala.annotation.nowarn
import scala.util.{Success, Try}

package object config {

  final case class AppConfig(
    httpConfig: HttpConfig)

  final case class HttpConfig(
    port: Int)

  object AppConfig {

    def create(httpConfig: HttpConfig): AppConfig =
      AppConfig(httpConfig)

    val live: ZLayer[Any, Config.Error, AppConfig] = ZLayer.fromZIO(for {
      httpConf      <- HttpConfig.create()
    } yield AppConfig(httpConf))

    val test: AppConfig = AppConfig(
      HttpConfig(
        port = 8080
      )
    )

  }

  object HttpConfig {

    def create(): ZIO[Any, Config.Error, HttpConfig] = {
      val config =
        Config.int("HTTP_SERVER_PORT")

      ConfigProvider.envProvider
        .load(config)
        .map(HttpConfig(_))
    }

  }

  @nowarn
  private[this] def extractValue[T](f: String => Try[T]): String => Either[Config.Error, T] =
    (rawData: String) =>
      f(rawData) match {
        case Success(value) => Right(value)
      }

}
