package com.devcode.accountiq.settlement

import zio.{Config, ConfigProvider, ZIO, ZLayer}

import scala.annotation.nowarn
import scala.util.{Success, Try}
import zio.config.magnolia.deriveConfig

final case class ElasticConfig(mode: String, remote: Remote, local: Local)

final case class Remote(url: String,
                        username: String,
                        password: String,
                        maxConnTotal: Int,
                        maxConnPerRoute: Int)

final case class Local(host: String, port: Int)

object ElasticConfig {
  val config: Config[ElasticConfig] = deriveConfig[ElasticConfig].nested("ElasticConfig")
  val live = ZLayer.fromZIO(ZIO.config[ElasticConfig](ElasticConfig.config))
}

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
