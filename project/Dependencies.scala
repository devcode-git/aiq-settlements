import sbt._


object Dependencies {
  import sbt._
//  import sbtghpackages.GitHubPackagesPlugin.autoImport.GHPackagesResolverSyntax

//  val ghResolver = Resolver.githubPackages("devcode-git")

  object Version {
    val zio = "2.0.15"
    val zioConfig = "4.0.0-RC16"
    val d11ZioHttp = "2.0.0-RC11"
    val zioJson = "0.6.0"
    val zioLogging = "2.1.13"
    val sqs = "2.16.49"
    val containers = "0.40.17"
    val slf4j = "1.7.36"
    val tapir = "1.6.0"
    val quartzSch = "2.5.0-rc1"
  }

  val zio = Seq(
    "dev.zio" %% "zio" % Version.zio,
    "dev.zio" %% "zio-config" % Version.zioConfig,
    "dev.zio" %% "zio-config-magnolia" % Version.zioConfig,
    "dev.zio" %% "zio-config-typesafe" % Version.zioConfig,
    "io.d11" %% "zhttp" % Version.d11ZioHttp,
    "dev.zio" %% "zio-json" % Version.zioJson,
    "dev.zio" %% "zio-streams" % Version.zio,
    "dev.zio" %% "zio-logging" % Version.zioLogging
  )

  val tapir = Seq(
    "com.softwaremill.sttp.tapir" %% "tapir-zio" % Version.tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-zio-http-server" % Version.tapir
  )

  val scalaFixDeps = Seq("com.github.liancheng" %% "organize-imports" % "0.6.0")

//  private val serviceSpecific = Seq(
//    "com.devcode" %% "aiq-common-domain" % "0.0.3",
//  )

  val all = zio ++ tapir
//  val all = zio ++ serviceSpecific
}