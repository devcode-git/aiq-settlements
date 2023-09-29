import sbt.Keys.{scalacOptions, semanticdbEnabled, semanticdbVersion}
//import sbtghpackages.GitHubPackagesPlugin.autoImport.{TokenSource, githubOwner, githubRepository, githubTokenSource}
import scalafix.sbt.ScalafixPlugin.autoImport.{scalafixCaching, scalafixSemanticdb}
import wartremover.WartRemover.autoImport.{Wart, Warts, wartremoverErrors}

object Options {
  import sbt._
  import sbt.Keys.{organization, scalaVersion}

  inThisBuild(
    Seq(
      organization := "com.devcode",
      scalaVersion := "2.13.12"
    )
  )

//  val githubSettings = Seq(
//    githubOwner       := "devcode-git",
//    githubRepository  := "https://github.com/devcode-git/aiq-settlement-reconciliation",
//    githubTokenSource := TokenSource.Environment("GITHUB_TOKEN") || TokenSource.GitConfig("github.token")
//  )

  val lintingSettings = Seq(
//    scalafixDependencies ++= Dependencies.scalaFixDeps,
    scalafixCaching   := true,
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    wartremoverErrors ++= Warts.allBut(
      // we use zio, which often infers types to *containing* Any/Nothing (but not exactly Any/Nothing)
      Wart.Any,
      Wart.DefaultArguments,
      Wart.ImplicitParameter,
      Wart.Overloading,
      Wart.Product,
      Wart.Nothing
    )
  )

  val compilerOptions = Seq(
    scalacOptions := Seq(
      "-feature",
      "-deprecation",
      "-explaintypes",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-language:higherKinds",
      "-language:existentials",
      "-Ywarn-value-discard",
      "-Ywarn-numeric-widen",
      "-Ywarn-extra-implicit",
      "-Ywarn-unused:-implicits,_",
      "-Wconf:cat=lint-package-object-classes:s,cat=deprecation:e"
    )
  )

}