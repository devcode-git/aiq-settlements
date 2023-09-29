addCommandAlias(
  "lintCheck", "scalafmtCheck; scalafixAll --check"
)

lazy val root = (project in file("."))
  .settings(
    name                 := "aiq-settlement",
    Compile / mainClass  := Some("com.devcode.accountiq.settlement.Main"),
    libraryDependencies  ++= Dependencies.all,
//    resolvers            += Dependencies.ghResolver,
//    Options.githubSettings,
    Options.lintingSettings,
    Options.compilerOptions
  )
  .enablePlugins(JavaAppPackaging)
