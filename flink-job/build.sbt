ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"

lazy val root = (project in file("."))
  .enablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    name := "flink-job",

    // Entry point for Flink
    Compile / mainClass := Some("com.example.ErrorPatternJob"),

    libraryDependencies ++= Seq(
      // Flink core Scala API modules
      "org.apache.flink" %% "flink-streaming-scala" % "1.19.0" % Provided,
      "org.apache.flink" %% "flink-scala"           % "1.19.0" % Provided,

      // Flink clients (Java-only, no Scala suffix!)
      "org.apache.flink" %  "flink-clients"         % "1.19.0" % Provided,

      // Kafka connector (new unified connector)
      "org.apache.flink" %  "flink-connector-kafka" % "3.3.0-1.19",

      // CEP (Complex Event Processing)
      "org.apache.flink" %% "flink-cep-scala"       % "1.19.0" % Provided,
      "org.apache.flink" %  "flink-cep"             % "1.19.0" % Provided
    ),

    // Merge strategy for shading
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) =>
        xs.map(_.toLowerCase) match {
          case Seq("versions", _, "module-info.class") => MergeStrategy.discard
          case Seq("manifest.mf")                      => MergeStrategy.discard
          case Seq("index.list") | Seq("dependencies") => MergeStrategy.discard
          case _                                       => MergeStrategy.first
        }
      case PathList("reference.conf")    => MergeStrategy.concat
      case PathList("application.conf")  => MergeStrategy.concat
      case _                             => MergeStrategy.first
    }
  )

Compile / mainClass := Some("com.example.ErrorPatternJob")
