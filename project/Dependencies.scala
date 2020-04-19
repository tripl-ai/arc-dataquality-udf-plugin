import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "2.4.5"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"

  // arc
  val arc = "ai.tripl" %% "arc" % "2.10.0" % "provided"

  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

  val libPhoneNumber = "com.googlecode.libphonenumber" % "libphonenumber" % "8.12.1"

  // Project
  val etlDeps = Seq(
    scalaTest,
    
    arc,

    sparkSql,

    libPhoneNumber
  )
}