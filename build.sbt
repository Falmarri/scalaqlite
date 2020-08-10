import scala.sys.process._
name := "scalaqlite"

organization := "com.meraki"

version := "0.8-RC2"

scalaVersion := "2.13.3"
crossScalaVersions := Seq(scalaVersion.value, "2.12.12", "2.11.12")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.32.3.2"

scalacOptions ++= Seq("-deprecation")

publishTo := Some(Resolver.file("file",  new File( "releases" )) )
