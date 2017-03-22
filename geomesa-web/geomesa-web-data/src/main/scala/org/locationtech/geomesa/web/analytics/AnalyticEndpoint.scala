/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web.analytics

import javax.servlet.http.HttpServletRequest

import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.sql.types.StructType
import org.json4s.{DefaultFormats, Formats}
import org.locationtech.geomesa.compute.spark.sql.GeoMesaSparkSql
import org.locationtech.geomesa.utils.cache.FilePersistence
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.locationtech.geomesa.web.core.GeoMesaDataStoreServlet
import org.scalatra.BadRequest
import org.scalatra.json.NativeJsonSupport

import scala.io.Source
import scala.xml.Elem

/**
 * Rest endpoint to access geomesa analytics
 */
class AnalyticEndpoint(val persistence: FilePersistence) extends GeoMesaDataStoreServlet with NativeJsonSupport {

  import AnalyticEndpoint._
  override protected implicit def jsonFormats: Formats = DefaultFormats

  override val root: String = "analytics"

  override def defaultFormat: Symbol = 'json

  // TODO ensure hadoop settings are available on the classpath...

  // register any persisted data stores with the sql engine
  getPersistedDataStores.values.foreach(GeoMesaSparkSql.registerDataStore)
  sys.addShutdownHook(GeoMesaSparkSql.stop(0))

  // jars that will be distributed with the spark job
  lazy val distributedJars: Seq[String] = {
    val url = Option(getClass.getClassLoader.getResource("spark-jars.list"))
        .getOrElse(getClass.getClassLoader.getResource("spark-jars-default.list"))
    val source = Source.fromURL(url)
    val jars = try { source.getLines().toList } finally { source.close() }
    val searchLocations = Iterator(
      () => ClassPathUtils.getJarsFromClasspath(classOf[AnalyticEndpoint]),
      () => ClassPathUtils.getJarsFromClasspath(classOf[HttpServletRequest]),
      // jboss classloaders don't allow us to get jars out... instead just use the filesystem
      () => ClassPathUtils.getJarsFromEnvironment("JBOSS_HOME")
    )
    ClassPathUtils.findJars(jars, searchLocations).map(_.getAbsolutePath)
  }

  // explicit reference to the spark jar - this is needed b/c spark can't find itself with jboss' classloading
  lazy val jbossSparkJar = {
    // this will match the default geomesa-assembly - for a custom install you can specify
    // the jar along with the other configs, which will override this
    val jbossJars = ClassPathUtils.getJarsFromEnvironment("JBOSS_HOME")
    val sparkJar = jbossJars.find { jar =>
      jar.getName.startsWith("spark-") && jar.getName.endsWith("-geomesa-assembly.jar")
    }
    sparkJar.map(j => Map("spark.yarn.jar" -> j.getAbsolutePath))
  }

  before() {
    contentType = formats(format)
  }

  /**
   * Set spark configuration properties
   */
  post("/spark/config") {
    val config = params.filterKeys(_.startsWith("spark."))
    try {
      if (config.isEmpty) {
        BadRequest(reason = "No configuration properties specified")
      } else {
        persistSparkConfigs(config)
      }
    } catch {
      case e: Exception => handleError(s"Error persisting spark configs:", e)
    }
  }

  /**
   * Retrieve spark configuration properties
   */
  get("/spark/config/?") {
    try {
      sparkConfigs
    } catch {
      case e: Exception => handleError(s"Error reading spark configs:", e)
    }
  }

  /**
   * Execute a sql query
   */
  get("/sql") {
    val sql = params.get("q").getOrElse(params("query"))
    try {
      GeoMesaSparkSql.start(sparkConfigs, distributedJars) // ensure the spark context is running
      val results = GeoMesaSparkSql.execute(sql)
      val output = SqlResults(results._1, results._2.toSeq.map(_.toSeq))
      format match {
        case "txt" => sqlToText(output)
        case "xml" => sqlToXml(output)
        case _     => output // automatic conversion
      }
    } catch {
      case e: Exception => handleError("Error running sql query", e)
    }
  }

  /**
   * Restart the spark sql context - this will pick up the latest configuration
   */
  post("/sql/restart") {
    try {
      GeoMesaSparkSql.stop()
      getPersistedDataStores.values.foreach(GeoMesaSparkSql.registerDataStore)
      GeoMesaSparkSql.start(sparkConfigs, distributedJars)
    } catch {
      case e: Exception => handleError(s"Error restarting sql context", e)
    }
  }

  /**
   * Stop the spark sql context - frees up yarn resources
   */
  post("/sql/stop") {
    try {
      GeoMesaSparkSql.stop()
    } catch {
      case e: Exception => handleError(s"Error restarting sql context", e)
    }
  }

  /**
   * Starts up the spark sql context
   */
  post("/sql/start") {
    try {
      getPersistedDataStores.values.foreach(GeoMesaSparkSql.registerDataStore)
      GeoMesaSparkSql.start(sparkConfigs, distributedJars)
    } catch {
      case e: Exception => handleError(s"Error restarting sql context", e)
    }
  }

  /**
   * Convert to xml - scalatra doesn't handle this well on it's own
   */
  private def sqlToXml(results: SqlResults): Elem = {
    val fields = results.schema.toSeq.map { s =>
      <field name={s.name} dataType={s.dataType.typeName} nillable={s.nullable.toString} />
    }
    <response>
      <schema>
        { fields }
      </schema>
      <results>
        { results.results.map(r => <row>{r.map(c => <col>{c}</col>)}</row>) }
      </results>
    </response>
  }

  /**
   * Convert to tsv. Output will look like:
   *
   * col1 col2  col3
   * ---- ----  ----
   * row1col1 row1col2  row1col3
   * row2col1 row2col2  row2col3
   */
  private def sqlToText(results: SqlResults): String = {
    val delim = params.get("delim") match {
      case Some("t") | Some("tab") | None => "\t"
      case Some("c") | Some("comma")      => ","
      case _ => throw new RuntimeException(s"Unknown delimiter ${params("delim")}")
    }
    val fields = results.schema.fieldNames
    val header = fields.mkString(delim)
    val separator = fields.map(f => Array.fill(f.length)('-').mkString).mkString("\n", delim, "\n")
    val rows = if (delim == ",") {
      results.results.map(_.map(String.valueOf).map(StringEscapeUtils.escapeCsv))
    } else {
      results.results
    }
    val output = rows.map(_.mkString(delim)).mkString("\n")
    header + separator + output
  }

  /**
   * Gets spark configs
   */
  private def sparkConfigs: Map[String, String] = {
    val configs = persistence.entries("spark-config.").map { case (k, v) => (k.substring(13), v) }.toMap
    // explicitly reference the spark jar - this is needed for jboss
    jbossSparkJar match {
      case None    => configs
      case Some(j) => j ++ configs // allow for configs to override spark jar
    }
  }

  /**
   * Save spark configs
   */
  private def persistSparkConfigs(config: Map[String, String]): Unit =
    persistence.persistAll(config.map { case (k, v) => (s"spark-config.$k", v) })
}

object AnalyticEndpoint {

  case class SqlResults(schema: StructType, results: Seq[Seq[Any]])

  private def keyFor(alias: String, param: String = "") = s"ds.$alias.$param"
}