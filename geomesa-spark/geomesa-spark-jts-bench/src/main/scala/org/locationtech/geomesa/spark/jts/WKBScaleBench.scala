/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts

import java.util.concurrent.TimeUnit

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.openjdk.jmh.annotations._

import scala.beans.BeanProperty
import scala.collection.mutable.ListBuffer
import scala.util.Random

@BenchmarkMode(Array(Mode.Throughput))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MINUTES)
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.MICROSECONDS)
class WKBScaleBench {

  import WKBScaleBench._

  @transient
  val spark = SparkSession.builder
    .master("local[*]")
    .appName(getClass.getSimpleName)
    .config("spark.ui.enabled", false)
    .config("spark.ui.showConsoleProgress", false)
    .getOrCreate
    .withJTS

  @Setup(Level.Trial)
  def setupData(): Unit = {
    val pointsDF = spark.createDataFrame(points)
    val countriesDF = spark.createDataFrame(countries)

    pointsDF.createOrReplaceTempView("points")
    countriesDF.createOrReplaceTempView("countries")

    pointsDF.persist(MEMORY_ONLY)
    countriesDF.persist(MEMORY_ONLY)
  }

  @Benchmark
  def jtsGroupPointsByCountries: Long = {
    println("Benchmark start")
    val res = spark.sql(
      """
        |select name, points.geom as p_geom
        |from points join countries on st_contains(countries.geom, points.geom)
      """.stripMargin)
    val count = res.count()
    println(s"Result: $count")
    count
  }
}

object WKBScaleBench {
  case class CountryBean(@BeanProperty name: String, @BeanProperty geom: Geometry)
  case class PointBean(@BeanProperty geom: Geometry)

  val r = new Random()
  val n = 300000
  val pointsRaw = {
    for (_ <- 0 to n) yield {
      val lat = (r.nextInt(180) - 90).toFloat + r.nextFloat()
      val lon = (r.nextInt(360) - 180).toFloat + r.nextFloat()
      s"POINT (${lon.formatted("%1.4f")} ${lat.formatted("%1.4f")})"
    }
  }
  val points: List[PointBean] = {
      pointsRaw.map{ wkt =>
        PointBean(WKTUtils.read(wkt))
    }.toList
  }

  val countriesRaw: List[Feature] = {
    val featJson = new FeatureJSON()
    val is = getClass.getResourceAsStream("/countries.geojson")
    val fc = featJson.readFeatureCollection(is)
    val featIter = fc.features()
    val featurelist = new ListBuffer[Feature]()
    try {
      while (featIter.hasNext) {
        featurelist += featIter.next()
      }
    } finally {
      featIter.close()
    }
    featurelist.toList
  }
  val countries: List[CountryBean] = {
    countriesRaw.map { c =>
      val wkt = c.asInstanceOf[SimpleFeature].getDefaultGeometry.toString
      val name = c.getProperty("name").getValue.toString
      CountryBean(name, WKTUtils.read(wkt))
    }
  }
}
