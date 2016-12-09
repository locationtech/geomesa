/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark.accumulo

import java.util.{Map => JMap}

import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.geotools.data.DataStoreFinder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloQueryProperties
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkSQLDataTest extends Specification {
  val createPoint = JTSFactoryFinder.getGeometryFactory.createPoint(_: Coordinate)

  "sql data tests" should {
    sequential

    System.setProperty(QueryProperties.SCAN_RANGES_TARGET.property, "1")
    System.setProperty(AccumuloQueryProperties.SCAN_BATCH_RANGES.property, s"${Int.MaxValue}")

    var mac: MiniAccumuloCluster = null
    var dsParams: JMap[String, String] = null
    var ds: AccumuloDataStore = null
    var spark: SparkSession = null
    var sc: SQLContext = null

    var df: DataFrame = null
    var gndf: DataFrame = null
    var sdf: DataFrame = null

    // before
    step {
      mac = SparkSQLTestUtils.setupMiniAccumulo()
      dsParams = SparkSQLTestUtils.createDataStoreParams(mac)
      ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]

      spark = SparkSession.builder().master("local[*]").getOrCreate()
      sc = spark.sqlContext
      sc.setConf("spark.sql.crossJoin.enabled", "true")
    }

    "ingest chicago" >> {
      SparkSQLTestUtils.ingestChicago(ds)

      df = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .load()
      df.printSchema()
      df.createOrReplaceTempView("chicago")

      df.collect.length mustEqual 3
    }

    "ingest geonames" >> {
      SparkSQLTestUtils.ingestGeoNames(dsParams)

      gndf = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "geonames")
        .load()
      gndf.printSchema()
      gndf.createOrReplaceTempView("geonames")

      gndf.collect.length mustEqual 2550
    }

    "ingest states" >> {
      import org.apache.spark.sql.functions.broadcast

      SparkSQLTestUtils.ingestStates(ds)

      sdf = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "states")
        .load()
      sdf.printSchema()
      sdf.createOrReplaceTempView("states")

      broadcast(sdf).createOrReplaceTempView("broadcastStates")

      sdf.collect.length mustEqual 56
    }

    "basic sql 1" >> {
      val r = sc.sql("select * from chicago where case_number = 1")
      r.show()
      val d = r.collect

      d.length mustEqual 1
      d.head.getAs[Point]("geom") mustEqual createPoint(new Coordinate(-76.5, 38.5))
    }

    "join st_contains" >> {
      val r = sc.sql(
        """
          |select broadcastStates.STUSPS, count(*), sum(population)
          |from geonames, broadcastStates
          |where st_contains(broadcastStates.the_geom, geonames.geom)
          |  and featurecode = "PPL"
          |group by broadcastStates.STUSPS
          |order by broadcastStates.STUSPS
        """.stripMargin
      )
      val d = r.collect()
      val d1 = d.head
      d.length mustEqual 48
      d1.getAs[String](0) mustEqual "AL"
      d1.getAs[Long](1) mustEqual 6
      d1.getAs[Long](2) mustEqual 2558
    }

    "st_translate" >> {
      val r = sc.sql(
        """
          |select ST_Translate(st_geomFromWKT('POINT(0 0)'), 5, 12)
        """.stripMargin)

      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(5 12)")
    }
  }
}
