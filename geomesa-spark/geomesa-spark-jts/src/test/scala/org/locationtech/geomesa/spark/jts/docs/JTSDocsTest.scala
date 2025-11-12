/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.docs

import org.locationtech.geomesa.spark.jts.TestWithSpark

import java.io.File

/**
 * Test rig for verifying examples in the user manual. Note: this cannot be in the
 * parent package and safely confirm all proper imports are included in the examples.
 */
class JTSDocsTest extends TestWithSpark {

  import org.apache.spark.sql.types._
  import org.locationtech.geomesa.spark.jts._
  import org.locationtech.jts.geom._

  lazy val csv = new File(getClass.getClassLoader.getResource("jts-example.csv").toURI).getAbsolutePath

  "jts documentation example" should {

    "read and convert geospatial csv" >> {
      import spark.implicits._
      spark.withJTS
      val schema = StructType(Array(
        StructField("name",StringType, nullable=false),
        StructField("pointText", StringType, nullable=false),
        StructField("polygonText", StringType, nullable=false),
        StructField("latitude", DoubleType, nullable=false),
        StructField("longitude", DoubleType, nullable=false)))

      val df = spark.read
        .schema(schema)
        .option("sep", "-")
        .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
        .csv(csv)

      val alteredDF = df
        .withColumn("polygon", st_polygonFromText($"polygonText"))
        .withColumn("point", st_makePoint($"latitude", $"longitude"))

      val points = alteredDF.select($"pointText".as[String], $"point".as[Point]).collect()
      forall(points) {
        case (pt, p) => pt.toUpperCase() shouldEqual p.toString
      }

      val polys =  alteredDF.select($"polygonText".as[String], $"polygon".as[Polygon]).collect()
      forall(polys) {
        case (pt, p) => pt.toUpperCase() shouldEqual p.toString
      }
    }

    "convert point into dataset" >> {
      import spark.implicits._

      val point = new GeometryFactory().createPoint(new Coordinate(3.4, 5.6))
      val df = Seq(point).toDF("point")

      df.as[Point].first shouldEqual point
    }

    "should search chicago" >> {
      import org.locationtech.geomesa.spark.jts._
      import spark.implicits._

      val chicagoDF = dfBlank().withColumn("geom", st_makePoint(10.0, 10.0))
      chicagoDF.where(st_contains(st_makeBBOX(0.0, 0.0, 90.0, 90.0), $"geom"))

      chicagoDF.count() shouldEqual 1
    }
  }
}
