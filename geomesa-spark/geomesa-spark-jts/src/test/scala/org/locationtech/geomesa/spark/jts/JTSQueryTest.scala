/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.locationtech.jts.geom._
import org.specs2.specification.BeforeAll

import java.io.File

class JTSQueryTest extends TestWithSpark with BeforeAll {

  sequential

  lazy val csv = new File(getClass.getClassLoader.getResource("jts-example.csv").toURI).getAbsolutePath

  var df: DataFrame = _
  var newDF: DataFrame = _

  override def beforeAll(): Unit = {
    val schema = StructType(Array(StructField("name",StringType, nullable=false),
      StructField("pointText", StringType, nullable=false),
      StructField("polygonText", StringType, nullable=false),
      StructField("latitude", DoubleType, nullable=false),
      StructField("longitude", DoubleType, nullable=false)))
    df = spark.read.schema(schema)
      .option("sep", "-")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .csv(csv)
  }

  "spark jts module" should {

    "have rows with user defined types" in {

      newDF = df.withColumn("point", st_pointFromText(col("pointText")))
                .withColumn("polygon", st_polygonFromText(col("polygonText")))
                .withColumn("pointB", st_makePoint(col("latitude"), col("longitude")))

      newDF.createOrReplaceTempView("example")
      val row = newDF.first()
      row.get(5).isInstanceOf[Point] mustEqual true
      row.get(6).isInstanceOf[Polygon] mustEqual true
      row.get(7).isInstanceOf[Point] mustEqual true
    }

    "create a df from sequence of points" in {
      val points = newDF.collect().map{r => r.getAs[Point](5)}
      val testDF = spark.createDataset(points).toDF()
      testDF.count() mustEqual df.count()
    }

    "udfs intergrate with dataframe api" in {
      val countSQL = sc.sql("select * from example where st_contains(st_makeBBOX(0.0, 0.0, 90.0, 90.0), point)").count()
      val countDF = newDF
        .where(st_contains(st_makeBBOX(lit(0.0), lit(0.0), lit(90.0), lit(90.0)), col("point")))
        .count()
      countSQL mustEqual countDF
    }
  }
}
