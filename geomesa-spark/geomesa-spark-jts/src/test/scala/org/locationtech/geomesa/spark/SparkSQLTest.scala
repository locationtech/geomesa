/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.vividsolutions.jts.geom._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class SparkSQLTest extends Specification {

  "spark jts module" should {
    sequential

    var spark: SparkSession = null
    var sc: SQLContext = null
    var df: DataFrame = null
    var newDF: DataFrame = null

    // before
    step {

      spark = SparkSession.builder()
        .appName("testSpark")
        .master("local[*]")
        .getOrCreate()

      sc = spark.sqlContext
      JTSTypes.init(sc)

      val schema = StructType(Array(StructField("name",StringType, nullable=false),
                                    StructField("pointText", StringType, nullable=false),
                                    StructField("polygonText", StringType, nullable=false),
                                    StructField("latitude", DoubleType, nullable=false),
                                    StructField("longitude", DoubleType, nullable=false)))

      val dataFile = this.getClass.getClassLoader.getResource("jts-example.csv").getPath
      df = spark.read.schema(schema).option("sep", "-").option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").csv(dataFile)

    }

    "have rows with user defined types" >> {
      import org.locationtech.geomesa.spark.SQLGeometricConstructorFunctions._
      newDF = df.withColumn("point", st_pointFromText(col("pointText")))
                .withColumn("polygon", st_polygonFromText(col("polygonText")))
                .withColumn("pointB", st_makePoint(col("latitude"), col("longitude")))

      newDF.createOrReplaceTempView("example")
      val row = newDF.first()
      val gf = new GeometryFactory
      row.get(5).isInstanceOf[Point] mustEqual true
      row.get(6).isInstanceOf[Polygon] mustEqual true
      row.get(7).isInstanceOf[Point] mustEqual true
    }

    "create a df from sequence of points" >> {
      import org.locationtech.geomesa.spark.SpatialEncoders.jtsPointEncoder
      val points = newDF.collect().map{r => r.getAs[Point](5)}
      val testDF = spark.createDataset(points).toDF()
      testDF.count() mustEqual df.count()
    }

    "udfs intergrate with dataframe api" >> {
      import org.locationtech.geomesa.spark.SQLGeometricConstructorFunctions.st_makeBBOX
      import org.locationtech.geomesa.spark.SQLSpatialFunctions.st_contains
      val countSQL = sc.sql("select * from example where st_contains(st_makeBBOX(0.0, 0.0, 90.0, 90.0), point)").count()
      val countDF = newDF.where(st_contains(st_makeBBOX(lit(0.0), lit(0.0), lit(90.0), lit(90.0)), col("point"))).count()
      countSQL mustEqual countDF
    }

    // after
    step {
      spark.stop()
    }
  }
}