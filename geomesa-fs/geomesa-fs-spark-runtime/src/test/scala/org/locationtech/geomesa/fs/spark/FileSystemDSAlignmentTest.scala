/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.spark

import java.nio.file.{Files, Path}

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.spark.sql.{SQLContext, SQLTypes, SparkSession}
import org.geotools.data.{DataStore, DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType
import org.locationtech.geomesa.spark.SparkSQLTestUtils
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class FileSystemDSAlignmentTest extends Specification with LazyLogging {
  sequential

  val tempDir: Path = Files.createTempDirectory("fsAlignmentTest")

  lazy val directory1: String = tempDir + "/data/first"
  lazy val params1 = Map("fs.path" -> directory1)
  lazy val ds1: DataStore = DataStoreFinder.getDataStore(params1.asJava)

  lazy val directory2: String = tempDir + "/data/second"
  lazy val params2 = Map("fs.path" -> directory1)
  lazy val ds2: DataStore = DataStoreFinder.getDataStore(params2.asJava)

  var spark: SparkSession = _
  var sc: SQLContext = _

  step {
    spark = SparkSQLTestUtils.createSparkSession()
    sc = spark.sqlContext
    SQLTypes.init(sc)
  }

  // NB: The first focus is on parquet.  To work on Orc, we'll need to abstract over the .parquet/.orc write methods
  // val formats = Seq("orc", "parquet")
  val formats = Seq("parquet")

  "FileSystem DataStore" should {
    "Write data to directory1 using the GT FSDS" >> {
      println(s" ***: Temporary directory is ${tempDir}.  Directory is $directory1.")

      formats.foreach { format =>
        val sft = SimpleFeatureTypes.createType(format,
          "arrest:String,case_number:Int:index=full:cardinality=high,dtg:Date,*geom:Point:srid=4326")
        sft.setScheme("z2-8bits")
        sft.setEncoding(format)
        ds1.createSchema(sft)

        val features = List(
          ScalaSimpleFeature.create(sft, "1", "true", 1, "2016-01-01T00:00:00.000Z", "POINT (-76.5 38.5)"),
          ScalaSimpleFeature.create(sft, "2", "true", 2, "2016-01-02T00:00:00.000Z", "POINT (-77.0 38.0)"),
          ScalaSimpleFeature.create(sft, "3", "true", 3, "2016-01-03T00:00:00.000Z", "POINT (-78.0 39.0)")
        )

        WithClose(ds1.getFeatureWriterAppend(format, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
      }
      ok
    }

    // Commented out since this works!
//    "Query directory2 with GeoMesa's Spark integration" >> {
//      foreach(formats) { format =>
//        queryWithSpark(format, directory1)
//      }
//    }

    // Commented out since this works!
//    "Query directory1 with the GM FSDS DS" >> {
//      foreach(formats) { format =>
//        queryWithGeoTools(format, directory1)
//      }
//    }

    "write data to directory2 using Spark" >> {
      foreach(formats) { format =>
        val df = spark.read
          .format("geomesa")
          .options(params1)
          .option("geomesa.feature", format)
          .load()

        df.write
          .format(format)
          .parquet(directory2+"/parquet/")
        ok
      }
    }

    "Query directory2 with GeoMesa's Spark integration" >> {
      foreach(formats) { format =>
        queryWithSpark(format, directory2)
      }
    }.pendingUntilFixed("Fails since SFT data is not registered.  Error is java.io.IOException: Schema 'parquet' does not exist.")

    "Query directory2 with the GM FSDS DS" >> {
      foreach(formats) { format =>
        queryWithGeoTools(format, directory2)
      }
    } //.pendingUntilFixed("Fails since SFT metadata, file metadata, and column alignment are wrong.")
  }

  private def queryWithGeoTools(format: String, location: String) = {
    val outputDirectoryParams = Map("fs.path" -> location)
    val outputDS: DataStore = DataStoreFinder.getDataStore(outputDirectoryParams.asJava)
    val numberOfSFTS = outputDS.getTypeNames.length
    numberOfSFTS must beGreaterThan(0)

    val fs = outputDS.getFeatureSource(format)
    val q = new Query(format)
    fs.getCount(new Query(format)) mustEqual(3)
    //fs.getCount(new Query(format, ECQL.toFilter("case_number = 1"))) mustEqual(1)

    CloseableIterator(fs.getFeatures(new Query(format, ECQL.toFilter("bbox(geom,-80,35,-75,45) " +
      "AND dtg > '2016-01-01T12:00:00Z' AND dtg < '2016-01-03T12:00:00Z'"))).features()).size mustEqual(2)
//    fs.getCount(new Query(format, ECQL.toFilter("bbox(geom,-80,35,-75,45) " +
//      "AND dtg > '2016-01-01T12:00:00Z' AND dtg < '2016-01-02T12:00:00Z'"))) mustEqual(2)


  }

  private def queryWithSpark(format: String, directory: String) = {
    val df = spark.read
      .format("geomesa")
      .option("fs.path", directory)
      .option("geomesa.feature", format)
      .load()
    logger.debug(df.schema.treeString)
    df.createOrReplaceTempView(format)
    sc.sql(s"select * from $format").collect() must haveLength(3)

    // "select count(*) from chicago"
    val countStar = sc.sql(s"select count(*) from $format").collect()
    countStar must haveLength(1)
    countStar.head.get(0) mustEqual 3L

    // "select by spatiotemporal filter"
    val stQuery = sc.sql(s"select * from $format where st_intersects(geom, st_makeBbox(-80,35,-75,45)) AND " +
      "dtg > '2016-01-01T12:00:00Z' AND dtg < '2016-01-02T12:00:00Z'").collect()
    stQuery must haveLength(1)
    stQuery.head.get(0) mustEqual "2"

    // "select by secondary indexed attribute, using dataframe API"
    val cases = df.select("case_number").where("case_number = 1").collect().map(_.getInt(0))
    cases mustEqual Array(1)
  }

  step {
    ds1.dispose()
    // Stop MiniCluster
//    cluster.shutdown()
    FileUtils.deleteDirectory(tempDir.toFile)
  }
}
