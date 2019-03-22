/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.apache.spark.sql.{DataFrame, SQLContext, SQLTypes, SparkSession}
import org.geotools.data.{DataStore, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory
import org.locationtech.geomesa.spark.SparkSQLTestUtils
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom._
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class FSSparkProviderTest extends Specification with BeforeAfterAll with LazyLogging {

  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  sequential

  lazy val sftName: String = "chicago"
  val tempDir: Path = Files.createTempDirectory("fsSparkTest")
  var cluster: MiniDFSCluster = _
  var directory: String = _

  def spec: String = SparkSQLTestUtils.ChiSpec

  def dtgField: Option[String] = Some("dtg")

  lazy val params = Map(
    "fs.path" -> directory,
    "fs.encoding" -> "orc")
  lazy val dsf: FileSystemDataStoreFactory = new FileSystemDataStoreFactory()
  lazy val ds: DataStore = dsf.createDataStore(params)

  var spark: SparkSession = _
  var sc: SQLContext = _
  var df: DataFrame = _

  override def beforeAll(): Unit = {
    // Start MiniCluster
    val conf = new HdfsConfiguration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.toFile.getAbsolutePath)
    cluster = new MiniDFSCluster.Builder(conf).build()
    directory = cluster.getURI + s"/data/$sftName"
    val ds = dsf.createDataStore(params)

    val sft = SparkSQLTestUtils.ChicagoSpec
    sft.setScheme("z2-8bits")
    ds.createSchema(sft)

    SparkSQLTestUtils.ingestChicago(ds)
  }

  "FileSystem datastore Spark Data Tests" should {
    // before
    "start spark" >> {
      spark = SparkSQLTestUtils.createSparkSession()
      sc = spark.sqlContext
      SQLTypes.init(sc)

      df = spark.read
        .format("geomesa")
        .options(params.map { case (k, v) => k -> v.toString })
        .option("geomesa.feature", "chicago")
        .load()

      logger.debug(df.schema.treeString)
      df.createOrReplaceTempView("chicago")
      true
    }

    "select * from chicago" >> {
      sc.sql("select * from chicago").collect().length must beEqualTo(3)
    }

    "select count(*) from chicago" >> {
      val rows = sc.sql("select count(*) from chicago").collect()
      rows.length mustEqual(1)
      rows.apply(0).get(0).asInstanceOf[Long] mustEqual(3l)
    }

    "select by spatiotemporal filter" >> {
      val rows = sc.sql("select * from chicago where st_intersects(geom, st_makeBbox(-80,35,-75,45)) AND " +
          "dtg > '2016-01-01T12:00:00Z' AND dtg < '2016-01-02T12:00:00Z'").collect()
      rows.length mustEqual 1
      rows.apply(0).get(0) mustEqual "2"
    }

    "select by secondary indexed attribute" >> {
      val cases = df.select("case_number").where("case_number = 1").collect().map(_.getInt(0))
      cases.length mustEqual 1
    }

    "complex st_buffer" >> {
      val buf = sc.sql("select st_asText(st_bufferPoint(geom,10)) from chicago where case_number = 1").collect().head.getString(0)
      sc.sql(
        s"""
           |select *
           |from chicago
           |where
           |  st_contains(st_geomFromWKT('$buf'), geom)
                        """.stripMargin
      ).collect().length must beEqualTo(1)
    }

    "write data and properly index" >> {
      val subset = sc.sql("select case_number,geom,dtg from chicago")
      subset.write.format("geomesa")
        .options(params.map { case (k, v) => k -> v.toString })
        .option("geomesa.feature", "chicago2")
        .save()

      val sft = ds.getSchema("chicago2")
      val enabledIndexes = sft.getUserData.get("geomesa.indices").asInstanceOf[String]
      enabledIndexes.indexOf("z3") must be greaterThan -1
    }.pendingUntilFixed("FSDS can't guess the parameters")

    "Handle all the geometry types" >> {
      val typeName = "orc"
      val pt: Point              = WKTUtils.read("POINT(0 0)").asInstanceOf[Point]
      val line: LineString       = WKTUtils.read("LINESTRING(0 0, 1 1, 4 4)").asInstanceOf[LineString]
      val polygon: Polygon       = WKTUtils.read("POLYGON((10 10, 10 20, 20 20, 20 10, 10 10), (11 11, 19 11, 19 19, 11 19, 11 11))").asInstanceOf[Polygon]
      val mpt: MultiPoint        = WKTUtils.read("MULTIPOINT((0 0), (1 1))").asInstanceOf[MultiPoint]
      val mline: MultiLineString = WKTUtils.read("MULTILINESTRING ((0 0, 1 1), \n  (2 2, 3 3))").asInstanceOf[MultiLineString]
      val mpolygon: MultiPolygon = WKTUtils.read("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((10 10, 10 20, 20 20, 20 10, 10 10), (11 11, 19 11, 19 19, 11 19, 11 11)))").asInstanceOf[MultiPolygon]

      val sft: SimpleFeatureType = SimpleFeatureTypes.createType(typeName, "name:String,age:Int,dtg:Date," +
        "*geom:MultiLineString:srid=4326,pt:Point,line:LineString,poly:Polygon,mpt:MultiPoint,mline:MultiLineString,mpoly:MultiPolygon")
      sft.setScheme("daily")
      sft.setLeafStorage(false)

      val features: Seq[ScalaSimpleFeature] = Seq.tabulate(10) { i =>
        ScalaSimpleFeature.create(sft, s"$i", s"test$i", 100 + i, s"2017-06-0${5 + (i % 3)}T04:03:02.0001Z", s"MULTILINESTRING((0 0, 10 10.$i))",
          pt, line, polygon, mpt, mline, mpolygon)
      }

      val ds = dsf.createDataStore(params)
      ds.createSchema(sft)
      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach { feature =>
          FeatureUtils.copyToWriter(writer, feature, useProvidedFid = true)
          writer.write()
        }
      }

      df = spark.read
        .format("geomesa")
        .options(params.map { case (k, v) => k -> v.toString })
        .option("geomesa.feature", typeName)
        .load()

      logger.debug(df.schema.treeString)
      df.createOrReplaceTempView(typeName)
      sc.sql(s"select * from $typeName").show()

      true
    }
  }

  override def afterAll(): Unit = {
    // Stop MiniCluster
    cluster.shutdown()
    FileUtils.deleteDirectory(tempDir.toFile)
  }
}
