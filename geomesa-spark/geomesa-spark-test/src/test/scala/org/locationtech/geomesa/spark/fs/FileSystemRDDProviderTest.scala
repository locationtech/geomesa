/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark.fs

import org.geomesa.testcontainers.HadoopContainer
import org.geotools.api.data.{DataStore, DataStoreFinder, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.spark.TestWithSpark
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.locationtech.geomesa.utils.text.WKTUtils

class FileSystemRDDProviderTest extends TestWithSpark {

  import scala.collection.JavaConverters._

  private val hadoop = new HadoopContainer().withNetwork(network)

  // these params will work in the spark executor, but not locally outside the docker network
  lazy val sparkDsParams = {
    val host = hadoop.execInContainer("hostname", "-s").getStdout.trim
    logger.debug("Using host: {}", host)
    Map(
      "fs.path" -> s"${hadoop.getHdfsUrl}/${getClass.getSimpleName}/".replaceAll(hadoop.getHost, host),
      "fs.config.xml" -> hadoop.getConfigurationXml.replaceAll(hadoop.getHost, host)
    )
  }

  lazy val ds: DataStore = {
    val params = Map(
      "fs.path" -> s"${hadoop.getHdfsUrl}/${getClass.getSimpleName}/",
      "fs.config.xml" -> hadoop.getConfigurationXml
    )
    DataStoreFinder.getDataStore(params.asJava)
  }

  val formats = Seq(/*"orc",*/ "parquet") // TODO fix orc

  override def beforeAll(): Unit = {
    hadoop.start()

    // note: the host reach-back networking required for spark seems to mess up the accumulo networking unless accumulo starts first
    super.beforeAll()

    // note: have to create all data up front to avoid caching issues in the spark executor
    formats.foreach { format =>
      val sft = SimpleFeatureTypes.createType(format,
        "arrest:String,case_number:Int:index=full:cardinality=high,dtg:Date,*geom:Point:srid=4326")
      sft.getUserData.put("geomesa.fs.encoding", format)
      sft.getUserData.put("geomesa.fs.scheme", """{"name":"z2-8bits"}""")
      ds.createSchema(sft)

      val features = List(
        ScalaSimpleFeature.create(sft, "1", "true", 1, "2016-01-01T00:00:00.000Z", "POINT (-76.5 38.5)"),
        ScalaSimpleFeature.create(sft, "2", "true", 2, "2016-01-02T00:00:00.000Z", "POINT (-77.0 38.0)"),
        ScalaSimpleFeature.create(sft, "3", "true", 3, "2016-01-03T00:00:00.000Z", "POINT (-78.0 39.0)")
      )

      WithClose(ds.getFeatureWriterAppend(format, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
    }
    formats.foreach { format =>
      val sft = SimpleFeatureTypes.createType(s"${format}_complex",
        "name:String,age:Int,dtg:Date,*geom:MultiLineString:srid=4326,pt:Point,line:LineString," +
          "poly:Polygon,mpt:MultiPoint,mline:MultiLineString,mpoly:MultiPolygon")
      sft.getUserData.put("geomesa.fs.encoding", format)
      sft.getUserData.put("geomesa.fs.scheme", """{"name":"daily"}""")
      sft.getUserData.put("geomesa.fs.leaf-storage", "false")
      ds.createSchema(sft)

      val features = Seq.tabulate(10) { i =>
        ScalaSimpleFeature.create(
          sft,
          s"$i",
          s"test$i",
          100 + i,
          s"2017-06-0${5 + (i % 3)}T04:03:02.0001Z",
          s"MULTILINESTRING((0 0, 10 10.$i))",
          "POINT(0 0)", "LINESTRING(0 0, 1 1, 4 4)",
          "POLYGON((10 10, 10 20, 20 20, 20 10, 10 10), (11 11, 19 11, 19 19, 11 19, 11 11))",
          "MULTIPOINT((0 0), (1 1))",
          "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))",
          "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((10 10, 10 20, 20 20, 20 10, 10 10), (11 11, 19 11, 19 19, 11 19, 11 11)))")
      }

      foreach(features)(_.getAttributes.asScala must not(contain(beNull[AnyRef])))

      WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
    }
    formats.foreach { format =>
      val sft = SimpleFeatureTypes.createType(s"${format}_delete",
        "arrest:String,case_number:Int:index=full:cardinality=high,dtg:Date,*geom:Point:srid=4326")
      sft.getUserData.put("geomesa.fs.encoding", format)
      sft.getUserData.put("geomesa.fs.scheme", """{"name":"z2-8bits"}""")
      ds.createSchema(sft)

      val features = List(
        ScalaSimpleFeature.create(sft, "1", "true", 1, "2016-01-01T00:00:00.000Z", "POINT (-76.5 38.5)"),
        ScalaSimpleFeature.create(sft, "2", "true", 2, "2016-01-02T00:00:00.000Z", "POINT (-77.0 38.0)"),
        ScalaSimpleFeature.create(sft, "3", "true", 3, "2016-01-03T00:00:00.000Z", "POINT (-78.0 39.0)")
      )

      WithClose(ds.getFeatureWriterAppend(s"${format}_delete", Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
      WithClose(ds.getFeatureWriter(s"${format}_delete", ECQL.toFilter("IN ('1', '2')"), Transaction.AUTO_COMMIT)) { writer =>
        var i = 0
        while (i < 2) {
          writer.hasNext must beTrue
          val sf = writer.next()
          sf.getID match {
            case "1" => sf.setAttribute("geom", "POINT (76.5 38.5)"); writer.write()
            case "2" => writer.remove()
          }
          i += 1
        }
        writer.hasNext must beFalse
      }
    }
    formats.foreach { format =>
      val df = spark.read
        .format("geomesa")
        .options(sparkDsParams)
        .option("geomesa.feature", format)
        .load()
      logger.debug(df.schema.treeString)
      df.createOrReplaceTempView(format)
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    CloseWithLogging(ds, hadoop)
  }

  "FileSystemRDDProvider" should {
    "select * from chicago" >> {
      foreach(formats) { format =>
        sc.sql(s"select * from $format").collect() must haveLength(3)
      }
    }

    "select count(*) from chicago" >> {
      foreach(formats) { format =>
        val rows = sc.sql(s"select count(*) from $format").collect()
        rows must haveLength(1)
        rows.head.get(0) mustEqual 3L
      }
    }

    "select by spatiotemporal filter" >> {
      foreach(formats) { format =>
        val select = s"select * from $format where st_intersects(geom, st_makeBbox(-80,35,-75,45)) AND " +
            "dtg > '2016-01-01T12:00:00Z' AND dtg < '2016-01-02T12:00:00Z'"
        val rows = sc.sql(select).collect()
        rows must haveLength(1)
        rows.head.get(0) mustEqual "2"
      }
    }

    "select by secondary indexed attribute, using dataframe API" >> {
      foreach(formats) { format =>
        val df = spark.read
            .format("geomesa")
            .options(sparkDsParams)
            .option("geomesa.feature", format)
            .load()
        val cases = df.select("case_number").where("case_number = 1").collect().map(_.getInt(0))
        cases mustEqual Array(1)
      }
    }

    "select complex st_buffer" >> {
      foreach(formats) { format =>
        val select = s"select st_asText(st_bufferPoint(geom,10)) from $format where case_number = 1"
        val res = sc.sql(select).collect()
        res must haveLength(1)
        val reselect = s"select * from $format where st_contains(st_geomFromWKT('${res.head.getString(0)}'), geom)"
        sc.sql(reselect).collect() must haveLength(1)
      }
    }

    "write data" >> {
      foreach(formats) { format =>
        val subset = sc.sql(s"select case_number,geom,dtg from $format")
        subset
            .write
            .format("geomesa")
            .options(sparkDsParams)
            .option("geomesa.feature", s"${format}2")
            .save()
        ds.getSchema(s"${format}2") must not(beNull)
      }
    }.pendingUntilFixed("FSDS can't guess the parameters")

    "handle all the geometry types" >> {
      foreach(formats) { format =>
        val df = spark.read
          .format("geomesa")
          .options(sparkDsParams)
          .option("geomesa.feature", s"${format}_complex")
          .load()

        logger.debug(df.schema.treeString)
        df.createOrReplaceTempView(s"${format}_complex")
        val res = sc.sql(s"select * from ${format}_complex").collect()
        res must haveLength(10)
        foreach(res)(r => foreach(Seq.tabulate(r.length)(r.get))(_ must not(beNull)))
      }
    }

    "support updates/deletes" >> {
      foreach(formats) { format =>
        val df = spark.read
          .format("geomesa")
          .options(sparkDsParams)
          .option("geomesa.feature", s"${format}_delete")
          .load()
        df.createOrReplaceTempView(s"${format}_delete")
        val res = sc.sql(s"select * from ${format}_delete").collect()
        res must haveLength(2)
        res.map(_.get(0)).toSeq must containTheSameElementsAs(Seq("1", "3"))
        res.collectFirst { case r if r.get(0) == "1" => r.get(4) } must beSome[Any](WKTUtils.read("POINT (76.5 38.5)"))
      }
    }
  }
}
