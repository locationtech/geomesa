/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.everit.json.schema.loader.SchemaLoader
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.json.{JSONObject, JSONTokener}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.StorageKeys
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadataFactory
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorageFactory
import org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.fs.storage.parquet.io.{GeoParquetMetadata, SimpleFeatureParquetSchema}
import org.locationtech.geomesa.security.{AuthsParam, DefaultAuthorizationsProvider, SecurityUtils, VisibilityUtils}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.{Geometry, Point}
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

import java.nio.file.Files
import java.sql.DriverManager
import java.util.{Locale, UUID}

@RunWith(classOf[JUnitRunner])
class ParquetStorageTest extends Specification with AllExpectations with LazyLogging {

  import scala.collection.JavaConverters._

  lazy val config = new Configuration()
  config.set("parquet.compression", "gzip")

  // 8 bits resolution creates 3 partitions with our test data
  lazy val scheme = NamedOptions("z2-8bits")

  lazy val geoParquetSchema = WithClose(getClass.getClassLoader.getResourceAsStream("geoparquet-1.1.0-schema.json")) { is =>
    SchemaLoader.load(new JSONObject(new JSONTokener(is)))
  }

  "ParquetFileSystemStorage" should {
    "read and write features" in {
      val sft = SimpleFeatureTypes.createType("parquet-test", "*geom:Point:srid=4326,name:String,age:Int,dtg:Date")

      val features = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(1, s"name$i")
        sf.setAttribute(2, s"$i")
        sf.setAttribute(3, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(0, s"POINT(4$i 5$i)")
        sf
      }

      foreach(Seq(GeometryEncoding.GeoMesaV1, GeometryEncoding.GeoParquetNative, GeometryEncoding.GeoParquetWkb)) { encoding =>
        withTestDir { dir =>
          val config = new Configuration(this.config)
          config.set(SimpleFeatureParquetSchema.GeometryEncodingKey, encoding.toString)
          val context = FileSystemContext(dir, config)
          val metadata =
            new FileBasedMetadataFactory()
                .create(context, Map.empty, Metadata(sft, "parquet", scheme, leafStorage = true))
          WithClose(new ParquetFileSystemStorageFactory().apply(context, metadata)) { storage =>
            storage must not(beNull)

            val writers = scala.collection.mutable.Map.empty[String, FileSystemWriter]

            features.foreach { f =>
              val partition = storage.metadata.scheme.getPartitionName(f)
              val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
              writer.write(f)
            }

            writers.foreach(_._2.close())

            logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

            val partitions = storage.getPartitions.map(_.name)
            partitions must haveLength(writers.size)

            val transformsList = Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))

            val doTest = testQuery(storage, sft) _

            foreach(transformsList) { transforms =>
              doTest("INCLUDE", transforms, features)
              doTest("IN('0', '2')", transforms, Seq(features(0), features(2)))
              doTest("bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.dropRight(2))
              doTest("bbox(geom,42,48,52,62) and dtg DURING 2013-12-15T00:00:00.000Z/2014-01-15T00:00:00.000Z", transforms, features.drop(2))
              doTest("bbox(geom,42,48,52,62)", transforms, features.drop(2))
              doTest("dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.dropRight(2))
              doTest("name = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.slice(5, 6))
              doTest("name < 'name5'", transforms, features.take(5))
              doTest("name = 'name5'", transforms, features.slice(5, 6))
              doTest("age < 5", transforms, features.take(5))
            }

            // verify we can load an existing storage
            val loaded = new FileBasedMetadataFactory().load(context)
            loaded must beSome
            WithClose(new ParquetFileSystemStorageFactory().apply(context, loaded.get))(testQuery(_, sft)("INCLUDE", null, features))

            // verify GeoParquet metadata
            foreach(storage.getPartitions.headOption.flatMap(_.files.headOption)) { file =>
              val path = new Path(dir, file.name)
              WithClose(ParquetFileReader.open(HadoopInputFile.fromPath(path, context.conf))) { reader =>
                val meta = reader.getFileMetaData.getKeyValueMetaData
                val geo = Option(meta.get(GeoParquetMetadata.GeoParquetMetadataKey)).map(new JSONObject(_)).orNull
                geo must not(beNull)
                geoParquetSchema.validate(geo) must not(throwAn[Exception])
                val col = geo.getJSONObject("columns").getJSONObject("geom")
                val expectedEncoding = encoding match {
                  case GeometryEncoding.GeoParquetWkb => "WKB"
                  case _ => "point"
                }
                col.getString("encoding") mustEqual expectedEncoding
                col.getJSONArray("geometry_types").asScala.toSeq mustEqual Seq("Point")
                // first partition contains first 5 features
                col.getJSONArray("bbox").asScala.map(_.asInstanceOf[Number].doubleValue()) mustEqual Seq(40d, 50d, 44d, 54d)
                if (encoding == GeometryEncoding.GeoParquetWkb) {
                  val covering = col.getJSONObject("covering").getJSONObject("bbox")
                  foreach(Seq("xmin", "ymin", "xmax", "ymax")) { corner =>
                    covering.getJSONArray(corner).toString mustEqual s"""["__geom_bbox__","$corner"]"""
                  }
                } else {
                  col.has("covering") must beFalse
                }
              }
            }
          }
        }
      }
    }

    "read and write complex features" in {
      val sft = SimpleFeatureTypes.createType("parquet-test-complex",
        "name:String,age:Int,time:Long,height:Float,weight:Double,bool:Boolean," +
            "uuid:UUID,bytes:Bytes,list:List[Int],map:Map[String,Long]," +
            "line:LineString,mpt:MultiPoint,poly:Polygon,mline:MultiLineString,mpoly:MultiPolygon,g:Geometry," +
            "dtg:Date,*geom:Point:srid=4326")

      val features = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute("name", s"name$i")
        sf.setAttribute("age", s"$i")
        sf.setAttribute("time", s"$i")
        sf.setAttribute("height", s"$i")
        sf.setAttribute("weight", s"$i")
        sf.setAttribute("bool", Boolean.box(i < 5))
        sf.setAttribute("uuid", UUID.fromString(s"00000000-0000-0000-0000-00000000000$i"))
        sf.setAttribute("bytes", Array.tabulate[Byte](i)(i => i.toByte))
        sf.setAttribute("list", Seq.tabulate[Integer](i)(i => Int.box(i)))
        sf.setAttribute("map", (0 until i).map(i => i.toString -> Long.box(i)).toMap)
        sf.setAttribute("line", s"LINESTRING(0 $i, 2 $i, 8 ${10 - i})")
        sf.setAttribute("mpt", s"MULTIPOINT(0 $i, 2 3)")
        sf.setAttribute("poly",
          if (i == 5) {
            // polygon example with holes from wikipedia
            "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))"
          } else {
            s"POLYGON((40 3$i, 42 3$i, 42 2$i, 40 2$i, 40 3$i))"
          }
        )
        sf.setAttribute("mline", s"MULTILINESTRING((0 2, 2 $i, 8 6),(0 $i, 2 $i, 8 ${10 - i}))")
        sf.setAttribute("mpoly", s"MULTIPOLYGON(((-1 0, 0 $i, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6), (-1 5, 2 5, 2 2, -1 2, -1 5)))")
        sf.setAttribute("g", sf.getAttribute(Seq("line", "mpt", "poly", "mline", "mpoly").drop(i % 5).head))
        sf.setAttribute("dtg", f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute("geom", s"POINT(4$i 5$i)")
        sf
      }

      foreach(Seq(GeometryEncoding.GeoMesaV1, GeometryEncoding.GeoParquetNative, GeometryEncoding.GeoParquetWkb)) { encoding =>
        withTestDir { dir =>
          val config = new Configuration(this.config)
          config.set(SimpleFeatureParquetSchema.GeometryEncodingKey, encoding.toString)
          val context = FileSystemContext(dir, config)
          val metadata =
            new FileBasedMetadataFactory()
              .create(context, Map.empty, Metadata(sft, "parquet", scheme, leafStorage = true))
          WithClose(new ParquetFileSystemStorageFactory().apply(context, metadata)) { storage =>
            storage must not(beNull)

            val writers = scala.collection.mutable.Map.empty[String, FileSystemWriter]

            features.foreach { f =>
              val partition = storage.metadata.scheme.getPartitionName(f)
              val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
              writer.write(f)
            }

            writers.foreach(_._2.close())

            logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

            val partitions = storage.getPartitions.map(_.name)
            partitions must haveLength(writers.size)

            val transformsList = Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))

            val doTest = testQuery(storage, sft) _

            foreach(transformsList) { transforms =>
              doTest("INCLUDE", transforms, features)
              doTest("IN('0', '2')", transforms, Seq(features(0), features(2)))
              doTest("bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.dropRight(2))
              doTest("bbox(geom,42,48,52,62) and dtg DURING 2013-12-15T00:00:00.000Z/2014-01-15T00:00:00.000Z", transforms, features.drop(2))
              doTest("bbox(geom,42,48,52,62)", transforms, features.drop(2))
              doTest("dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.dropRight(2))
              doTest("name = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.slice(5, 6))
              doTest("name < 'name5'", transforms, features.take(5))
              doTest("name = 'name5'", transforms, features.slice(5, 6))
              doTest("age < 5", transforms, features.take(5))
              doTest("age > 5", transforms, features.drop(6))
            }

            foreach(storage.getPartitions.headOption.flatMap(_.files.headOption)) { file =>
              val path = new Path(dir, file.name)
              // verify 3rd party integration by reading with DuckDB
              if (encoding == GeometryEncoding.GeoParquetWkb) {
                val geoms = Seq("line", "mpt", "poly", "mline", "mpoly", "g", "geom")
                WithClose(DriverManager.getConnection("jdbc:duckdb:")) { conn =>
                  WithClose(conn.createStatement())(_.execute("INSTALL spatial;LOAD spatial;"))
                  WithClose(conn.prepareStatement(s"SELECT ${geoms.map(g => s"ST_AsText($g)").mkString(", ")} FROM '$path';")) { ps =>
                    WithClose(ps.executeQuery()) { rs =>
                      rs.next() must beTrue
                      val data = Seq.tabulate(geoms.length)(i => rs.getObject(i + 1))
                      // jts adds parens around multipoints, duckdb does not... apparently both are valid
                      data mustEqual geoms.map(g => features.head.getAttribute(g).toString.replace("MULTIPOINT ((0 0), (2 3))", "MULTIPOINT (0 0, 2 3)"))
                    }
                  }
                }
              } else if (encoding == GeometryEncoding.GeoParquetNative) {
                // TODO find a 3rd party java library we can verify native encoding with
                // geopandas seems to be the only thing that currently reads GeoParquet native encoding
              }
              // verify GeoParquet metadata
              WithClose(ParquetFileReader.open(HadoopInputFile.fromPath(path, context.conf))) { reader =>
                val meta = reader.getFileMetaData.getKeyValueMetaData
                val geo = Option(meta.get(GeoParquetMetadata.GeoParquetMetadataKey)).map(new JSONObject(_)).orNull
                geo must not(beNull)
                geoParquetSchema.validate(geo) must not(throwAn[Exception])
                val cols = geo.getJSONObject("columns")
                if (encoding == GeometryEncoding.GeoMesaV1) {
                  cols.length() mustEqual 2
                  val geom = cols.getJSONObject("geom")
                  geom.getString("encoding") mustEqual "point"
                  geom.getJSONArray("geometry_types").asScala.toSeq mustEqual Seq("Point")
                  geom.getJSONArray("bbox").asScala.map(_.asInstanceOf[Number].doubleValue()) mustEqual Seq(40d, 50d, 44d, 54d)
                  geom.has("covering") must beFalse

                  val wkb = cols.getJSONObject("g")
                  wkb.getString("encoding") mustEqual "WKB"
                  wkb.getJSONArray("geometry_types").asScala.toSeq must beEmpty
                  wkb.getJSONArray("bbox").asScala.map(_.asInstanceOf[Number].doubleValue()) mustEqual Seq(-2d, -1d, 42d, 32d)
                  val covering = wkb.getJSONObject("covering").getJSONObject("bbox")
                  foreach(Seq("xmin", "ymin", "xmax", "ymax")) { corner =>
                    covering.getJSONArray(corner).toString mustEqual s"""["__g_bbox__","$corner"]"""
                  }
                } else {
                  cols.length() mustEqual 7
                  val geoms = Seq("line", "mpt", "poly", "mline", "mpoly", "g", "geom")
                  foreach(geoms) { geom =>
                    val binding = sft.getDescriptor(geom).getType.getBinding
                    val col = cols.getJSONObject(geom)
                    if (encoding == GeometryEncoding.GeoParquetWkb || binding == classOf[Geometry]) {
                      col.getString("encoding") mustEqual "WKB"
                    } else {
                      col.getString("encoding") mustEqual binding.getSimpleName.toLowerCase(Locale.US)
                    }
                    if (binding == classOf[Geometry]) {
                      col.getJSONArray("geometry_types").asScala.toSeq must beEmpty
                    } else {
                      col.getJSONArray("geometry_types").asScala.toSeq mustEqual Seq(binding.getSimpleName)
                    }
                    if (encoding == GeometryEncoding.GeoParquetNative && binding == classOf[Point]) {
                      col.has("covering") must beFalse
                    } else {
                      val covering = col.getJSONObject("covering").getJSONObject("bbox")
                      foreach(Seq("xmin", "ymin", "xmax", "ymax")) { corner =>
                        covering.getJSONArray(corner).toString mustEqual s"""["__${geom}_bbox__","$corner"]"""
                      }
                    }
                    val bbox = col.getJSONArray("bbox").asScala.toSeq
                    bbox must haveLength(4)
                    foreach(bbox)(_ must beAnInstanceOf[Number])
                  }
                }
              }
            }
          }
        }
      }
    }

    "read and write features with visibilities" in {
      val sft = SimpleFeatureTypes.createType("parquet-test", "*geom:Point:srid=4326,name:String,age:Int,dtg:Date")

      val features = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(1, s"name$i")
        sf.setAttribute(2, s"$i")
        sf.setAttribute(3, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(0, s"POINT(4$i 5$i)")
        SecurityUtils.setFeatureVisibility(sf, if (i % 2 == 0) "user" else "user&admin")
        sf
      }

      val testCases = Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name")).flatMap { transforms =>
        Seq(
          ("INCLUDE", transforms, features),
          ("IN('0', '2')", transforms, Seq(features(0), features(2))),
          ("bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.dropRight(2)),
          ("bbox(geom,42,48,52,62) and dtg DURING 2013-12-15T00:00:00.000Z/2014-01-15T00:00:00.000Z", transforms, features.drop(2)),
          ("bbox(geom,42,48,52,62)", transforms, features.drop(2)),
          ("dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.dropRight(2)),
          ("name = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, features.slice(5, 6)),
          ("name < 'name5'", transforms, features.take(5)),
          ("name = 'name5'", transforms, features.slice(5, 6)),
          ("age < 5", transforms, features.take(5)),
        )
      }

      withTestDir { dir =>
        val config = new Configuration(this.config)
        config.set(AuthsParam.key, "user,admin")
        val context = FileSystemContext(dir, config)
        val metadata =
          new FileBasedMetadataFactory()
            .create(context, Map.empty, Metadata(sft, "parquet", scheme, leafStorage = true))

        WithClose(new ParquetFileSystemStorageFactory().apply(context, metadata)) { storage =>

          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[String, FileSystemWriter]

          features.foreach { f =>
            val partition = storage.metadata.scheme.getPartitionName(f)
            val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
            writer.write(f)
          }

          writers.foreach(_._2.close())

          logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

          foreach(testCases) { case (filter, transforms, expected) =>
            testQuery(storage, sft)(filter, transforms, expected)
          }
        }

        // verify we can load an existing storage, without specifying vis
        val loaded = new FileBasedMetadataFactory().load(context)
        loaded must beSome
        foreach(Seq("user", "")) { auths =>
          val config = new Configuration(this.config)
          config.set(AuthsParam.key, auths)
          WithClose(new ParquetFileSystemStorageFactory().apply(FileSystemContext(dir, config), loaded.get)) { storage =>
            foreach(testCases) { case (filter, transforms, expected) =>
              val isVisible = VisibilityUtils.visible(Some(new DefaultAuthorizationsProvider(auths.split(",").filter(_.nonEmpty))))
              testQuery(storage, sft)(filter, transforms, expected.filter(isVisible))
            }
          }
        }
      }
    }

    "modify and delete features" in {
      val sft = SimpleFeatureTypes.createType("parquet-test", "*geom:Point:srid=4326,name:String,age:Int,dtg:Date")

      val features = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(1, s"name$i")
        sf.setAttribute(2, s"$i")
        sf.setAttribute(3, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(0, s"POINT(4$i 5$i)")
        sf
      }

      withTestDir { dir =>
        val context = FileSystemContext(dir, config)
        val metadata =
          new FileBasedMetadataFactory()
              .create(context, Map.empty, Metadata(sft, "parquet", scheme, leafStorage = true))
        WithClose(new ParquetFileSystemStorageFactory().apply(context, metadata)) { storage =>
          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[String, FileSystemWriter]

          features.foreach { f =>
            val partition = storage.metadata.scheme.getPartitionName(f)
            val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
            writer.write(f)
          }

          writers.foreach(_._2.close())

          logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

          testQuery(storage, sft)("INCLUDE", null, features)

          val updater = storage.getWriter(Filter.INCLUDE)

          updater.hasNext must beTrue
          while (updater.hasNext) {
            val feature = updater.next
            if (feature.getID == "0") {
              updater.remove()
            } else if (feature.getID == "1") {
              feature.setAttribute(1, "name-updated")
              updater.write()
            }
          }
          updater.close()

          val updates = features.drop(2) :+ {
            val mod = ScalaSimpleFeature.copy(features.drop(1).head)
            mod.setAttribute("name", "name-updated")
            mod
          }

          testQuery(storage, sft)("INCLUDE", null, updates)
        }
      }
    }

    "use custom file observers" in {
      val userData = s"${StorageKeys.ObserversKey}=${classOf[TestObserverFactory].getName}"
      val sft = SimpleFeatureTypes.createType("parquet-test",
        s"*geom:Point:srid=4326,name:String,age:Int,dtg:Date;$userData")

      val features = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(1, s"name$i")
        sf.setAttribute(2, s"$i")
        sf.setAttribute(3, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(0, s"POINT(4$i 5$i)")
        sf
      }

      withTestDir { dir =>
        val context = FileSystemContext(dir, config)
        val metadata =
          new FileBasedMetadataFactory()
              .create(context, Map.empty, Metadata(sft, "parquet", scheme, leafStorage = true))
        WithClose(new ParquetFileSystemStorageFactory().apply(context, metadata)) { storage =>
          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[String, FileSystemWriter]

          features.foreach { f =>
            val partition = storage.metadata.scheme.getPartitionName(f)
            val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
            writer.write(f)
          }

          TestObserverFactory.observers must haveSize(3) // 3 partitions due to our data and scheme
          forall(TestObserverFactory.observers)(_.closed must beFalse)

          writers.foreach(_._2.close())
          forall(TestObserverFactory.observers)(_.closed must beTrue)
          TestObserverFactory.observers.flatMap(_.features) must containTheSameElementsAs(features)
          TestObserverFactory.observers.clear()

          logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

          val updater = storage.getWriter(Filter.INCLUDE)

          updater.hasNext must beTrue
          while (updater.hasNext) {
            val feature = updater.next
            if (feature.getID == "0") {
              updater.remove()
            } else if (feature.getID == "1") {
              feature.setAttribute(1, "name-updated")
              updater.write()
            }
          }

          TestObserverFactory.observers must haveSize(2) // 2 partitions were updated
          forall(TestObserverFactory.observers)(_.closed must beFalse)

          updater.close()

          forall(TestObserverFactory.observers)(_.closed must beTrue)
          TestObserverFactory.observers.flatMap(_.features) must haveLength(2)
        }
      }
    }

    "write files with a target size" in {
      val sft = SimpleFeatureTypes.createType("parquet-test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

      val features = (0 until 10000).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name${i % 10}")
        sf.setAttribute(1, s"${i % 10}")
        sf.setAttribute(2, f"2014-01-${i % 10 + 1}%02dT00:00:01.000Z")
        sf.setAttribute(3, s"POINT(4${i % 10} 5${i % 10})")
        sf
      }

      // note: this is somewhat of a magic number, in that it works the first time through with no remainder
      val targetSize = 3600L

      withTestDir { dir =>
        val context = FileSystemContext(dir, config)
        val metadata =
          new FileBasedMetadataFactory()
              .create(context, Map.empty, Metadata(sft, "parquet", scheme, leafStorage = true, Some(targetSize)))
        WithClose(new ParquetFileSystemStorageFactory().apply(context, metadata)) { storage =>
          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[String, FileSystemWriter]

          features.foreach { f =>
            val partition = storage.metadata.scheme.getPartitionName(f)
            val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
            writer.write(f)
          }

          writers.foreach(_._2.close())

          logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

          val partitions = storage.getPartitions.map(_.name)
          partitions must haveLength(writers.size)
          foreach(partitions) { partition =>
            val paths = storage.getFilePaths(partition)
            paths.size must beGreaterThan(1)
            foreach(paths)(p => context.fs.getFileStatus(p.path).getLen must beCloseTo(targetSize, targetSize / 10))
          }
        }
      }
    }

    "read old point geometries" in {
      foreach(Seq("2.3.0", "5.2.2")) { version =>
        val url = getClass.getClassLoader.getResource(s"data/$version/example-csv/")
        url must not(beNull)
        val path = new Path(url.toURI)
        val context = FileSystemContext(path, config)
        val metadata = StorageMetadataFactory.load(context).orNull
        metadata must not(beNull)
        WithClose(FileSystemStorageFactory(context, metadata)) { storage =>
          val features = SelfClosingIterator(storage.getReader(new Query("example-csv"))).toList.sortBy(_.getID)
          features must haveLength(3)
          features.map(_.getAttribute("name").asInstanceOf[String]) mustEqual Seq("Harry", "Hermione", "Severus")
          features.map(_.getAttribute("geom").toString) mustEqual
            Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)")

          val testCases = Seq(null, Array("geom"), Array("geom", "lastseen"), Array("geom", "name")).flatMap { transforms =>
            Seq(
              ("INCLUDE", transforms, features),
              ("IN('23623', '3233')", transforms, features.take(1) ++ features.takeRight(1)),
              ("bbox(geom,-105,-65,45,25) and lastseen DURING 2015-01-01T00:00:00.000Z/2015-08-01T00:00:00.000Z", transforms, features.take(2)),
              ("bbox(geom,-105,-65,0,25) and lastseen DURING 2015-01-01T00:00:00.000Z/2015-08-01T00:00:00.000Z", transforms, features.take(1)),
              ("bbox(geom,-105,-65,0,25)", transforms, features.take(1)),
              ("lastseen DURING 2015-01-01T00:00:00.000Z/2015-08-01T00:00:00.000Z", transforms, features.take(2)),
              ("name like 'H%'", transforms, features.take(2)),
              ("name = 'Harry'", transforms, features.take(1)),
              ("age > 22", transforms, features.drop(1)),
            )
          }
          foreach(testCases) { case (filter, transform, expected) =>
            val query = new Query("example-csv", ECQL.toFilter(filter))
            if (transform != null) {
              query.setPropertyNames(transform: _*)
            }
            val result = SelfClosingIterator(storage.getReader(query)).toList.sortBy(_.getID)
            if (transform == null) {
              result mustEqual expected
            } else {
              result.map(_.getID) mustEqual expected.map(_.getID)
              result.map(_.getAttributeCount).distinct mustEqual Seq(transform.length)
              foreach(transform) { prop =>
                result.map(_.getAttribute(prop)) mustEqual expected.map(_.getAttribute(prop))
              }
            }
          }
        }
      }
    }

    "read old non-point geometries" in {
      foreach(Seq("lines", "polygons")) { geoms =>
        val typeName = s"example-csv-$geoms"
        val url = getClass.getClassLoader.getResource(s"data/5.2.2/$typeName/")
        url must not(beNull)
        val path = new Path(url.toURI)
        val context = FileSystemContext(path, config)
        val metadata = StorageMetadataFactory.load(context).orNull
        metadata must not(beNull)
        WithClose(FileSystemStorageFactory(context, metadata)) { storage =>
          val features = SelfClosingIterator(storage.getReader(new Query(typeName))).toList.sortBy(_.getID)
          features must haveLength(3)
          features.map(_.getID) mustEqual Seq("1", "2", "3")
          features.map(_.getAttribute("name").asInstanceOf[String]) mustEqual Seq("amy", "bob", "carl")

          val testCases = Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name")).flatMap { transforms =>
            Seq(
              ("INCLUDE", transforms, features),
              ("IN('1', '3')", transforms, features.take(1) ++ features.drop(2)),
              ("bbox(geom,5,5,50,50) and dtg DURING 2015-04-01T00:00:00.000Z/2015-06-01T00:00:00.000Z", transforms, features.take(1)),
              ("bbox(geom,5,5,50,50) and dtg DURING 2015-04-01T00:00:00.000Z/2015-07-01T00:00:00.000Z", transforms, features),
              ("bbox(geom,19,9,21,11)", transforms, features.slice(1, 2)),
              ("dtg DURING 2015-06-01T00:00:00.000Z/2015-06-03T00:00:00.000Z", transforms, features.drop(1)),
              ("name like '%a%'", transforms, features.take(1) ++ features.drop(2)),
              ("name = 'bob'", transforms, features.slice(1, 2)),
            )
          }
          foreach(testCases) { case (filter, transform, expected) =>
            val query = new Query(typeName, ECQL.toFilter(filter))
            if (transform != null) {
              query.setPropertyNames(transform: _*)
            }
            val result = SelfClosingIterator(storage.getReader(query)).toList.sortBy(_.getID)
            if (transform == null) {
              result mustEqual expected
            } else {
              result.map(_.getID) mustEqual expected.map(_.getID)
              result.map(_.getAttributeCount).distinct mustEqual Seq(transform.length)
              foreach(transform) { prop =>
                result.map(_.getAttribute(prop)) mustEqual expected.map(_.getAttribute(prop))
              }
            }
          }
        }
      }
    }
  }

  def withTestDir[R](code: Path => R): R = {
    val file = new Path(Files.createTempDirectory("gm-parquet-test").toUri)
    try { code(file) } finally {
      file.getFileSystem(new Configuration).delete(file, true)
    }
  }

  def testQuery(storage: FileSystemStorage,
      sft: SimpleFeatureType)
      (filter: String,
          transforms: Array[String],
          results: Seq[SimpleFeature]): MatchResult[Any] = {
    val query = new Query(sft.getTypeName, ECQL.toFilter(filter), transforms: _*)
    val features = {
      val iter = SelfClosingIterator(storage.getReader(query))
      // note: need to copy features in iterator as same object is re-used
      iter.map(ScalaSimpleFeature.copy).toList
    }
    val attributes = Option(transforms).getOrElse(sft.getAttributeDescriptors.asScala.map(_.getLocalName).toArray)
    features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
    forall(features) { feature =>
      feature.getAttributes must haveLength(attributes.length)
      forall(attributes.zipWithIndex) { case (attribute, i) =>
        feature.getAttribute(attribute) mustEqual feature.getAttribute(i)
        feature.getAttribute(attribute) mustEqual results.find(_.getID == feature.getID).get.getAttribute(attribute)
      }
      SecurityUtils.getVisibility(feature) mustEqual SecurityUtils.getVisibility(results.find(_.getID == feature.getID).get)
    }
  }
}
