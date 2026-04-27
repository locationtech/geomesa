/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
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
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.core.metadata.FileBasedMetadataCatalog
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, FileSystemStorage, Partition, StorageKeys, StorageMetadataCatalog}
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorageFactory
import org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.fs.storage.parquet.io.{GeoParquetMetadata, SimpleFeatureParquetSchema}
import org.locationtech.geomesa.security.{AuthsParam, DefaultAuthorizationsProvider, SecurityUtils, VisibilityUtils}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.{Geometry, Point}
import org.specs2.matcher.MatchResult
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.sql.DriverManager
import java.util.{Locale, UUID}

class ParquetStorageTest extends SpecificationWithJUnit with BeforeAfterAll with LazyLogging {

  import scala.collection.JavaConverters._

  // 8 bits resolution creates 3 partitions with our test data
  val schemes = Seq("z2:bits=8")

  lazy val geoParquetSchema = WithClose(getClass.getClassLoader.getResourceAsStream("geoparquet-1.1.0-schema.json")) { is =>
    SchemaLoader.load(new JSONObject(new JSONTokener(is)))
  }

  private val container =
    new PostgreSQLContainer(DockerImageName.parse("postgres").withTag(sys.props("postgres.docker.tag")).asCompatibleSubstituteFor("postgres"))
      .withDatabaseName("postgres") // if we don't set the default db/name to postgres, the startup check fails as it restarts 3 times instead of the expected 2
      .withUsername("postgres")

  private lazy val config = Map(
    "parquet.compression" -> "gzip",
    "fs.metadata.jdbc.url" -> container.getJdbcUrl,
    "fs.metadata.jdbc.user" -> container.getUsername,
    "fs.metadata.jdbc.password" -> container.getPassword,
  )

  override def beforeAll(): Unit = container.start()

  override def afterAll(): Unit = container.stop()

  "ParquetFileSystemStorage" should {
    "read and write features" in {
      val sft = SimpleFeatureTypes.createType("parquet-test", "*geom:Point:srid=4326,name:String:fs.bounds=true,age:Int,dtg:Date")

      val features = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(1, s"name$i")
        sf.setAttribute(2, s"$i")
        sf.setAttribute(3, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(0, s"POINT(4$i 5$i)")
        sf
      }

      foreach(Seq("jdbc", "file")) { metadataType =>
        foreach(Seq(GeometryEncoding.GeoParquetNative, GeometryEncoding.GeoParquetWkb)) { encoding =>
          withTestDir { dir =>
            val geomConfig = Map(SimpleFeatureParquetSchema.GeometryEncodingKey -> encoding.toString)
            val metaConfig = Map(StorageMetadataCatalog.MetadataTypeConfig -> metadataType)
            val context = FileSystemContext.create(dir, config ++ geomConfig ++ metaConfig)
            val catalog = StorageMetadataCatalog(context)
            val metadata = catalog.create(sft, schemes)
            WithClose(new ParquetFileSystemStorageFactory().apply(context, metadata)) { storage =>
              storage must not(beNull)

              val writers = scala.collection.mutable.Map.empty[Partition, FileSystemWriter]

              features.foreach { f =>
                val partition = Partition(storage.metadata.schemes.map(_.getPartition(f)))
                val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
                writer.write(f)
              }

              writers.foreach(_._2.close())

              logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

              val partitions = storage.metadata.getFiles().map(_.partition).distinct
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
              WithClose(new ParquetFileSystemStorageFactory().apply(context, catalog.load(sft.getTypeName)))(testQuery(_, sft)("INCLUDE", null, features))

              // verify GeoParquet metadata - look for partition 225 so we can verify the expected bounds
              val firstPartitionFile = storage.metadata.getFiles().find(_.partition.values.map(_.value).contains("225")).orNull
              firstPartitionFile must not(beNull)
              WithClose(ParquetFileReader.open(HadoopInputFile.fromPath(new Path(dir.resolve(firstPartitionFile.file)), new Configuration()))) { reader =>
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

      foreach(Seq(GeometryEncoding.GeoParquetNative, GeometryEncoding.GeoParquetWkb)) { encoding =>
        withTestDir { dir =>
          val context = FileSystemContext.create(dir, Map(SimpleFeatureParquetSchema.GeometryEncodingKey -> encoding.toString))
          val metadata = new FileBasedMetadataCatalog(context).create(sft, schemes)
          WithClose(new ParquetFileSystemStorageFactory().apply(context, metadata)) { storage =>
            storage must not(beNull)

            val writers = scala.collection.mutable.Map.empty[Partition, FileSystemWriter]

            features.foreach { f =>
              val partition = Partition(storage.metadata.schemes.map(_.getPartition(f)))
              val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
              writer.write(f)
            }

            writers.foreach(_._2.close())

            logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

            val partitions = storage.metadata.getFiles().map(_.partition).distinct
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

            val firstPartitionFile = storage.metadata.getFiles().find(_.partition.values.map(_.value).contains("225")).orNull
            firstPartitionFile must not(beNull)

            val firstPartitionPath = dir.resolve(firstPartitionFile.file)
            // verify 3rd party integration by reading with DuckDB
            if (encoding == GeometryEncoding.GeoParquetWkb) {
              val geoms = Seq("line", "mpt", "poly", "mline", "mpoly", "g", "geom")
              WithClose(DriverManager.getConnection("jdbc:duckdb:")) { conn =>
                WithClose(conn.createStatement())(_.execute("INSTALL spatial;LOAD spatial;"))
                WithClose(conn.prepareStatement(s"SELECT ${geoms.map(g => s"ST_AsText($g)").mkString(", ")} FROM '$firstPartitionPath';")) { ps =>
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
            WithClose(ParquetFileReader.open(HadoopInputFile.fromPath(new Path(firstPartitionPath), new Configuration()))) { reader =>
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
        val context = FileSystemContext.create(dir, config ++ Map(AuthsParam.key -> "user,admin"))
        val catalog = new FileBasedMetadataCatalog(context)
        val metadata = catalog.create(sft, schemes)
        WithClose(new ParquetFileSystemStorageFactory().apply(context, metadata)) { storage =>

          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[Partition, FileSystemWriter]

          features.foreach { f =>
            val partition = Partition(storage.metadata.schemes.map(_.getPartition(f)))
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
        foreach(Seq("user", "")) { auths =>
          val config =  this.config ++ Map(AuthsParam.key -> auths)
          WithClose(new ParquetFileSystemStorageFactory().apply(FileSystemContext.create(dir, config), catalog.load(sft.getTypeName))) { storage =>
            foreach(testCases) { case (filter, transforms, expected) =>
              val isVisible = VisibilityUtils.visible(new DefaultAuthorizationsProvider(auths.split(",").filter(_.nonEmpty)))
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
        val context = FileSystemContext.create(dir, config)
        val metadata = new FileBasedMetadataCatalog(context).create(sft, schemes)
        WithClose(new ParquetFileSystemStorageFactory().apply(context, metadata)) { storage =>
          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[Partition, FileSystemWriter]

          features.foreach { f =>
            val partition = Partition(storage.metadata.schemes.map(_.getPartition(f)))
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
      val userData = s"${StorageKeys.ObserversKey}='${classOf[TestObserverFactory].getName}'"
      val sft = SimpleFeatureTypes.createType("parquet-test",
        s"*geom:Point:srid=4326,name:String,age:Int,dtg:Date;$userData")

      val features = Seq.tabulate(10) { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(1, s"name$i")
        sf.setAttribute(2, s"$i")
        sf.setAttribute(3, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(0, s"POINT(4$i 5$i)")
        sf
      }

      withTestDir { dir =>
        val context = FileSystemContext.create(dir, config)
        val metadata = new FileBasedMetadataCatalog(context).create(sft, schemes)
        WithClose(new ParquetFileSystemStorageFactory().apply(context, metadata)) { storage =>
          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[Partition, FileSystemWriter]

          features.foreach { f =>
            val partition = Partition(storage.metadata.schemes.map(_.getPartition(f)))
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
            val feature = updater.next()
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
        val context = FileSystemContext.create(dir, config)
        val metadata = new FileBasedMetadataCatalog(context).create(sft, schemes, Some(targetSize))
        WithClose(new ParquetFileSystemStorageFactory().apply(context, metadata)) { storage =>
          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[Partition, FileSystemWriter]

          features.foreach { f =>
            val partition = Partition(storage.metadata.schemes.map(_.getPartition(f)))
            val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
            writer.write(f)
          }

          writers.foreach(_._2.close())

          logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

          val partitions = storage.metadata.getFiles().map(_.partition).distinct
          partitions must haveLength(writers.size)
          foreach(partitions) { partition =>
            val paths = storage.metadata.getFiles(partition)
            paths.size must beGreaterThan(1)
            foreach(paths)(p => storage.fs.size(context.root.resolve(p.file)) must beCloseTo(targetSize, targetSize / 10))
          }
        }
      }
    }
  }

  def withTestDir[R](code: URI => R): R = {
    val file = Files.createTempDirectory("gm-parquet-test").toUri
    try { code(file) } finally {
      FileUtils.deleteDirectory(new File(file))
    }
  }

  def testQuery(storage: FileSystemStorage, sft: SimpleFeatureType)
      (filter: String, transforms: Array[String], results: Seq[SimpleFeature]): MatchResult[Any] = {
    val query = new Query(sft.getTypeName, ECQL.toFilter(filter), transforms: _*)
    val features = {
      val iter = CloseableIterator(storage.getReader(query))
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
