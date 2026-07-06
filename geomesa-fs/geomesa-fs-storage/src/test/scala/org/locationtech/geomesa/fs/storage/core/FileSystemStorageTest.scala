/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.LocalInputFile
import org.everit.json.schema.loader.SchemaLoader
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.json.{JSONObject, JSONTokener}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.core.FileSystemStorageTest.IcebergRestContainer
import org.locationtech.geomesa.fs.storage.core.fs.S3ObjectStore
import org.locationtech.geomesa.fs.storage.core.parquet.s3.S3InputFile
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeoParquetMetadata
import org.locationtech.geomesa.fs.storage.core.parquet.schema.GeometrySchema.GeometryEncoding
import org.locationtech.geomesa.fs.storage.core.utils.TestObserverFactory
import org.locationtech.geomesa.security.{AuthsParam, DefaultAuthorizationsProvider, SecurityUtils, VisibilityUtils}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Geometry
import org.specs2.matcher.MatchResult
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.{GenericContainer, MinIOContainer, Network}
import org.testcontainers.utility.DockerImageName

import java.io.FileOutputStream
import java.net.URI
import java.nio.file.Files
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Locale, UUID}

class FileSystemStorageTest extends SpecificationWithJUnit with BeforeAfterAll with LazyLogging {

  import scala.collection.JavaConverters._

  // 8 bits resolution creates 3 partitions with our test data
  val schemes = Seq("z2:bits=8")

  private val paths = new AtomicInteger()

  lazy val geoParquetSchema = WithClose(getClass.getClassLoader.getResourceAsStream("geoparquet-1.1.0-schema.json")) { is =>
    SchemaLoader.load(new JSONObject(new JSONTokener(is)))
  }

  private val network = Network.newNetwork()

  private val minio =
    new MinIOContainer(DockerImageName.parse("minio/minio").withTag(sys.props("minio.docker.tag")))
      .withNetwork(network)
      .withNetworkAliases("minio")

  private val iceberg = new IcebergRestContainer().withNetwork(network)

  private lazy val s3Conf = Map(
    "fs.s3.region" -> "us-east-1",
    "fs.s3.endpoint" -> minio.getS3URL,
    "fs.s3.access-key-id" -> minio.getUserName,
    "fs.s3.secret-access-key" -> minio.getPassword,
    "fs.s3.force-path-style" -> "true",
  )

  override def beforeAll(): Unit = {
    minio.start()
    minio.execInContainer("mc", "alias", "set", "localhost", "http://localhost:9000", minio.getUserName, minio.getPassword)
    minio.execInContainer("mc", "mb", "localhost/geomesa")
    iceberg.start()
  }

  override def afterAll(): Unit = {
    iceberg.stop()
    minio.stop()
    network.close()
  }

  // make a valid, unique namespace for each test
  def newPath(): FileSystemContext = {
    val path = f"${paths.getAndIncrement()}%03d"
    val root = URI.create(s"s3://geomesa/$path/")
    val conf = s3Conf ++ Map(
      "type" -> "rest",
      "uri" -> s"http://${iceberg.getHost}:${iceberg.getFirstMappedPort}/",
      "iceberg.namespace" -> path,
    )
    FileSystemContext.create(root, conf, None)
  }

  "FileSystemStorage" should {
    "read and write features" in {
      val sft = SimpleFeatureTypes.createType("parquet-test", "name:String,age:Int,*geom:Point:srid=4326,dtg:Date")

      val features = Seq.tabulate(10) { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, s"$i")
        sf.setAttribute(2, s"POINT(4$i 5$i)")
        sf.setAttribute(3, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf
      }

      val encoding = GeometryEncoding.GeoParquetWkb
      WithClose(StorageCatalog(newPath())) { catalog =>
        WithClose(catalog.create(sft, schemes)) { storage =>
          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[Partition, FileSystemWriter]

          features.foreach { f =>
            val partition = Partition(storage.schemes.map(_.getPartition(f)))
            val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
            writer.write(f)
          }

          writers.foreach(_._2.close())

          logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

          storage.metadata.partitions() must haveLength(writers.size)

          val transformsList = Seq(null, Array("name", "dtg", "geom"), Array("geom"), Array("geom", "dtg"), Array("geom", "name"))

          val doTest = testQuery(storage, sft) _

          foreach(transformsList) { transforms =>
            doTest("INCLUDE", transforms, features)
            doTest("IN('0', '2')", transforms, Seq(features.head, features(2)))
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
          WithClose(catalog.load(sft.getTypeName))(testQuery(_, sft)("INCLUDE", null, features))

          // verify GeoParquet metadata - look for partition e1 so we can verify the expected bounds
          val firstPartitionFile =
            storage.metadata.files().forPartition(Partition(Seq(PartitionKey(storage.schemes.head.name, "e1")))).scan().headOption.orNull
          firstPartitionFile must not(beNull)
          WithClose(S3ObjectStore(s3Conf)) { fs =>
            WithClose(ParquetFileReader.open(new S3InputFile(fs, URI.create(firstPartitionFile.location())))) { reader =>
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
              val covering = col.getJSONObject("covering").getJSONObject("bbox")
              foreach(Seq("xmin", "ymin", "xmax", "ymax")) { corner =>
                covering.getJSONArray(corner).toString mustEqual s"""["__geom_bbox__","$corner"]"""
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

      val features = Seq.tabulate(10) { i =>
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
        sf.setAttribute("list", Seq.tabulate[Integer](i)(i => Int.box(i * 2)))
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

      val encoding = GeometryEncoding.GeoParquetWkb
      WithClose(StorageCatalog(newPath())) { catalog =>
        WithClose(catalog.create(sft, schemes)) { storage =>
          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[Partition, FileSystemWriter]

          features.foreach { f =>
            val partition = Partition(storage.schemes.map(_.getPartition(f)))
            val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
            writer.write(f)
          }

          writers.foreach(_._2.close())

          logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

          storage.metadata.partitions() must haveLength(writers.size)

          val transformsList = Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))

          val doTest = testQuery(storage, sft) _

          transformsList.foreach { transforms =>
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

          val firstPartitionFile =
            storage.metadata.files().forPartition(Partition(Seq(PartitionKey(storage.schemes.head.name, "e1")))).scan().headOption.orNull
          firstPartitionFile must not(beNull)
          // copy to a local file
          val tmpFile = Files.createTempFile("fs", ".parquet")
          try {
            WithClose(S3ObjectStore(s3Conf)) { fs =>
              WithClose(new FileOutputStream(tmpFile.toFile)) { os =>
                WithClose(fs.read(URI.create(firstPartitionFile.location())).orNull) { is =>
                  IOUtils.copy(is, os)
                }
              }
            }

            // verify 3rd party integration by reading with DuckDB
            if (encoding == GeometryEncoding.GeoParquetWkb) {
              val geoms = Seq("line", "mpt", "poly", "mline", "mpoly", "g", "geom")
              WithClose(DriverManager.getConnection("jdbc:duckdb:")) { conn =>
                WithClose(conn.createStatement())(_.execute("INSTALL spatial;LOAD spatial;"))
                WithClose(conn.prepareStatement(s"SELECT ${geoms.map(g => s"ST_AsText($g)").mkString(", ")} FROM '${tmpFile.toFile.getAbsolutePath}';")) { ps =>
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
            WithClose(ParquetFileReader.open(new LocalInputFile(tmpFile))) { reader =>
              val meta = reader.getFileMetaData.getKeyValueMetaData
              val geo = Option(meta.get(GeoParquetMetadata.GeoParquetMetadataKey)).map(new JSONObject(_)).orNull
              geo must not(beNull)
              geoParquetSchema.validate(geo) must not(throwAn[Exception])
              val cols = geo.getJSONObject("columns")
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
                val covering = col.getJSONObject("covering").getJSONObject("bbox")
                foreach(Seq("xmin", "ymin", "xmax", "ymax")) { corner =>
                  covering.getJSONArray(corner).toString mustEqual s"""["__${geom}_bbox__","$corner"]"""
                }
                val bbox = col.getJSONArray("bbox").asScala.toSeq
                bbox must haveLength(4)
                foreach(bbox)(_ must beAnInstanceOf[Number])
              }
            }
          } finally {
            Files.delete(tmpFile)
          }
        }
      }
    }

    "read and write features with visibilities" in {
      val sft = SimpleFeatureTypes.createType("parquet-test", "*geom:Point:srid=4326,name:String,age:Int,dtg:Date")

      val features = Seq.tabulate(10) { i =>
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
          ("IN('0', '2')", transforms, Seq(features.head, features(2))),
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

      val context = newPath()
      WithClose(StorageCatalog(context.copy(conf = context.conf ++ Map(AuthsParam.key -> "user,admin")))) { catalog =>
        WithClose(catalog.create(sft, schemes)) { storage =>
          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[Partition, FileSystemWriter]

          features.foreach { f =>
            val partition = Partition(storage.schemes.map(_.getPartition(f)))
            val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
            writer.write(f)
          }

          writers.foreach(_._2.close())

          logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

          foreach(testCases) { case (filter, transforms, expected) =>
            testQuery(storage, sft)(filter, transforms, expected)
          }
        }
      }
      // verify we can load an existing storage, without specifying vis
      foreach(Seq("user", "")) { auths =>
        WithClose(StorageCatalog(context.copy(conf = context.conf ++ Map(AuthsParam.key -> auths)))) { catalog =>
          WithClose(catalog.load(sft.getTypeName)) { storage =>
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

      val features = Seq.tabulate(10) { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(1, s"name$i")
        sf.setAttribute(2, s"$i")
        sf.setAttribute(3, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(0, s"POINT(4$i 5$i)")
        sf
      }
      val context = newPath()
      WithClose(StorageCatalog(context)) { catalog =>
        WithClose(catalog.create(sft, schemes)) { storage =>
          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[Partition, FileSystemWriter]

          features.foreach { f =>
            val partition = Partition(storage.schemes.map(_.getPartition(f)))
            val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
            writer.write(f)
          }

          writers.foreach(_._2.close())

          logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

          testQuery(storage, sft)("INCLUDE", null, features)

          val updater = storage.getWriter(Filter.INCLUDE, 1)

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

      WithClose(StorageCatalog(newPath())) { catalog =>
        WithClose(catalog.create(sft, schemes)) { storage =>
          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[Partition, FileSystemWriter]

          features.foreach { f =>
            val partition = Partition(storage.schemes.map(_.getPartition(f)))
            val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
            writer.write(f)
          }

          TestObserverFactory.observers must haveSize(3) // 3 partitions due to our data and scheme
          forall(TestObserverFactory.observers)(_.closed must beFalse)

          writers.foreach(_._2.close())

          logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

          forall(TestObserverFactory.observers)(_.closed must beTrue)
          TestObserverFactory.observers.flatMap(_.features) must containTheSameElementsAs(features)

          TestObserverFactory.observers.clear()
          val updater = storage.getWriter(Filter.INCLUDE, 1)

          updater.hasNext must beTrue
          while (updater.hasNext) {
            val feature = updater.next()
            if (feature.getID == "0") {
              updater.remove()
            } else if (feature.getID == "5") {
              feature.setAttribute(1, "name-updated")
              updater.write()
            }
          }

          TestObserverFactory.observers must haveSize(1) // 1 partition was updated (deletes are not passed to observers)
          forall(TestObserverFactory.observers)(_.closed must beFalse)

          updater.close()

          forall(TestObserverFactory.observers)(_.closed must beTrue)
          TestObserverFactory.observers.flatMap(_.features) must haveLength(1)
        }
      }
    }

    "write files with a target size" in {
      val sft = SimpleFeatureTypes.createType("parquet-test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

      val features = Seq.tabulate(10000) { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name${i % 10}")
        sf.setAttribute(1, s"${i % 10}")
        sf.setAttribute(2, f"2014-01-${i % 10 + 1}%02dT00:00:01.000Z")
        sf.setAttribute(3, s"POINT(4${i % 10} 5${i % 10})")
        sf
      }

      // note: this is somewhat of a magic number, in that it works the first time through with no remainder
      val targetSize = 4000L

      WithClose(StorageCatalog(newPath())) { catalog =>
        WithClose(catalog.create(sft, schemes, Some(targetSize))) { storage =>
          storage must not(beNull)

          val writers = scala.collection.mutable.Map.empty[Partition, FileSystemWriter]

          features.foreach { f =>
            val partition = Partition(storage.schemes.map(_.getPartition(f)))
            val writer = writers.getOrElseUpdate(partition, storage.getWriter(partition))
            writer.write(f)
          }

          writers.foreach(_._2.close())

          logger.debug(s"wrote to ${writers.size} partitions for ${features.length} features")

          val partitions = storage.metadata.partitions()
          partitions must haveLength(writers.size)
          foreach(partitions) { partition =>
            val paths = storage.metadata.files().forPartition(partition).scan()
            paths.size must beGreaterThan(1)
            foreach(paths) { p =>
              storage.table.io().newInputFile(p.location()).getLength must beCloseTo(targetSize, targetSize / 10)
            }
          }
        }
      }
    }
  }

  def testQuery(storage: FileSystemStorage, sft: SimpleFeatureType)
      (filter: String, transforms: Array[String], results: Seq[SimpleFeature]): MatchResult[Any] = {
    val query = new Query(sft.getTypeName, ECQL.toFilter(filter), transforms: _*)
    val features = CloseableIterator(storage.getReader(query, 1)).toList
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

object FileSystemStorageTest {
  class IcebergRestContainer
      extends GenericContainer[IcebergRestContainer](DockerImageName.parse("tabulario/iceberg-rest").withTag(sys.props("iceberg.rest.docker.tag"))) {
    withExposedPorts(8181)
    // Override the upstream image's malformed default URI (jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory)
    // `mode=memory` ended up in the filename instead of as a query parameter. Also add a busy_timeout, so transient
    // contention from Iceberg's connection pool doesn't show up as SQLITE_BUSY 500s during multi-table ingests
    withEnv("CATALOG_URI", "jdbc:sqlite:file:/tmp/iceberg_rest.db?journal_mode=WAL&synchronous=NORMAL&busy_timeout=30000")
    withEnv("CATALOG_WAREHOUSE", "s3://geomesa/iceberg/")
    withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
    withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000")
    withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
    withEnv("AWS_REGION", "us-east-1")
    withEnv("AWS_ACCESS_KEY_ID", "minioadmin")
    withEnv("AWS_SECRET_ACCESS_KEY", "minioadmin")
  }
}
