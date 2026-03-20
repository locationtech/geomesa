/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.common.StorageKeys
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Geometry
import org.slf4j.LoggerFactory
import org.specs2.matcher.Matcher
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

import java.io.{File, IOException}
import java.nio.file.Files
import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

@RunWith(classOf[JUnitRunner])
class FileSystemDataStoreTest extends Specification with BeforeAfterAll with LazyLogging {

  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  sequential

  def createFormat(geom: String = "Point", createGeom: Int => String = createPoint): (SimpleFeatureType, Seq[SimpleFeature]) = {
    val sft = SimpleFeatureTypes.createType("parquet", s"name:String,age:Int,dtg:Date,*geom:$geom:srid=4326")
    sft.setScheme(Seq("daily"))
    val features = Seq.tabulate(10) { i =>
      ScalaSimpleFeature.create(sft, s"$i", s"test$i", 100 + i, s"2017-06-0${5 + (i % 3)}T04:03:02.0001Z", createGeom(i))
    }
    (sft, features)
  }

  private def createPoint(i: Int): String = s"POINT(10 10.$i)"
  private def createLine(i: Int): String = s"LINESTRING(10 10, 11 12.$i)"
  private def createPolygon(i: Int): String = s"POLYGON((3$i 28, 41 28, 41 29, 3$i 29, 3$i 28))"

  private val beUUID: Matcher[Any] = (
    (_: Any) match {
      case s: String =>
        try {
          java.util.UUID.fromString(s); true
        } catch {
          case _: IllegalArgumentException => false
        }
      case _ => false
    },
    (_: Any) + " is not valid UUID"
  )

  private val (sft, features) = createFormat()

  private var dir: File = _

  private val filters = Seq(
    "INCLUDE",
    s"name IN ${(0 until 10).mkString("('test", "','test", "')")}",
    "bbox(geom, 5, 5, 15, 15)",
    "dtg DURING 2017-06-05T04:03:00.0000Z/2017-06-07T04:04:00.0000Z",
    "dtg > '2017-06-05T04:03:00.0000Z' AND dtg < '2017-06-07T04:04:00.0000Z'",
    "dtg DURING 2017-06-05T04:03:00.0000Z/2017-06-07T04:04:00.0000Z and bbox(geom, 5, 5, 15, 15)"
  ).map(ECQL.toFilter)

  private val gzip = "<configuration><property><name>parquet.compression</name><value>gzip</value></property></configuration>"

  private val container =
    new PostgreSQLContainer(DockerImageName.parse("postgres").withTag(sys.props("postgres.docker.tag")).asCompatibleSubstituteFor("postgres"))
      .withDatabaseName("postgres") // if we don't set the default db/name to postgres, the startup check fails as it restarts 3 times instead of the expected 2
      .withUsername("postgres")

  private lazy val jdbcConfig =
    s"""jdbc.url=${container.getJdbcUrl}
       |jdbc.user=${container.getUsername}
       |jdbc.password=${container.getPassword}
       |""".stripMargin

  private lazy val dsParams = Seq(
    Map(
      "fs.path" -> s"${dir.getPath}/file",
      "fs.metadata.type" -> "file",
      "fs.config.xml" -> gzip
    ),
    Map(
      "fs.path" -> s"${dir.getPath}/jdbc",
      "fs.metadata.type" -> "jdbc",
      "fs.metadata.config" -> jdbcConfig,
      "fs.config.xml" -> gzip
    ),
  )

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("fsds-test").toFile
    if (logger.underlying.isDebugEnabled()) {
      container.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("postgres")))
      container.setCommand("postgres", "-c", "fsync=off", "-c", "log_statement=all")
    }
    container.start()
  }

  override def afterAll(): Unit = {
    if (dir != null) {
      FileUtils.deleteDirectory(dir)
    }
    container.stop()
  }

  "FileSystemDataStore" should {
    "create a DS" >> {
      foreach(dsParams) { params =>
        WithClose(DataStoreFinder.getDataStore(params.asJava).asInstanceOf[FileSystemDataStore]) { ds =>
          ds.createSchema(sft)

          WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }

          val expected = Set(Set("800043aa"), Set("800043ab"), Set("800043ac"))// TODO verify these correspond to Seq("2017/06/05", "2017/06/06", "2017/06/07")
          val partitions = ds.storage(sft.getTypeName).metadata.getFiles().map(_.partition).toSet
          partitions must haveLength(3)
          partitions.map(_.values.map(_.value)) mustEqual expected

          ds.getTypeNames must have size 1
          val fs = ds.getFeatureSource(sft.getTypeName)
          fs must not(beNull)

          // This shows that the FeatureSource doing the writing has an up-to-date view of the metadata
          fs.getCount(Query.ALL) must beEqualTo(10)
          fs.getBounds must equalTo(new ReferencedEnvelope(10.0, 10.0, 10.0, 10.9, CRS_EPSG_4326))

          val results = CloseableIterator(fs.getFeatures(new Query(sft.getTypeName)).features()).toList
          results must containTheSameElementsAs(features)

          // This shows that a new FeatureSource has a correct view of the metadata on disk
          WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds2 =>
            val fs2 = ds2.getFeatureSource(sft.getTypeName)
            fs2.getCount(Query.ALL) must beEqualTo(10)
            fs2.getBounds must equalTo(new ReferencedEnvelope(10.0, 10.0, 10.0, 10.9, CRS_EPSG_4326))
          }
        }
      }
    }

    "not modify feature type in create schema" >> {
      sft.getUserData.get(StorageKeys.SchemeKey) mustEqual "daily"
    }

    "create a second ds with the same path" >> {
      foreach(dsParams) { params =>
        WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
          ds.getTypeNames.toList must containTheSameElementsAs(Seq(sft.getTypeName))
          val results = CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList
          results must containTheSameElementsAs(features)
        }
      }
    }

    "query with multiple threads" >> {
      foreach(dsParams) { params =>
        WithClose(DataStoreFinder.getDataStore((params ++ Map("fs.read-threads" -> "4")).asJava)) { ds =>
          ds.getTypeNames.toList must containTheSameElementsAs(Seq(sft.getTypeName))
          val results = CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList
          results must containTheSameElementsAs(features)
        }

        WithClose(DataStoreFinder.getDataStore((params ++ Map("fs.read-threads" -> "4", "namespace" -> "ns0")).asJava)) { dsWithNs =>
          val name = dsWithNs.getSchema(sft.getTypeName).getName
          name.getNamespaceURI mustEqual "ns0"
          name.getLocalPart mustEqual sft.getTypeName

          val queries = Seq(
            new Query(sft.getTypeName),
            new Query(sft.getTypeName, Filter.INCLUDE, "geom")
          )
          foreach(queries) { query =>
            val reader = dsWithNs.getFeatureReader(query, Transaction.AUTO_COMMIT)
            reader.getFeatureType.getName mustEqual name
            val features = CloseableIterator(reader).toList
            features must not(beEmpty)
            foreach(features)(_.getFeatureType.getName mustEqual name)
          }
        }
      }
    }

    "support query timeouts" >> {
      foreach(dsParams) { params =>
        WithClose(DataStoreFinder.getDataStore((params ++ Map("fs.read-threads" -> "2", "geomesa.query.timeout" -> "200ms")).asJava)) { ds =>
          ds.getTypeNames.toList must containTheSameElementsAs(Seq(sft.getTypeName))
          val reader = ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)
          try {
            eventually(10, 200.millis) {
              reader.hasNext must beTrue
              reader.next() must throwA[RuntimeException]
            }
          } finally {
            reader.close()
          }
        }
      }
    }

    "call create schema on existing type" >> {
      foreach(dsParams) { params =>
        WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
          val sameSft = SimpleFeatureTypes.createType(sft.getTypeName, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
          ds.createSchema(sameSft) must not(throwA[Throwable])
        }
      }
    }

    "reject schemas with reserved words" >> {
      foreach(dsParams) { params =>
        val reserved = SimpleFeatureTypes.createType("reserved", "dtg:Date,*point:Point:srid=4326")
        reserved.setScheme(Seq("daily"))
        WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
          ds.createSchema(reserved) must throwAn[IllegalArgumentException]
          ds.getSchema(reserved.getTypeName) must throwAn[IOException] // content data store schema does not exist
        }
      }
    }

    "support transforms" >> {
      val transforms = Seq(null, Array("name"), Array("dtg", "geom"))
      foreach(dsParams) { params =>
        WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
          filters.foreach { filter =>
            transforms.foreach { transform =>
              val query = new Query(sft.getTypeName, filter, transform: _*)
              val results = CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
              results must haveLength(features.length)
              if (transform == null) {
                results must containTheSameElementsAs(features)
              } else {
                results.map(_.getID) must containTheSameElementsAs(features.map(_.getID))
                results.foreach { result =>
                  result.getAttributeCount mustEqual transform.length
                  val matched = features.find(_.getID == result.getID).get
                  transform.foreach(t => result.getAttribute(t) mustEqual matched.getAttribute(t))
                }
              }
            }
          }
        }
        ok
      }
    }

    "support append without fid" >> {
      foreach(dsParams) { params =>
        val dir = Files.createTempDirectory("fsds-test-append-without-fid").toFile
        try {
          WithClose(DataStoreFinder.getDataStore((params ++ Map("fs.path" -> dir.getPath)).asJava)) { ds =>
            ds.createSchema(sft)
            WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
              features.foreach { feature =>
                val featureWithEmptyFid = ScalaSimpleFeature.copy(feature)
                featureWithEmptyFid.setId("")
                FeatureUtils.write(writer, featureWithEmptyFid)
              }
            }
            val results = CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList
            results.map(_.getID) must contain(allOf(beUUID))
          }
        } finally {
          FileUtils.deleteDirectory(dir)
        }
      }
    }

    "support updates" >> {
      foreach(dsParams) { params =>
        WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
          WithClose(ds.getFeatureWriter(sft.getTypeName, ECQL.toFilter("IN ('0', '1', '2')"), Transaction.AUTO_COMMIT)) { writer =>
            def modify(f: SimpleFeature): Unit = {
              f.getID match {
                case "0" => writer.remove()
                case "1" => f.setAttribute("dtg", "2017-06-05T04:03:02.0001Z"); writer.write() // note: move partition
                case "2" => f.setAttribute("name", "test0"); writer.write()
              }
            }
            foreach(0 to 2) { _ =>
              writer.hasNext must beTrue
              modify(writer.next)
              ok
            }
            writer.hasNext must beFalse
          }

          val expected = features.drop(1).map(ScalaSimpleFeature.copy)
          expected.head.setAttribute("dtg", "2017-06-05T04:03:02.0001Z")
          expected(1).setAttribute("name", "test0")

          foreach(filters) { filter =>
            val query = new Query(sft.getTypeName, filter)
            val results = CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
            results must containTheSameElementsAs(expected)
          }
        }
      }
    }

    "support different geometry types" in {
      val types = Seq(
        ("LineString", createLine _),
        ("Polygon",    createPolygon _),
        ("Geometry",   (i: Int) => if (i % 2 == 0) { createLine(i) } else { createPoint(i) })
      )

      val all = types.flatMap { case (geom, createGeom) =>
        val (sft, format) = createFormat(geom, createGeom)
        sft.getUserData.put("geomesa.mixed.geometries", "true")
        dsParams.map(p => (sft, format, p))
      }

      foreach(all) { case (sft, features, params) =>
        val dir = Files.createTempDirectory("fsds-test").toFile
        try {
          WithClose(DataStoreFinder.getDataStore((params ++ Map("fs.path" -> dir.getPath)).asJava)) { ds =>
            ds must not(beNull)
            ds.createSchema(sft)
            WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
              features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
            }

            ds.getTypeNames must have size 1
            val fs = ds.getFeatureSource(sft.getTypeName)
            fs must not(beNull)

            // verify metadata - count and bounds
            fs.getCount(Query.ALL) mustEqual 10
            val env = new ReferencedEnvelope(CRS_EPSG_4326)
            features.foreach(f => env.expandToInclude(f.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal))
            fs.getBounds mustEqual env

            foreach(Seq("INCLUDE", s"bbox(geom,${env.getMinX},${env.getMinY},${env.getMaxX},${env.getMaxY})")) { filter =>
              val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
              CloseableIterator(fs.getFeatures(query).features()).toList must containTheSameElementsAs(features)
              val transform = new Query(sft.getTypeName, ECQL.toFilter(filter), "dtg", "geom")
              val transformSft = SimpleFeatureTypes.createType(sft.getTypeName,
                s"dtg:Date,*geom:${sft.getGeometryDescriptor.getType.getBinding.getSimpleName}")
              CloseableIterator(fs.getFeatures(transform).features()).toList must
                  containTheSameElementsAs(features.map(ScalaSimpleFeature.retype(transformSft, _)))
            }
          }
        } finally {
          FileUtils.deleteDirectory(dir)
        }
      }
    }
  }
}
