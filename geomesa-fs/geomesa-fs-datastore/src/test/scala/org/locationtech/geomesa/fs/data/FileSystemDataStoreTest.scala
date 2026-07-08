/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.api.filter.sort.SortOrder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.data.container.FsContainerTest
import org.locationtech.geomesa.fs.storage.core.StorageKeys
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Geometry
import org.specs2.matcher.{MatchResult, Matcher}
import org.specs2.mutable.SpecificationWithJUnit

import java.io.{File, IOException}
import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

class FileSystemDataStoreTest extends SpecificationWithJUnit with FsContainerTest with LazyLogging {

  sequential

  def createFormat(geom: String = "Point", createGeom: Int => String = createPoint): (SimpleFeatureType, Seq[SimpleFeature]) = {
    import org.locationtech.geomesa.fs.storage.core.RichSimpleFeatureType
    val sft = SimpleFeatureTypes.createType("parquet", s"name:String:fs.bounds=true,age:Int,dtg:Date,*geom:$geom:srid=4326")
    sft.setScheme("daily")
    val features = Seq.tabulate(10) { i =>
      val sf = ScalaSimpleFeature.create(sft, s"$i", s"test$i", 100 + i, s"2017-06-0${5 + (i % 3)}T04:03:02.0001Z", createGeom(i))
      sf.getUserData.put("geomesa.feature.visibility", "user")
      sf
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

  private val filters = Seq(
    "INCLUDE",
    s"name IN ${(0 until 10).mkString("('test", "','test", "')")}",
    "bbox(geom, 5, 5, 15, 15)",
    "dtg DURING 2017-06-05T04:03:00.0000Z/2017-06-07T04:04:00.0000Z",
    "dtg > '2017-06-05T04:03:00.0000Z' AND dtg < '2017-06-07T04:04:00.0000Z'",
    "dtg DURING 2017-06-05T04:03:00.0000Z/2017-06-07T04:04:00.0000Z and bbox(geom, 5, 5, 15, 15)"
  ).map(ECQL.toFilter)

  "FileSystemDataStore" should {
    "load deprecated hadoop configs" in {
      val params = dsParams ++ Map(
        "iceberg.namespace" -> "deprecatedhaoop",
        "fs.config.xml" -> "<configuration><property><name>config.xml</name><value>test</value></property></configuration>",
        "fs.config.paths" -> new File(getClass.getClassLoader.getResource("test-site.xml").toURI).getAbsolutePath,
      )
      WithClose(DataStoreFinder.getDataStore(params.asJava).asInstanceOf[FileSystemDataStore]) { ds =>
        ds.createSchema(sft)
        ds.storage(sft.getTypeName).context.conf must containAllOf(
          Map(
            "config.xml" -> "test", // from direct data store param
            "test-site" -> "bar", // from test-site.xml
            "geomesa.test" -> "foo", // auto-loaded from core-site.xml on classpath
          ).toSeq
        )
      }
    }

    "create a DS" in {
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[FileSystemDataStore]) { ds =>
        ds.createSchema(sft)

        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        val expected = Set(Seq("2017-06-05"), Seq("2017-06-06"), Seq("2017-06-07"))
        val storage = ds.storage(sft.getTypeName)
        val partitions = storage.metadata.files().scan().map(f => storage.metadata.partition(f)).toSet
        partitions must haveLength(3)
        partitions.map(_.values.map(_.value)) mustEqual expected

        ds.getTypeNames must have size 1
        val fs = ds.getFeatureSource(sft.getTypeName)
        fs must not(beNull)

        // This shows that the FeatureSource doing the writing has an up-to-date view of the metadata
        fs.getCount(Query.ALL) must beEqualTo(10)
        compareBounds(fs.getBounds, new ReferencedEnvelope(10.0, 10.0, 10.0, 10.9, CRS_EPSG_4326))

        val results = CloseableIterator(fs.getFeatures(new Query(sft.getTypeName)).features()).toList
        results must containTheSameElementsAs(features)

        // This shows that a new FeatureSource has a correct view of the metadata on disk
        WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds2 =>
          val fs2 = ds2.getFeatureSource(sft.getTypeName)
          fs2.getCount(Query.ALL) must beEqualTo(10)
          compareBounds(fs2.getBounds, new ReferencedEnvelope(10.0, 10.0, 10.0, 10.9, CRS_EPSG_4326))
        }

        // test stats queries
        ds.stats.getCount(sft) must beSome(10L)
        ds.stats.getCount(sft, exact = true) must beSome(10L)
        val minMax = ds.stats.getMinMax[String](sft, "name").orNull
        minMax must not(beNull)
        minMax.min mustEqual "test0"
        minMax.max mustEqual "test9"
      }
    }

    "not modify feature type in create schema" in {
      sft.getUserData.get(StorageKeys.SchemeKey) mustEqual "daily"
    }

    "create a second ds with the same path" in {
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds =>
        ds.getTypeNames.toList must containTheSameElementsAs(Seq(sft.getTypeName))
        val results = CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList
        results must containTheSameElementsAs(features)
      }
    }

    "query with multiple threads" in {
      WithClose(DataStoreFinder.getDataStore((dsParams ++ Map("geomesa.query.threads" -> "4")).asJava)) { ds =>
        ds.getTypeNames.toList must containTheSameElementsAs(Seq(sft.getTypeName))
        val results = CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList
        results must containTheSameElementsAs(features)
      }
    }

    "support namespaces" in {
      WithClose(DataStoreFinder.getDataStore((dsParams ++ Map("namespace" -> "ns0")).asJava)) { dsWithNs =>
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

    "enforce authorizations" in {
      WithClose(DataStoreFinder.getDataStore(dsParams.filter(_._1 != "geomesa.security.auths").asJava)) { ds =>
        ds.getTypeNames.toList must containTheSameElementsAs(Seq(sft.getTypeName))
        val results = CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList
        results must beEmpty
      }
    }

    "support query timeouts" in {
      WithClose(DataStoreFinder.getDataStore((dsParams ++ Map("geomesa.query.threads" -> "2", "geomesa.query.timeout" -> "200ms")).asJava)) { ds =>
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

    "call create schema on existing type" in {
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds =>
        val sameSft = SimpleFeatureTypes.createType(sft.getTypeName, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
        ds.createSchema(sameSft) must not(throwA[Throwable])
      }
    }

    "reject schemas with reserved words" in {
      import org.locationtech.geomesa.fs.storage.core.RichSimpleFeatureType
      val reserved = SimpleFeatureTypes.createType("reserved", "dtg:Date,*point:Point:srid=4326")
      reserved.setScheme("daily")
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds =>
        ds.createSchema(reserved) must throwAn[IllegalArgumentException]
        ds.getSchema(reserved.getTypeName) must throwAn[IOException] // content data store schema does not exist
      }
    }

    "support transforms" in {
      val transforms = Seq(null, Array("name"), Array("dtg", "geom"))
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds =>
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
        ok
      }
    }

    "support sorting and limiting" in {
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds =>
        foreach(Seq(SortOrder.ASCENDING, SortOrder.DESCENDING)) { sortOrder =>
          val query = new Query(sft.getTypeName)
          query.setSortBy(FilterHelper.ff.sort("name", sortOrder))
          query.setMaxFeatures(1)
          val results = CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
          results must haveSize(1)
          if (sortOrder == SortOrder.ASCENDING) {
            results.head.getID mustEqual "0"
          } else {
            results.head.getID mustEqual "9"
          }
        }
      }
    }

    "support append without fid" in {
      val params = dsParams.map {
        case ("fs.config.properties", props) => "fs.config.properties" -> props.replaceAll("iceberg.namespace=.*", "iceberg.namespace=appendnofid")
        case (k, v) => k -> v
      }
      WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
        ds.createSchema(sft)
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach { feature =>
            val featureWithEmptyFid = ScalaSimpleFeature.copy(feature)
            featureWithEmptyFid.setId(null)
            FeatureUtils.write(writer, featureWithEmptyFid)
          }
        }
        val results = CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList
        results.map(_.getID) must contain(allOf(beUUID))
      }
    }

    "support updates" in {
      WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds =>
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
          val results = CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
          results mustEqual expected
        }
      }
    }

    "support different geometry types" in {
      val types = Seq(
        ("LineString", createLine _),
        ("Polygon",    createPolygon _),
        ("Geometry",   (i: Int) => if (i % 2 == 0) { createLine(i) } else { createPoint(i) })
      )

      val all = types.map { case (geom, createGeom) =>
        val (sft, features) = createFormat(geom, createGeom)
        sft.getUserData.put("geomesa.mixed.geometries", "true")
        val renamed = SimpleFeatureTypes.renameSft(sft, geom)
        val renamedFeatures = features.map(ScalaSimpleFeature.copy(renamed, _))
        (renamed, renamedFeatures)
      }

      foreach(all) { case (sft, features) =>
        WithClose(DataStoreFinder.getDataStore(dsParams.asJava)) { ds =>
          ds must not(beNull)
          ds.getTypeNames.toSeq must not(contain(sft.getTypeName))
          ds.createSchema(sft)
          WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }

          val fs = ds.getFeatureSource(sft.getTypeName)
          fs must not(beNull)

          // verify metadata - count and bounds
          fs.getCount(Query.ALL) mustEqual 10
          val env = new ReferencedEnvelope(CRS_EPSG_4326)
          features.foreach(f => env.expandToInclude(f.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal))
          compareBounds(fs.getBounds, env, 10) // xz2 inversion is not very precise...

          foreach(Seq("INCLUDE", s"bbox(geom,${env.getMinX},${env.getMinY},${env.getMaxX},${env.getMaxY})")) { filter =>
            val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
            CloseableIterator(fs.getFeatures(query).features()).toList.sortBy(_.getID) mustEqual features
            val transform = new Query(sft.getTypeName, ECQL.toFilter(filter), "dtg", "geom")
            val transformSft = SimpleFeatureTypes.createType(sft.getTypeName,
              s"dtg:Date,*geom:${sft.getGeometryDescriptor.getType.getBinding.getSimpleName}")
            CloseableIterator(fs.getFeatures(transform).features()).toList.sortBy(_.getID) mustEqual
              features.map(ScalaSimpleFeature.retype(transformSft, _))
          }
        }
      }
    }
  }

  private def compareBounds(bounds: ReferencedEnvelope, expected: ReferencedEnvelope, delta: Double = 0.01): MatchResult[_] = {
    def toSeq(b: ReferencedEnvelope): Seq[Double] = Seq(b.getMinX, b.getMinY, b.getMaxX, b.getMaxY)
    foreach(toSeq(bounds).zip(toSeq(expected))) { case (b, e) =>
      b must beCloseTo(e, delta)
    }
  }
}
