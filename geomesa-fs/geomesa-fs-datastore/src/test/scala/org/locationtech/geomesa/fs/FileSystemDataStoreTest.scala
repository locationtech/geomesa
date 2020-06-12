/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import java.io.{File, IOException}
import java.nio.file.Files
import java.util.Collections

import org.apache.commons.io.FileUtils
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class FileSystemDataStoreTest extends Specification {

  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  sequential

  def createFormat(
      format: String,
      geom: String = "Point",
      createGeom: Int => String = createPoint): (String, SimpleFeatureType, Seq[SimpleFeature]) = {
    val sft = SimpleFeatureTypes.createType(format, s"name:String,age:Int,dtg:Date,*geom:$geom:srid=4326")
    sft.setScheme("daily")
    sft.setLeafStorage(false)
    val features = Seq.tabulate(10) { i =>
      ScalaSimpleFeature.create(sft, s"$i", s"test$i", 100 + i, s"2017-06-0${5 + (i % 3)}T04:03:02.0001Z", createGeom(i))
    }
    (format, sft, features)
  }

  private def createPoint(i: Int): String = s"POINT(10 10.$i)"
  private def createLine(i: Int): String = s"LINESTRING(10 10, 11 12.$i)"
  private def createPolygon(i: Int): String = s"POLYGON((3$i 28, 41 28, 41 29, 3$i 29, 3$i 28))"

  val encodings = Seq("orc", "parquet")

  val formats = encodings.map(createFormat(_))

  val dirs = scala.collection.mutable.Map.empty[String, File]

  val filters = Seq(
    "INCLUDE",
    s"name IN ${(0 until 10).mkString("('test", "','test", "')")}",
    "bbox(geom, 5, 5, 15, 15)",
    "dtg DURING 2017-06-05T04:03:00.0000Z/2017-06-07T04:04:00.0000Z",
    "dtg > '2017-06-05T04:03:00.0000Z' AND dtg < '2017-06-07T04:04:00.0000Z'",
    "dtg DURING 2017-06-05T04:03:00.0000Z/2017-06-07T04:04:00.0000Z and bbox(geom, 5, 5, 15, 15)"
  ).map(ECQL.toFilter)

  val gzip = "<configuration><property><name>parquet.compression</name><value>gzip</value></property></configuration>"

  step {
    formats.foreach { case (f, _, _) => dirs.put(f, Files.createTempDirectory(s"fsds-test-$f").toFile) }
  }

  "FileSystemDataStore" should {
    "create a DS" >> {
      foreach(formats) { case (format, sft, features) =>
        val dir = dirs(format)
        val dsParams = Map(
          "fs.path" -> dir.getPath,
          "fs.encoding" -> format,
          "fs.config.xml" -> gzip)

        val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[FileSystemDataStore]

        ds.createSchema(sft)

        WithClose(ds.getFeatureWriterAppend(format, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        // metadata
        new File(dir, s"$format/metadata").exists() must beTrue
        new File(dir, s"$format/metadata").isDirectory must beTrue

        val expected = Seq("2017/06/05", "2017/06/06", "2017/06/07")
        ds.storage(sft.getTypeName).getPartitions must haveLength(3)
        ds.storage(sft.getTypeName).getPartitions.map(_.name) must containTheSameElementsAs(expected)
        foreach(expected)(name => new File(dir, s"$format/$name").isDirectory must beTrue)

        ds.getTypeNames must have size 1
        val fs = ds.getFeatureSource(format)
        fs must not(beNull)

        // This shows that the FeatureSource doing the writing has an up-to-date view of the metadata
        fs.getCount(Query.ALL) must beEqualTo(10)
        fs.getBounds must equalTo(new ReferencedEnvelope(10.0, 10.0, 10.0, 10.9, CRS_EPSG_4326))

        val results = SelfClosingIterator(fs.getFeatures(new Query(format)).features()).toList
        results must containTheSameElementsAs(features)

        // This shows that a new FeatureSource has a correct view of the metadata on disk
        val ds2 = DataStoreFinder.getDataStore(dsParams)
        val fs2 = ds2.getFeatureSource(format)
        fs2.getCount(Query.ALL) must beEqualTo(10)
        fs2.getBounds must equalTo(new ReferencedEnvelope(10.0, 10.0, 10.0, 10.9, CRS_EPSG_4326))
      }
    }

    "create a second ds with the same path" >> {
      foreach(formats) { case (format, _, features) =>
        val dir = dirs(format)
        // Load a new datastore to read metadata and stuff
        val ds = DataStoreFinder.getDataStore(Collections.singletonMap("fs.path", dir.getPath))
        ds.getTypeNames.toList must containTheSameElementsAs(Seq(format))

        val results = SelfClosingIterator(ds.getFeatureReader(new Query(format), Transaction.AUTO_COMMIT)).toList
        results must containTheSameElementsAs(features)
      }
    }

    "query with multiple threads" >> {
      foreach(formats) { case (format, sft, features) =>
        val dir = dirs(format)
        // Load a new datastore to read metadata and stuff
        val ds = DataStoreFinder.getDataStore(Map("fs.path" -> dir.getPath, "fs.read-threads" -> "4"))
        ds.getTypeNames.toList must containTheSameElementsAs(Seq(format))

        val results = SelfClosingIterator(ds.getFeatureReader(new Query(format), Transaction.AUTO_COMMIT)).toList
        results must containTheSameElementsAs(features)

        val dsWithNs = DataStoreFinder.getDataStore(Map("fs.path" -> dir.getPath, "fs.read-threads" -> "4", "namespace" -> "ns0"))
        val name = dsWithNs.getSchema(sft.getTypeName).getName
        name.getNamespaceURI mustEqual "ns0"
        name.getLocalPart mustEqual sft.getTypeName

        val queries = Seq(
          new Query(sft.getTypeName),
          new Query(sft.getTypeName, Filter.INCLUDE, Array("geom"))
        )
        foreach(queries) { query =>
          val reader = dsWithNs.getFeatureReader(query, Transaction.AUTO_COMMIT)
          reader.getFeatureType.getName mustEqual name
          val features = SelfClosingIterator(reader).toList
          features must not(beEmpty)
          foreach(features)(_.getFeatureType.getName mustEqual name)
        }
      }
    }

    "call create schema on existing type" >> {
      foreach(formats) { case (format, _, _) =>
        val dir = dirs(format)
        val ds = DataStoreFinder.getDataStore(Collections.singletonMap("fs.path", dir.getPath))
        val sameSft = SimpleFeatureTypes.createType(format, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
        sameSft.setScheme("daily")
        ds.createSchema(sameSft) must not(throwA[Throwable])
      }
    }

    "reject schemas with reserved words" >> {
      foreach(formats) { case (format, _, _) =>
        val dir = dirs(format)
        val reserved = SimpleFeatureTypes.createType("reserved", "dtg:Date,*point:Point:srid=4326")
        reserved.setScheme("daily")
        val ds = DataStoreFinder.getDataStore(Map(
          "fs.path" -> dir.getPath,
          "fs.encoding" -> format,
          "fs.config.xml" -> gzip))
        ds.createSchema(reserved) must throwAn[IllegalArgumentException]
        ds.getSchema(reserved.getTypeName) must throwAn[IOException] // content data store schema does not exist
      }
    }

    "support transforms" >> {
      val transforms = Seq(null, Array("name"), Array("dtg", "geom"))

      foreach(formats) { case (format, _, features) =>
        val dir = dirs(format)
        val ds = DataStoreFinder.getDataStore(Collections.singletonMap("fs.path", dir.getPath))

        filters.foreach { filter =>
          transforms.foreach { transform =>
            val query = new Query(format, filter, transform)
            val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
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

    "support updates" >> {
      foreach(formats) { case (format, _, features) =>
        val dsParams = Map("fs.path" -> dirs(format).getPath, "fs.config.xml" -> gzip)
        val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[FileSystemDataStore]

        WithClose(ds.getFeatureWriter(format, ECQL.toFilter("IN ('0', '1', '2')"), Transaction.AUTO_COMMIT)) { writer =>
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
          val query = new Query(format, filter)
          val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
          results must containTheSameElementsAs(expected)
        }
      }
    }

    "support different geometry types" in {
      val types = Seq(
        ("LineString", createLine _),
        ("Polygon",    createPolygon _),
        ("Geometry",   (i: Int) => if (i % 2 == 0) { createLine(i) } else { createPoint(i) })
      )

      val all = types.flatMap { case (geom, createGeom) => encodings.map(createFormat(_, geom, createGeom)) }

      foreach(all) { case (format, sft, features) =>
        val dir = Files.createTempDirectory(s"fsds-test-$format").toFile
        try {
          val dsParams = Map(
            "fs.path" -> dir.getPath,
            "fs.encoding" -> format,
            "fs.config.xml" -> gzip)

          val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[FileSystemDataStore]
          ds must not(beNull)
          try {
            sft.getUserData.put("geomesa.mixed.geometries", "true")
            ds.createSchema(sft)
            WithClose(ds.getFeatureWriterAppend(format, Transaction.AUTO_COMMIT)) { writer =>
              features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
            }

            ds.getTypeNames must have size 1
            val fs = ds.getFeatureSource(format)
            fs must not(beNull)

            // verify metadata - count and bounds
            fs.getCount(Query.ALL) must beEqualTo(10)
            val env = new ReferencedEnvelope(CRS_EPSG_4326)
            features.foreach(f => env.expandToInclude(f.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal))
            fs.getBounds mustEqual env

            foreach(Seq("INCLUDE", s"bbox(geom,${env.getMinX},${env.getMinY},${env.getMaxX},${env.getMaxY})")) { filter =>
              val query = new Query(format, ECQL.toFilter(filter))
              SelfClosingIterator(fs.getFeatures(query).features()).toList must containTheSameElementsAs(features)
              val transform = new Query(format, ECQL.toFilter(filter), Array("dtg", "geom"))
              val transformSft = SimpleFeatureTypes.createType(format,
                s"dtg:Date,*geom:${sft.getGeometryDescriptor.getType.getBinding.getSimpleName}")
              SelfClosingIterator(fs.getFeatures(transform).features()).toList must
                  containTheSameElementsAs(features.map(ScalaSimpleFeature.retype(transformSft, _)))
            }
          } finally {
            ds.dispose()
          }
        } finally {
          FileUtils.deleteDirectory(dir)
        }
      }
    }
  }

  step {
    dirs.foreach { case (_, dir) => FileUtils.deleteDirectory(dir) }
  }
}
