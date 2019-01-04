/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.data

import java.io.{File, FileOutputStream}
import java.net.URL
import java.nio.file.Files

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.DirtyRootAllocator
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileWriter
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowDataStoreTest extends Specification {

  import ArrowDataStoreFactory.{CachingParam, UrlParam}
  import scala.collection.JavaConversions._

  implicit val allocator: BufferAllocator = new DirtyRootAllocator(Long.MaxValue, 6.toByte)

  val sft = SimpleFeatureTypes.createImmutableType("test", "name:String,foo:String,dtg:Date,*geom:Point:srid=4326")

  val features0 = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name0$i", s"foo${i % 2}", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
  }
  val features1 = (10 until 20).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", s"foo${i % 3}", s"2017-03-15T00:$i:00.000Z", s"POINT (4${i -10} 5${i -10})")
  }
  val features = features0 ++ features1

  "ArrowDataStore" should {
    "write and read values" >> {
      withTempFile { file =>
        val ds = DataStoreFinder.getDataStore(Map(UrlParam.key -> file))
        ds must not(beNull)

        ds.createSchema(sft)
        ds.getSchema(sft.getTypeName) mustEqual sft

        var caching = DataStoreFinder.getDataStore(Map(UrlParam.key -> file, CachingParam.key -> true))
        caching.getSchema(sft.getTypeName) mustEqual sft
        caching.dispose() must not(throwAn[Exception])

        var writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
        features0.foreach { f =>
          FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
          writer.write()
        }
        writer.close()

        caching = DataStoreFinder.getDataStore(Map(UrlParam.key -> file, CachingParam.key -> true))

        foreach(Seq(ds, caching, caching)) { store =>
          val results = CloseableIterator(store.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))
          try {
            results.map(ScalaSimpleFeature.copy).toSeq mustEqual features0
          } finally {
            results.close()
          }
        }

        caching.dispose() must not(throwAn[Exception])

        writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
        features1.foreach { f =>
          FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
          writer.write()
        }
        writer.close()

        caching = DataStoreFinder.getDataStore(Map(UrlParam.key -> file, CachingParam.key -> true))

        foreach(Seq(ds, caching, caching)) { store =>
          val results = CloseableIterator(store.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))
          try {
            results.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1
          } finally {
            results.close()
          }
        }

        caching.dispose() must not(throwAn[Exception])

        ds.dispose() must not(throwAn[Exception])
      }
    }

    "read and filter different files" >> {
      val sftName = "test"
      val queries = Seq(
        "INCLUDE",
        "foo = 'foo1'",
        "bbox(geom, 35, 45, 45, 55)",
        "bbox(geom, 35, 45, 45, 55) and dtg DURING 2017-03-15T00:00:00.000Z/2017-03-15T00:03:00.000Z"
      ).map(ecql => new Query(sftName, ECQL.toFilter(ecql)))

      "only schema" >> {
        withTempFile { url =>
          WithClose(SimpleFeatureArrowFileWriter(sft, new FileOutputStream(url.getPath))) { _ => }
          foreach(Seq(true, false)) { caching =>
            val ds = DataStoreFinder.getDataStore(Map(UrlParam.key -> url, CachingParam.key -> caching))
            ds.getSchema("test") mustEqual sft
            WithClose(ds.getFeatureSource(sftName).getFeatures().features())(_.hasNext must beFalse)
            ds.dispose() must not(throwAn[Exception])
          }
        }
      }

      "simple 2 batches" >> {
        val encoding = SimpleFeatureEncoding.min(includeFids = true)
        withTempFile { url =>
          WithClose(SimpleFeatureArrowFileWriter(sft, new FileOutputStream(url.getPath), encoding = encoding)) { writer =>
            features0.foreach(writer.add)
            writer.flush()
            features1.foreach(writer.add)
          }
          foreach(Seq(true, false)) { caching =>
            val ds = DataStoreFinder.getDataStore(Map(UrlParam.key -> url, CachingParam.key -> caching))
            ds.getSchema(sftName) mustEqual sft
            foreach(queries) { query =>
              WithClose(CloseableIterator(ds.getFeatureSource(sftName).getFeatures(query).features())) { results =>
                results.map(ScalaSimpleFeature.copy).toSeq mustEqual features.filter(query.getFilter.evaluate)
              }
            }
            ds.dispose() must not(throwAn[Exception])
          }
        }
      }

      "multiple logical files" >> {
        val encoding = SimpleFeatureEncoding.min(includeFids = true)
        withTempFile { url =>
          WithClose(SimpleFeatureArrowFileWriter(sft, new FileOutputStream(url.getPath), encoding = encoding)) { writer =>
            features0.foreach(writer.add)
          }
          WithClose(SimpleFeatureArrowFileWriter(sft, new FileOutputStream(url.getPath, true), encoding = encoding)) { writer =>
            features1.foreach(writer.add)
          }
          foreach(Seq(true, false)) { caching =>
            val ds = DataStoreFinder.getDataStore(Map(UrlParam.key -> url, CachingParam.key -> caching))
            ds.getSchema(sftName) mustEqual sft
            foreach(queries) { query =>
              WithClose(CloseableIterator(ds.getFeatureSource(sftName).getFeatures(query).features())) { results =>
                results.map(ScalaSimpleFeature.copy).toSeq mustEqual features.filter(query.getFilter.evaluate)
              }
            }
            ds.dispose() must not(throwAn[Exception])
          }
        }
      }

      "dictionary encoded files" >> {
        val encoding = SimpleFeatureEncoding.min(includeFids = true)
        val dicts = Map(
          "name" -> ArrowDictionary.create(1L, features.map(_.getAttribute(0).asInstanceOf[String]).toArray),
          "foo"  -> ArrowDictionary.create(2L, features.map(_.getAttribute(1).asInstanceOf[String]).toArray)
        )
        withTempFile { url =>
          WithClose(SimpleFeatureArrowFileWriter(sft, new FileOutputStream(url.getPath), dicts, encoding)) { writer =>
            features.foreach(writer.add)
          }
          foreach(Seq(true, false)) { caching =>
            var ds = DataStoreFinder.getDataStore(Map(UrlParam.key -> url, CachingParam.key -> caching))
            ds.getSchema(sftName) mustEqual sft
            foreach(queries) { query =>
              WithClose(CloseableIterator(ds.getFeatureSource(sftName).getFeatures(query).features())) { results =>
                results.map(ScalaSimpleFeature.copy).toSeq mustEqual features.filter(query.getFilter.evaluate)
              }
            }
            ds.dispose() must not(throwAn[Exception])
          }
        }
      }

      "dictionary encoded files with default values" >> {
        val encoding = SimpleFeatureEncoding.min(includeFids = true)
        val dicts = Map("foo"  -> ArrowDictionary.create(1L, Array("foo0", "foo1")))
        withTempFile { url =>
          WithClose(SimpleFeatureArrowFileWriter(sft, new FileOutputStream(url.getPath), dicts, encoding)) { writer =>
            features.foreach(writer.add)
          }
          // the file has only 'foo0' and 'foo1' encoded
          val dictionaryFeatures = features.map {
            case f if f.getAttribute("foo") != "foo2" => f
            case f =>
              val updated = ScalaSimpleFeature.copy(f)
              updated.setAttribute("foo", "[other]")
              updated
          }
          foreach(Seq(true, false)) { caching =>
            var ds = DataStoreFinder.getDataStore(Map(UrlParam.key -> url, CachingParam.key -> caching))
            ds.getSchema(sftName) mustEqual sft
            foreach(queries) { query =>
              WithClose(CloseableIterator(ds.getFeatureSource(sftName).getFeatures(query).features())) { results =>
                results.hasNext must beTrue // just check our filter was valid
                results.map(ScalaSimpleFeature.copy).toSeq mustEqual dictionaryFeatures.filter(query.getFilter.evaluate)
              }
            }
            ds.dispose() must not(throwAn[Exception])
          }
        }
      }
    }
  }

  def withTempFile[T](f: URL => T): T = {
    val url = Files.createTempFile("gm-arrow-ds", ".arrow").toUri.toURL
    try { f(url) } finally {
      if (!new File(url.getPath).delete()) {
        new File(url.getPath).deleteOnExit()
      }
    }
  }

  step {
    allocator.close()
  }
}
