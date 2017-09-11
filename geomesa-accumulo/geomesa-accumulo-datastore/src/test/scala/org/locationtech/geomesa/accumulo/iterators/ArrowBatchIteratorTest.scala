/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.filter.Filter
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowBatchIteratorTest extends TestWithDataStore {

  sequential

  override val spec = "name:String:index=true,team:String,dtg:Date,*geom:Point:srid=4326"

  implicit val allocator: BufferAllocator = new RootAllocator(Long.MaxValue)

  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name${i % 2}", s"team$i", s"2017-02-03T00:0$i:01.000Z", s"POINT(40 6$i)")
  }

  // hit all major indices
  val filters = Seq(
    "bbox(geom, 38, 59, 42, 70)",
    "bbox(geom, 38, 59, 42, 70) and dtg DURING 2017-02-03T00:00:00.000Z/2017-02-03T01:00:00.000Z",
    "name IN('name0', 'name1')",
    s"IN(${features.map(_.getID).mkString("'", "', '", "'")})").map(ECQL.toFilter)

  addFeatures(features)

  "ArrowBatchIterator" should {
    "return arrow encoded data" in {
      foreach(filters) { filter =>
        val query = new Query(sft.getTypeName, filter)
        query.getHints.put(QueryHints.ARROW_ENCODE, true)
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
        val out = new ByteArrayOutputStream
        results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
        def in() = new ByteArrayInputStream(out.toByteArray)
        WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
          SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq must
              containTheSameElementsAs(features)
        }
      }
    }
    "return arrow dictionary encoded data" in {
      foreach(filters) { filter =>
        val query = new Query(sft.getTypeName, filter)
        query.getHints.put(QueryHints.ARROW_ENCODE, true)
        query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
        val out = new ByteArrayOutputStream
        results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
        def in() = new ByteArrayInputStream(out.toByteArray)
        WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
          SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq must
              containTheSameElementsAs(features)
        }
      }
    }
    "return arrow dictionary encoded data with cached data" in {
      val filter = ECQL.toFilter("name = 'name0'")
      val query = new Query(sft.getTypeName, filter)
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      def in() = new ByteArrayInputStream(out.toByteArray)
      WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
        SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq must
            containTheSameElementsAs(features.filter(filter.evaluate))
        // verify all cached values were used for the dictionary
        reader.dictionaries.mapValues(_.values) mustEqual Map("name" -> Seq("name0", "name1"))
      }
    }
    "return arrow dictionary encoded data without caching" in {
      val filter = ECQL.toFilter("name = 'name0'")
      val query = new Query(sft.getTypeName, filter)
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
      query.getHints.put(QueryHints.ARROW_DICTIONARY_CACHED, java.lang.Boolean.FALSE)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      def in() = new ByteArrayInputStream(out.toByteArray)
      WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
        SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq must
            containTheSameElementsAs(features.filter(filter.evaluate))
        // verify only exact values were used for the dictionary
        reader.dictionaries.mapValues(_.values) mustEqual Map("name" -> Seq("name0"))
      }
    }
    "return arrow dictionary encoded data with provided dictionaries" in {
      foreach(filters) { filter =>
        val query = new Query(sft.getTypeName, filter)
        query.getHints.put(QueryHints.ARROW_ENCODE, true)
        query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
        query.getHints.put(QueryHints.ARROW_DICTIONARY_VALUES, "name,name0")
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
        val out = new ByteArrayOutputStream
        results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
        def in() = new ByteArrayInputStream(out.toByteArray)
        WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
          val expected = features.map {
            case f if f.getAttribute(0) != "name1" => f
            case f =>
              val e = ScalaSimpleFeature.copy(sft, f)
              e.setAttribute(0, "[other]")
              e
          }
          SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq must
              containTheSameElementsAs(expected)
        }
      }
    }
    "return arrow encoded projections" in {
      import scala.collection.JavaConverters._
      foreach(filters) { filter =>
        foreach(Seq(Array("dtg", "geom"))) { transform =>
          val query = new Query(sft.getTypeName, filter, transform)
          query.getHints.put(QueryHints.ARROW_ENCODE, true)
          val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
          val out = new ByteArrayOutputStream
          results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
          def in() = new ByteArrayInputStream(out.toByteArray)
          WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
            SelfClosingIterator(reader.features()).map(_.getAttributes.asScala).toSeq must
                containTheSameElementsAs(features.map(f => transform.toSeq.map(f.getAttribute)))
          }
        }
      }
    }
    "return sorted batches" in {
      // TODO figure out how to test multiple batches (client side merge)
      foreach(filters) { filter =>
        val query = new Query(sft.getTypeName, filter)
        query.getHints.put(QueryHints.ARROW_ENCODE, true)
        query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
        val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
        val out = new ByteArrayOutputStream
        results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
        def in() = new ByteArrayInputStream(out.toByteArray)
        WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
          SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toList mustEqual features
        }
      }
    }
    "return sampled arrow encoded data" in {
      val query = new Query(sft.getTypeName, Filter.INCLUDE)
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.SAMPLING, 0.2f)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      def in() = new ByteArrayInputStream(out.toByteArray)
      WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
        val results = SelfClosingIterator(reader.features()).map(ScalaSimpleFeature.copy).toSeq
        results must haveLength(2)
        foreach(results)(features must contain(_))
      }
    }
  }

  step {
    allocator.close()
  }
}
