/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.arrow.memory.RootAllocator
import org.geotools.data.{Query, Transaction}
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

  override val spec = "name:String,dtg:Date,*geom:Point:srid=4326"

  implicit val allocator = new RootAllocator(Long.MaxValue)

  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name${i % 2}", s"2017-02-03T00:0$i:00.000Z", s"POINT(40 6$i)")
  }

  addFeatures(features)

  "ArrowBatchIterator" should {
    "return arrow encoded data" in {
      val query = new Query(sft.getTypeName, Filter.INCLUDE)
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      val in = new ByteArrayInputStream(out.toByteArray)
      WithClose(new SimpleFeatureArrowFileReader(in)) { reader =>
        reader.features.toSeq must containTheSameElementsAs(features)
      }
    }
    "return arrow dictionary encoded data" in {
      val query = new Query(sft.getTypeName, Filter.INCLUDE)
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      val in = new ByteArrayInputStream(out.toByteArray)
      WithClose(new SimpleFeatureArrowFileReader(in)) { reader =>
        reader.features.toSeq must containTheSameElementsAs(features)
      }
    }
    "return arrow dictionary encoded data with provided dictionaries" in {
      val query = new Query(sft.getTypeName, Filter.INCLUDE)
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
      query.getHints.put(QueryHints.ARROW_DICTIONARY_VALUES, "name,name0")
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      val in = new ByteArrayInputStream(out.toByteArray)
      WithClose(new SimpleFeatureArrowFileReader(in)) { reader =>
        val read = reader.features.toSeq
        read must haveLength(10)
        read must containAllOf(features.filter(_.getAttribute(0) != "name1"))
        read must containAllOf {
          val expected = features.filter(_.getAttribute(0) == "name1").map(ScalaSimpleFeature.create(sft, _))
          expected.foreach(_.setAttribute(0, "[other]"))
          expected
        }
      }
    }
    "return arrow encoded projections" in {
      import scala.collection.JavaConverters._
      val query = new Query(sft.getTypeName, Filter.INCLUDE, Array("dtg", "geom"))
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      val in = new ByteArrayInputStream(out.toByteArray)
      WithClose(new SimpleFeatureArrowFileReader(in)) { reader =>
        reader.features.map(_.getAttributes.asScala).toSeq must
            containTheSameElementsAs(features.map(f => List(f.getAttribute("dtg"), f.getAttribute("geom"))))
      }
    }
    "return sampled arrow encoded data" in {
      val query = new Query(sft.getTypeName, Filter.INCLUDE)
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.SAMPLING, 0.2f)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      val in = new ByteArrayInputStream(out.toByteArray)
      WithClose(new SimpleFeatureArrowFileReader(in)) { reader =>
        val results = reader.features.toSeq
        results must haveLength(2)
        foreach(results)(features.contains(_) must beTrue)
      }
    }
  }

  step {
    allocator.close()
  }
}
