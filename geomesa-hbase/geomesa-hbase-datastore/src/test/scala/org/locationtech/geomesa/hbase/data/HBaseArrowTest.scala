/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.RootAllocator
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.{BigTableNameParam, ConnectionParam}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.filter.Filter

class HBaseArrowTest extends HBaseTest with LazyLogging  {

  implicit val allocator = new RootAllocator(Long.MaxValue)

  val sft = SimpleFeatureTypes.createType("arrow", "name:String,dtg:Date,*geom:Point:srid=4326")

  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name${i % 2}", s"2017-02-03T00:0$i:00.000Z", s"POINT(40 6$i)")
  }

  var ds: HBaseDataStore = _

  step {
    import scala.collection.JavaConversions._
    val params = Map(ConnectionParam.getName -> connection, BigTableNameParam.getName -> "HBaseArrowTest")
    ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
    ds.createSchema(sft)
    val writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
    features.foreach { f =>
      FeatureUtils.copyToWriter(writer, f, overrideFid = true)
      writer.write()
    }
    writer.close()
  }

  "ArrowFileCoprocessor" should {
    "return arrow dictionary encoded data" in {
      val query = new Query(sft.getTypeName, Filter.INCLUDE)
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
      query.getHints.put(QueryHints.ARROW_DICTIONARY_COMPUTE, false)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      def in() = new ByteArrayInputStream(out.toByteArray)
      WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
        // sft name gets dropped, so we can't compare directly
        SelfClosingIterator(reader.features()).map(f => (f.getID, f.getAttributes)).toSeq must
            containTheSameElementsAs(features.map(f => (f.getID, f.getAttributes)))
      }
    }
  }

  "ArrowBatchCoprocessor" should {
    "return arrow encoded data" in {
      val query = new Query(sft.getTypeName, Filter.INCLUDE)
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
    "return arrow dictionary encoded data" in {
      val query = new Query(sft.getTypeName, Filter.INCLUDE)
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
    "return arrow dictionary encoded data with provided dictionaries" in {
      val query = new Query(sft.getTypeName, Filter.INCLUDE)
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
    "return arrow encoded projections" in {
      import scala.collection.JavaConverters._
      val query = new Query(sft.getTypeName, Filter.INCLUDE, Array("dtg", "geom"))
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      def in() = new ByteArrayInputStream(out.toByteArray)
      WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
        SelfClosingIterator(reader.features()).map(_.getAttributes.asScala).toSeq must
            containTheSameElementsAs(features.map(f => List(f.getAttribute("dtg"), f.getAttribute("geom"))))
      }
    }
    "return sorted batches" in {
      // TODO figure out how to test multiple batches (client side merge)
      val query = new Query(sft.getTypeName, Filter.INCLUDE)
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
        results must haveLength(4) // TODO this seems to indicate two region servers?
        foreach(results)(features must contain(_))
      }
    }
  }

  step {
    allocator.close()
  }
}
