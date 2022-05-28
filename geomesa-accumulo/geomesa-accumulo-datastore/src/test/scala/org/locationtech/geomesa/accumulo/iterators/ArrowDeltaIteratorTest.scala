/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.DirtyRootAllocator
import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Point
import org.specs2.mock.Mockito
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowDeltaIteratorTest extends TestWithFeatureType with Mockito with LazyLogging {

  import scala.collection.JavaConverters._

  override val spec = "name:String,team:String,age:Int,props:String:json=true,dtg:Date,*geom:Point:srid=4326"

  implicit val allocator: BufferAllocator = new DirtyRootAllocator(Long.MaxValue, 6.toByte)

  val features = Seq.tabulate(10) { i =>
    val name = s"name$i"
    val team = s"team${i % 2}"
    val age = i % 5
    val props = s"""{"color":"${if (i % 2 == 0) "blue" else "red"}"}"""
    ScalaSimpleFeature.create(sft, s"$i", name, team, age, props, s"2017-02-03T00:0$i:01.000Z", s"POINT(40 6$i)")
  }

  addFeatures(features)

  "Arrow delta scans" should {
    "return projections" in {
      val filter =
        ECQL.toFilter("bbox(geom, 38, 59, 42, 70) and dtg DURING 2017-02-03T00:00:00.000Z/2017-02-03T01:00:00.000Z")
      val query = new Query(sft.getTypeName, filter, Array("name", "x=getX(geom)", "y=getY(geom)", "dtg", "geom"))
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
      query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      def in() = new ByteArrayInputStream(out.toByteArray)
      val result = WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
        WithClose(reader.features())(_.map(ScalaSimpleFeature.copy).toList)
      }
      logger.debug(result.map(_.getAttributes.asScala).toString)
      result.map(_.getID) mustEqual features.map(_.getID)
      result.map(_.getAttributes.asScala) mustEqual features.map { f =>
        val geom = f.getAttribute("geom").asInstanceOf[Point]
        Seq(f.getAttribute("name"), geom.getX, geom.getY, f.getAttribute("dtg"), geom)
      }
    }
    "return projections into json fields" in {
      val filter =
        ECQL.toFilter("bbox(geom, 38, 59, 42, 70) and dtg DURING 2017-02-03T00:00:00.000Z/2017-02-03T01:00:00.000Z")
      val query = new Query(sft.getTypeName, filter, Array("team", "color=\"$.props.color\"", "dtg", "geom"))
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "team,color")
      query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      def in() = new ByteArrayInputStream(out.toByteArray)
      val result = WithClose(SimpleFeatureArrowFileReader.streaming(in)) { reader =>
        WithClose(reader.features())(_.map(ScalaSimpleFeature.copy).toList)
      }
      result.map(_.getID) mustEqual features.map(_.getID)
      result.map(_.getAttributes.asScala) mustEqual features.map { f =>
        val color = if (f.getAttribute("props").toString.contains("red")) { "red" } else { "blue" }
         Seq(f.getAttribute("team"), color, f.getAttribute("dtg"), f.getAttribute("geom"))
      }
    }
  }

  step {
    allocator.close()
  }
}
