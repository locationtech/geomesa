/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
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
import org.locationtech.jts.geom.{MultiPoint, Point}
import org.opengis.filter.Filter
import org.specs2.mock.Mockito
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowMultiPointTest extends TestWithFeatureType with Mockito with LazyLogging {

  import scala.collection.JavaConverters._

  override val spec = "team:String,name:String,dtg:Date,*geom:MultiPoint:srid=4326"

  implicit val allocator: BufferAllocator = new DirtyRootAllocator(Long.MaxValue, 6.toByte)

  val features = Seq.tabulate(10) { i =>
    val name = s"name$i"
    val team = s"team${i % 2}"
    ScalaSimpleFeature.create(sft, s"$i", team, name, s"2017-02-03T00:0$i:01.000Z", s"MULTIPOINT((40 6$i),(4$i 60),(2 2))")
  }

  addFeatures(features)

  "Arrow delta scans" should {
    "query multipoints" in {
      val query = new Query(sft.getTypeName, Filter.INCLUDE, Array("name", "dtg", "geom"))
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
      result.map(_.getID) mustEqual features.map(_.getID)
      result.map(_.getAttributes.asScala) mustEqual features.map(_.getAttributes.asScala.drop(1))
    }
  }

  step {
    allocator.close()
  }
}
