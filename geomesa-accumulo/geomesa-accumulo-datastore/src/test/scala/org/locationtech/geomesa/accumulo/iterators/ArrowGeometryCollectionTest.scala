/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.DirtyRootAllocator
import org.geotools.api.data.{Query, Transaction}
import org.geotools.api.filter.Filter
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.specs2.runner.JUnitRunner

import java.io.ByteArrayOutputStream

@RunWith(classOf[JUnitRunner])
class ArrowGeometryCollectionTest extends TestWithFeatureType with LazyLogging {

  import scala.collection.JavaConverters._

  override val spec = "team:String,name:String,dtg:Date,*geom:GeometryCollection:srid=4326"

  implicit val allocator: BufferAllocator = new DirtyRootAllocator(Long.MaxValue, 6.toByte)

  val features = Seq.tabulate(10) { i =>
    val name = s"name$i"
    val team = s"team${i % 2}"
    val geom = s"GEOMETRYCOLLECTION(POINT(40 6$i),MULTIPOINT((40 6$i),(4$i 60),(2 2)))"
    ScalaSimpleFeature.create(sft, s"$i", team, name, s"2017-02-03T00:0$i:01.000Z", geom)
  }

  step {
    addFeatures(features)
  }

  "Arrow delta scans" should {
    "query geometry collections" in {
      val query = new Query(sft.getTypeName, Filter.INCLUDE, "name", "dtg", "geom")
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_BATCH_SIZE, 100)
      query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
      query.getHints.put(QueryHints.ARROW_SORT_FIELD, "dtg")
      val out = new ByteArrayOutputStream
      CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
        .foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      val result = SimpleFeatureArrowFileReader.read(out.toByteArray)
      result.map(_.getID) mustEqual features.map(_.getID)
      result.map(_.getAttributes.asScala) mustEqual features.map(_.getAttributes.asScala.drop(1))
    }
  }

  step {
    allocator.close()
  }
}
