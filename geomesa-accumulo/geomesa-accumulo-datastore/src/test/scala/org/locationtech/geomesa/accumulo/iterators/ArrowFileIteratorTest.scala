/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowFileIteratorTest extends TestWithDataStore {

  override val spec = "name:String,dtg:Date,*geom:Point:srid=4326"

  implicit val allocator: BufferAllocator = new RootAllocator(Long.MaxValue)

  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name${i % 2}", s"2017-02-03T00:0$i:01.000Z", s"POINT(40 6$i)")
  }

  // hit all major indices
  val filters = Seq(
    "bbox(geom, 38, 59, 42, 70)",
    "bbox(geom, 38, 59, 42, 70) and dtg DURING 2017-02-03T00:00:00.000Z/2017-02-03T01:00:00.000Z",
    "name IN('name0', 'name1')",
    s"IN(${features.map(_.getID).mkString("'", "', '", "'")})").map(ECQL.toFilter)

  addFeatures(features)

  "ArrowFileIterator" should {
    "return arrow dictionary encoded data" in {
      foreach(filters) { filter =>
        val query = new Query(sft.getTypeName, filter)
        query.getHints.put(QueryHints.ARROW_ENCODE, true)
        query.getHints.put(QueryHints.ARROW_DICTIONARY_FIELDS, "name")
        query.getHints.put(QueryHints.ARROW_MULTI_FILE, true)
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
  }

  step {
    allocator.close()
  }
}
