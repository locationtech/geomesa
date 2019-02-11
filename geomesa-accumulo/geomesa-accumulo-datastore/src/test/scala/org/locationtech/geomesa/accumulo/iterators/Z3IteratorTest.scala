/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util

import com.google.common.primitives.{Bytes, Longs}
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.{TimePeriod, Z3SFC}
import org.locationtech.geomesa.index.api.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.index.z3.Z3IndexKeySpace
import org.locationtech.geomesa.index.utils.ExplainNull
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Z3IteratorTest extends Specification {

  sequential

  "Z3Iterator" should {

    val srcIter = new SortedKeyValueIterator[Key, Value] {
      var key: Key = _
      var staged: Key = _
      override def deepCopy(iteratorEnvironment: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = this
      override def next(): Unit = {
        staged = key
        key = null
      }
      override def getTopValue: Value = null
      override def getTopKey: Key = staged
      override def init(sortedKeyValueIterator: SortedKeyValueIterator[Key, Value],
                        map: util.Map[String, String],
                        iteratorEnvironment: IteratorEnvironment): Unit = {}
      override def seek(range: Range, collection: util.Collection[ByteSequence], b: Boolean): Unit = {
        key = null
        staged = null
      }
      override def hasTop: Boolean = staged != null
    }

    val sft = SimpleFeatureTypes.createType("z3IteratorTest", "dtg:Date,*geom:Point:srid=4326")
    val filter = ECQL.toFilter("bbox(geom, -78, 38, -75, 40) AND dtg DURING 1970-01-01T00:05:00.000Z/1970-01-01T00:15:00.000Z")
    val indexValues = new Z3IndexKeySpace(sft, NoShardStrategy, "geom", "dtg").getIndexValues(filter, ExplainNull)

    val iter = new Z3Iterator
    iter.init(srcIter, Z3Iterator.configure(indexValues, hasSplits = false, isSharing = false, 25).getOptions, null)

    "iterate on points" >> {
      "keep in bounds values" >> {
        val test1 = Z3SFC(TimePeriod.Week).index(-76.0, 38.5, 500)
        val prefix = Array[Byte](0, 0)
        val row = Bytes.concat(prefix, Longs.toByteArray(test1.z))
        srcIter.key = new Key(new Text(row))
        iter.next()
        iter.hasTop must beTrue
      }

      "drop out of bounds values" >> {
        val test2 = Z3SFC(TimePeriod.Week).index(-70.0, 38.5, 500)
        val prefix = Array[Byte](0, 0)
        val row = Bytes.concat(prefix, Longs.toByteArray(test2.z))
        srcIter.key = new Key(new Text(row))
        iter.next()
        iter.hasTop must beFalse
      }
    }
  }
}
