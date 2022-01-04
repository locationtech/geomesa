/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.api.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.conf.FilterCompatibility
import org.locationtech.geomesa.index.index.z3.Z3IndexKeySpace
import org.locationtech.geomesa.index.index.z3.legacy.Z3IndexV4.Z3IndexKeySpaceV4
import org.locationtech.geomesa.index.utils.ExplainNull
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.ByteArrays
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Z3IteratorTest extends Specification {

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("z3IteratorTest", "dtg:Date,*geom:Point:srid=4326")
  val filter = ECQL.toFilter("bbox(geom, -78, 38, -75, 40) AND dtg DURING 1970-01-01T00:05:00.000Z/1970-01-01T00:15:00.000Z")
  val indexValues = new Z3IndexKeySpace(sft, NoShardStrategy, "geom", "dtg").getIndexValues(filter, ExplainNull)

  def iterator(k: Array[Byte]): Z3Iterator = {
    val srcIter: SortedKeyValueIterator[Key, Value] = new SortedKeyValueIterator[Key, Value] {
      private var key: Key = new Key(new Text(k))
      private var staged: Key = _
      override def deepCopy(iteratorEnvironment: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = this
      override def next(): Unit = {
        staged = key
        key = null
      }
      override def getTopValue: Value = null
      override def getTopKey: Key = staged
      override def init(
          sortedKeyValueIterator: SortedKeyValueIterator[Key, Value],
          map: java.util.Map[String, String],
          iteratorEnvironment: IteratorEnvironment): Unit = {}
      override def seek(range: Range, collection: java.util.Collection[ByteSequence], b: Boolean): Unit = {
        key = null
        staged = null
      }
      override def hasTop: Boolean = staged != null
    }

    val iter = new Z3Iterator
    iter.init(srcIter, Z3Iterator.configure(indexValues, 0, None, 25).getOptions, null)
    iter
  }

  "Z3Iterator" should {

    "iterate on points" >> {
      "keep in bounds values" >> {
        val test1 = indexValues.sfc.index(-76.0, 38.5, 500)
        val row = ByteArrays.toBytes(Array[Byte](0, 0), test1)
        val iter = iterator(row)
        iter.next()
        iter.hasTop must beTrue
      }

      "drop out of bounds values" >> {
        val test2 = indexValues.sfc.index(-70.0, 38.5, 500)
        val row = ByteArrays.toBytes(Array[Byte](0, 0), test2)
        val iter = iterator(row)
        iter.next()
        iter.hasTop must beFalse
      }
    }

    "configure 1.3 compatibility mode" >> {
      // v4 index corresponds to gm 1.3
      val keySpace = new Z3IndexKeySpaceV4(sft, Array.empty, NoShardStrategy, "geom", "dtg")
      val filter = "bbox(geom,0,-70,50,-50) and dtg during 2015-06-06T00:00:00.000Z/2015-06-08T00:00:00.000Z"
      val values = keySpace.getIndexValues(ECQL.toFilter(filter), ExplainNull)
      val config = Z3Iterator.configure(values, 2, Some(FilterCompatibility.`1.3`), 23)
      config.getIteratorClass mustEqual classOf[Z3Iterator].getName
      config.getPriority mustEqual 23
      // expected values taken from a 1.3 install
      config.getOptions.asScala mustEqual
          Map("zl" -> "8", "zo" -> "2", "zxy" -> "1048576:233017:1339847:466034", "zt" -> "2370;299595:599184")
    }
  }
}
