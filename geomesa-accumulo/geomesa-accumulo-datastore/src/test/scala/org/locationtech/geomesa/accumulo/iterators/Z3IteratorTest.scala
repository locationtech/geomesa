/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
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
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.index.legacy.z3.Z3IndexV2
import org.locationtech.geomesa.curve.{TimePeriod, Z3SFC}
import org.locationtech.sfcurve.zorder.Z3
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class Z3IteratorTest extends Specification {

  import Z3Iterator._

  sequential

  "Z3Iterator" should {

    val (lx, ly, lt) = (-78.0, 38, 300)
    val (ux, uy, ut) = (-75.0, 40, 800)

    val srcIter = new SortedKeyValueIterator[Key, Value] {
      var key: Key = null
      var staged: Key = null
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

    val sfc = Z3SFC(TimePeriod.Week)

    "iterate on points" >> {
      val (xmin, ymin, tmin) = sfc.index(lx, ly, lt).decode
      val (xmax, ymax, tmax) = sfc.index(ux, uy, ut).decode

      val iter = new Z3Iterator

      iter.init(srcIter, Map(ZOffsetKey -> "0", ZLengthKey -> "8",
        ZKeyXY -> s"$xmin:$ymin:$xmax:$ymax", ZKeyT -> s"0;$tmin:$tmax"), null)

      "keep in bounds values" >> {
        val test1 = sfc.index(-76.0, 38.5, 500)
        val prefix = Array[Byte](0, 0)
        val row = Bytes.concat(prefix, Longs.toByteArray(test1.z))
        srcIter.key = new Key(new Text(row))
        iter.next()
        iter.hasTop must beTrue
      }

      "drop out of bounds values" >> {
        val test2 = sfc.index(-70.0, 38.5, 500)
        val prefix = Array[Byte](0, 0)
        val row = Bytes.concat(prefix, Longs.toByteArray(test2.z))
        srcIter.key = new Key(new Text(row))
        iter.next()
        iter.hasTop must beFalse
      }
    }

    "iterate on non-points" >> {
      val (xmin, ymin, tmin) = Z3(sfc.index(lx, ly, lt).z & Z3IndexV2.GEOM_Z_MASK).decode
      val (xmax, ymax, tmax) = Z3(sfc.index(ux, uy, ut).z & Z3IndexV2.GEOM_Z_MASK).decode

      val iter = new Z3Iterator
      iter.init(srcIter, Map(ZOffsetKey -> "0", ZLengthKey -> Z3IndexV2.GEOM_Z_NUM_BYTES.toString,
        ZKeyXY -> s"$xmin:$ymin:$xmax:$ymax", ZKeyT -> s"0;$tmin:$tmax"), null)

      "keep in bounds values" >> {
        val test1 = sfc.index(-76.0, 38.5, 500)
        val prefix = Array[Byte](0, 0)
        val row = Bytes.concat(prefix, Longs.toByteArray(test1.z).take(Z3IndexV2.GEOM_Z_NUM_BYTES))
        srcIter.key = new Key(new Text(row))
        iter.next()
        iter.hasTop must beTrue
      }

      "drop out of bounds values" >> {
        val test2 = sfc.index(-70.0, 38.5, 500)
        val prefix = Array[Byte](0, 0)
        val row = Bytes.concat(prefix, Longs.toByteArray(test2.z).take(Z3IndexV2.GEOM_Z_NUM_BYTES))
        srcIter.key = new Key(new Text(row))
        iter.next()
        iter.hasTop must beFalse
      }
    }
  }
}
