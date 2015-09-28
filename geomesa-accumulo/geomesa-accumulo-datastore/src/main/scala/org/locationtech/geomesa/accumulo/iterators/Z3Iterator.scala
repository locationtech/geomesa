/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.iterators

import com.google.common.primitives.{Longs, Shorts}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Range => AccRange, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.curve.Z3

class Z3Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z3Iterator.zKey

  var source: SortedKeyValueIterator[Key, Value] = null
  var zNums: Array[Int] = null

  var xmin: Int = -1
  var xmax: Int = -1
  var ymin: Int = -1
  var ymax: Int = -1
  var tmin: Int = -1
  var tmax: Int = -1
  var wmin: Short = -1
  var wmax: Short = -1

  var tLo: Int = -1
  var tHi: Int = -1

  var topKey: Key = null
  var topValue: Value = null
  val row = new Text()

  override def next(): Unit = {
    source.next()
    findTop()
  }

  def findTop(): Unit = {
    topKey = null
    topValue = null
    while (source.hasTop && !inBounds(source.getTopKey)) { source.next() }
    if (source.hasTop) {
      topKey = source.getTopKey
      topValue = source.getTopValue
    }
  }

  private def inBounds(k: Key): Boolean = {
    k.getRow(row)
    val bytes = row.getBytes
    val week = Shorts.fromBytes(bytes(0), bytes(1))
    val keyZ = Longs.fromBytes(bytes(2), bytes(3), bytes(4), bytes(5), bytes(6), bytes(7), bytes(8), bytes(9))
    val (x, y, t) = Z3(keyZ).decode
    x >= xmin && x <= xmax && y >= ymin && y <= ymax && {
      if (week == wmin) {
        t >= tmin && t <= tHi
      } else if (week == wmax) {
        t >= tLo && t <= tmax
      } else {
        true
      }
    }
  }

  override def getTopValue: Value = topValue
  override def getTopKey: Key = topKey
  override def hasTop: Boolean = topKey != null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)

    this.source = source.deepCopy(env)
    zNums = options.get(zKey).split(":").map(_.toInt)
    xmin = zNums(0)
    xmax = zNums(1)
    ymin = zNums(2)
    ymax = zNums(3)
    tmin = zNums(4)
    tmax = zNums(5)
    wmin = zNums(6).toShort
    wmax = zNums(7).toShort
    tLo = if (wmin == wmax) tmin else zNums(8)
    tHi = if (wmin == wmax) tmax else zNums(9)
  }

  override def seek(range: AccRange, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    import scala.collection.JavaConversions._
    val iter = new Z3Iterator
    iter.init(source, Map(zKey -> zNums.mkString(":")), env)
    iter
  }
}

object Z3Iterator {

  val zKey = "z"

  def configure(xmin: Int,
                xmax: Int,
                ymin: Int,
                ymax: Int,
                tmin: Int,
                tmax: Int,
                wmin: Short,
                wmax: Short,
                tLo: Int,
                tHi: Int,
                priority: Int) = {
    val is = new IteratorSetting(priority, "z3", classOf[Z3Iterator])
    is.addOption(zKey, s"$xmin:$xmax:$ymin:$ymax:$tmin:$tmax:$wmin:$wmax:$tLo:$tHi")
    is
  }
}