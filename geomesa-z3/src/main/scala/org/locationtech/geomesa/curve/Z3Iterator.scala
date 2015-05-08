package org.locationtech.geomesa.curve

import com.google.common.primitives.Longs
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Range => AccRange, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text

class Z3Iterator extends SortedKeyValueIterator[Key, Value] {
  var source: SortedKeyValueIterator[Key, Value] = null
  var zmin: Long = -1L
  var zmax: Long = -1L

  var zlatmin: Long = -1L
  var zlonmin: Long = -1L
  var ztmin: Long = -1L

  var zlatmax: Long = -1L
  var zlonmax: Long = -1L
  var ztmax: Long = -1L

  var topKey: Key = null
  var topValue: Value = null
  val row = new Text()
  val rowZBytes = Array.ofDim[Byte](8)

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] =
    throw new UnsupportedOperationException("GeoMesa iterators do not support deepCopy")

  override def next(): Unit = {
    source.next()
    findTop()
  }

  def findTop(): Unit = {
    topKey = null
    topValue = null
    while(source.hasTop && !inBounds(source.getTopKey)) source.next()
    if(source.hasTop) {
      topKey = source.getTopKey
      topValue = source.getTopValue
    }
  }

  private def between(l: Long, v: Long, r: Long) = l <= v && v <= r

  private def inBounds(k: Key): Boolean = {
    k.getRow(row)
    System.arraycopy(row.getBytes, 2, rowZBytes, 0, 8)
    val keyZ = Longs.fromByteArray(rowZBytes)
    val (x, y, t) = Z3(keyZ).decode
    between(zlonmin, x, zlonmax) &&
      between(zlatmin, y, zlatmax) &&
      between(ztmin, t, ztmax)
  }

  override def getTopValue: Value = topValue
  override def getTopKey: Key = topKey
  override def hasTop: Boolean = topKey != null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = source.deepCopy(env)
    zmin = options.get("zmin").toLong
    zmax = options.get("zmax").toLong
    val (x0, y0, t0) = Z3(zmin).decode
    val (x1, y1, t1) = Z3(zmax).decode
    zlonmin = x0
    zlonmax = x1
    zlatmin = y0
    zlatmax = y1
    ztmin = t0
    ztmax = t1
  }


  override def seek(range: AccRange, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

}

object Z3Iterator {
  def configure(ll: Z3, ur: Z3) = {
    val is = new IteratorSetting(1, "z3", classOf[Z3Iterator].getCanonicalName)
    is.addOption("zmin", s"${ll.z}")
    is.addOption("zmax", s"${ur.z}")
    is
  }
}