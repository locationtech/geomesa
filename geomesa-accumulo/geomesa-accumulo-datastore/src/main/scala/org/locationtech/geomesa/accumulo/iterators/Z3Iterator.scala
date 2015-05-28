package org.locationtech.geomesa.accumulo.iterators

import com.google.common.primitives.Longs
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Value, Range => AccRange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.curve.Z3

class Z3Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z3Iterator.{zmaxKey, zminKey}

  var source: SortedKeyValueIterator[Key, Value] = null

  var zlatmin: Long = -1L
  var zlonmin: Long = -1L
  var ztmin: Long = -1L

  var zlatmax: Long = -1L
  var zlonmax: Long = -1L
  var ztmax: Long = -1L

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
    val keyZ = Longs.fromBytes(bytes(2), bytes(3), bytes(4), bytes(5), bytes(6), bytes(7), bytes(8), bytes(9))
    val (x, y, t) = Z3(keyZ).decode
    x >= zlonmin && x <= zlonmax && y >= zlatmin && y <= zlatmax && t >= ztmin && t <= ztmax
  }

  override def getTopValue: Value = topValue
  override def getTopKey: Key = topKey
  override def hasTop: Boolean = topKey != null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = source.deepCopy(env)
    val zmin = options.get(zminKey).toLong
    val zmax = options.get(zmaxKey).toLong
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

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    import scala.collection.JavaConversions._

    val zmin = Z3(zlonmin.toInt, zlatmin.toInt, ztmin.toInt).z.toString
    val zmax = Z3(zlonmax.toInt, zlatmax.toInt, ztmax.toInt).z.toString

    val iter = new Z3Iterator
    iter.init(source, Map(zminKey -> zmin, zmaxKey -> zmax), env)
    iter
  }
}

object Z3Iterator {
  val zminKey = "zmin"
  val zmaxKey = "zmax"

  def configure(ll: Z3, ur: Z3, priority: Int) = {
    val is = new IteratorSetting(priority, "z3", classOf[Z3Iterator].getCanonicalName)
    is.addOption(zminKey, s"${ll.z}")
    is.addOption(zmaxKey, s"${ur.z}")
    is
  }
}