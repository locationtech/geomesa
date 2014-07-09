package geomesa.core.iterators

import java.util.{Collection => JCollection, Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, Polygon}
import geomesa.core.index.IndexSchema
import geomesa.core.index.IndexSchema.DecodedIndexValue
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.Interval

class AttributeIndexFilteringIterator extends SortedKeyValueIterator[Key, Value] with Logging {

  var sourceIter: SortedKeyValueIterator[Key, Value] = null
  var topKey: Key = null
  var topValue: Value = null
  var bbox: Polygon = null
  var interval: Interval = null

  def init(source: SortedKeyValueIterator[Key, Value],
           options: JMap[String, String],
           env: IteratorEnvironment) {
    if(options.containsKey(AttributeIndexFilteringIterator.BBOX_KEY)) {
      // TODO validate bbox option
      val Array(minx, miny, maxx, maxy) = options.get(AttributeIndexFilteringIterator.BBOX_KEY).split(",").map(_.toDouble)
      val re = new ReferencedEnvelope(minx, maxx, miny, maxy, DefaultGeographicCRS.WGS84)
      bbox = JTS.toGeometry(re)
      logger.info(s"Set bounding box for values ${bbox.toString}")
    }
    if(options.containsKey(AttributeIndexFilteringIterator.INTERVAL_KEY)) {
      // TODO validate interval option
      interval = Interval.parse(options.get(AttributeIndexFilteringIterator.INTERVAL_KEY))
      logger.info(s"Set interval to ${interval.toString}")
    }
    sourceIter = source.deepCopy(env)
  }

  override def hasTop: Boolean = topKey != null

  override def deepCopy(env: IteratorEnvironment) = throw new IllegalArgumentException("not supported")

  override def next(): Unit = {
    topKey = null
    topValue = null
    while(sourceIter.hasTop && topKey == null && topValue == null) {
      val DecodedIndexValue(_, geom, dtgOpt) = IndexSchema.decodeIndexValue(sourceIter.getTopValue)

      // TODO This might be made more efficient
      if (filterBbox(geom) && dtgOpt.map(dtg => filterInterval(dtg)).getOrElse(true)) {
        topKey = new Key(sourceIter.getTopKey)
        topValue = new Value(sourceIter.getTopValue)
      } else {
        sourceIter.next()
      }
    }
  }

  // Intersect, not contains for geometry that hits this bbox
  def filterBbox(geom: Geometry) = Option(bbox).map(b => b.intersects(geom)).getOrElse(true)

  def filterInterval(dtg: Long) = Option(interval).map(i => i.contains(dtg)).getOrElse(true)

  override def getTopValue: Value = topValue

  override def getTopKey: Key = topKey

  override def seek(range: Range, columnFamilies: JCollection[ByteSequence], inclusive: Boolean): Unit = {
    sourceIter.seek(range, columnFamilies, inclusive)
    next()
  }
}

object AttributeIndexFilteringIterator {
  val BBOX_KEY = "geomesa.bbox"
  val INTERVAL_KEY = "geomesa.interval"
}
