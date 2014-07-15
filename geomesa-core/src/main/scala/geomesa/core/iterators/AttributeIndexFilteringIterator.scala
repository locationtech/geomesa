/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.core.iterators

import java.util.{Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, Polygon}
import geomesa.core.index.IndexSchema
import geomesa.core.index.IndexSchema.DecodedIndexValue
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{Filter, IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.Interval

class AttributeIndexFilteringIterator extends Filter with Logging {

  protected var bbox: Polygon = null
  protected var interval: Interval = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: JMap[String, String],
                    env: IteratorEnvironment) {
    super.init(source, options, env)
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
  }

  override def deepCopy(env: IteratorEnvironment) = {
    val copy = super.deepCopy(env).asInstanceOf[AttributeIndexFilteringIterator]
    copy.bbox = bbox.clone.asInstanceOf[Polygon]
    copy.interval = interval //interval is immutable - no need to deep copy
    copy
  }

  override def accept(k: Key, v: Value): Boolean = {
    val DecodedIndexValue(_, geom, dtgOpt) = IndexSchema.decodeIndexValue(v)

    // TODO This might be made more efficient
    filterBbox(geom) && dtgOpt.map(dtg => filterInterval(dtg)).getOrElse(true)
  }

  // Intersect, not contains for geometry that hits this bbox
  protected def filterBbox(geom: Geometry) = Option(bbox).map(b => b.intersects(geom)).getOrElse(true)

  protected def filterInterval(dtg: Long) = Option(interval).map(i => i.contains(dtg)).getOrElse(true)

}

object AttributeIndexFilteringIterator {
  val BBOX_KEY = "geomesa.bbox"
  val INTERVAL_KEY = "geomesa.interval"
}
