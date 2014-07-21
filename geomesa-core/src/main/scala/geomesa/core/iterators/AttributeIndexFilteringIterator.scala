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
import com.vividsolutions.jts.geom.Geometry
import geomesa.core._
import geomesa.core.index.IndexSchema
import geomesa.core.index.IndexSchema.DecodedIndexValue
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{Filter, IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.data.DataUtilities
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.Interval
import org.opengis.feature.simple.SimpleFeature

class AttributeIndexFilteringIterator extends Filter with Logging {

  protected var interval: Interval = null
  // At the minute, this filter only check geometry.
  protected var filter: org.opengis.filter.Filter = null
  protected var geomTestSF: SimpleFeature = null

  // NB: This is duplicated code from the STII.  Consider refactoring.
  lazy val wrappedGeomFilter: Geometry => Boolean = {
    if (filter != null && geomTestSF != null) {
      geom => {
        geomTestSF.setDefaultGeometry(geom)
        filter.evaluate(geomTestSF)
      }
    } else {
      _ => true
    }
  }

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: JMap[String, String],
                    env: IteratorEnvironment) {
    super.init(source, options, env)
    // NB: This is copied code from the STII.  Consider refactoring.
    if (options.containsKey(DEFAULT_FILTER_PROPERTY_NAME) && options.containsKey(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)) {
      val featureType = DataUtilities.createType("DummyType", options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE))
      featureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

      val filterString  = options.get(DEFAULT_FILTER_PROPERTY_NAME)
      filter = ECQL.toFilter(filterString)
      println(s"In AIFI with $filter")
      val sfb = new SimpleFeatureBuilder(featureType)
      geomTestSF = sfb.buildFeature("test")
    }

    if(options.containsKey(AttributeIndexFilteringIterator.INTERVAL_KEY)) {
      // TODO validate interval option
      interval = Interval.parse(options.get(AttributeIndexFilteringIterator.INTERVAL_KEY))
      logger.info(s"Set interval to ${interval.toString}")
    }
  }

  override def deepCopy(env: IteratorEnvironment) = {
    val copy = super.deepCopy(env).asInstanceOf[AttributeIndexFilteringIterator]

    copy.interval = interval //interval is immutable - no need to deep copy
    copy.filter = filter //.clone.asInstanceOf[Polygon]
    copy.geomTestSF = geomTestSF
    copy
  }

  override def accept(k: Key, v: Value): Boolean = {
    val DecodedIndexValue(_, geom, dtgOpt) = IndexSchema.decodeIndexValue(v)
    wrappedGeomFilter(geom) && dtgOpt.map(dtg => filterInterval(dtg)).getOrElse(true)
  }

  protected def filterInterval(dtg: Long) = Option(interval).map(i => i.contains(dtg)).getOrElse(true)
}

object AttributeIndexFilteringIterator {
  val INTERVAL_KEY = "geomesa.interval"
}
