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

import java.util.{Date, Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import geomesa.core._
import geomesa.core.index._
import geomesa.core.index.IndexSchema.DecodedIndexValue
import geomesa.utils.geotools.SimpleFeatureTypes
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{Filter, IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.opengis.feature.simple.SimpleFeature

class AttributeIndexFilteringIterator extends Filter with Logging {

  protected var filter: org.opengis.filter.Filter = null
  protected var testSimpleFeature: SimpleFeature = null
  protected var dateAttributeName: Option[String] = None

  // NB: This is duplicated code from the STII.  Consider refactoring.
  lazy val wrappedSTFilter: (Geometry, Option[Long]) => Boolean = {
    if (filter != null && testSimpleFeature != null) {
      (geom: Geometry, olong: Option[Long]) => {
        testSimpleFeature.setDefaultGeometry(geom)
        for {
          dateAttribute <- dateAttributeName
          long <- olong
        } {
          testSimpleFeature.setAttribute(dateAttribute, new Date(long))
        }
        filter.evaluate(testSimpleFeature)
      }
    } else {
      (_, _) => true
    }
  }

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: JMap[String, String],
                    env: IteratorEnvironment) {
    super.init(source, options, env)
    // NB: This is copied code from the STII.  Consider refactoring.
    if (options.containsKey(DEFAULT_FILTER_PROPERTY_NAME) && options.containsKey(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)) {
      val featureType = SimpleFeatureTypes.createType("DummyType", options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE))
      featureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
      dateAttributeName = getDtgFieldName(featureType)

      val filterString  = options.get(DEFAULT_FILTER_PROPERTY_NAME)
      filter = ECQL.toFilter(filterString)
      println(s"In AIFI with $filter")
      val sfb = new SimpleFeatureBuilder(featureType)

      testSimpleFeature = sfb.buildFeature("test")
    }
  }

  override def deepCopy(env: IteratorEnvironment) = {
    val copy = super.deepCopy(env).asInstanceOf[AttributeIndexFilteringIterator]
    copy.filter = filter
    copy.testSimpleFeature = testSimpleFeature
    copy
  }

  override def accept(k: Key, v: Value): Boolean = {
    val DecodedIndexValue(_, geom, dtgOpt) = IndexSchema.decodeIndexValue(v)
    wrappedSTFilter(geom, dtgOpt)
  }
}

