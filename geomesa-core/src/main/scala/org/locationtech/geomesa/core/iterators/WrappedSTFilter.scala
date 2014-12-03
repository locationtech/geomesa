/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.core.iterators

import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.opengis.feature.simple.SimpleFeature

trait WrappedSTFilter  {
  var filter: org.opengis.filter.Filter = null
  var testSimpleFeature: SimpleFeature = null
  var dateAttributeName: Option[String] = None

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
}
