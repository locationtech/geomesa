/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.filter

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.opengis.filter.Filter

object FilterUtils {

  val ff = CommonFactoryFinder.getFilterFactory2

  implicit class RichFilter(val filter: Filter) {
    def &&(that: Filter) = ff.and(filter, that)

    def ||(that: Filter) = ff.or(filter, that)

    def ! = ff.not(filter)
  }

  implicit def stringToFilter(s: String) = ECQL.toFilter(s)

  implicit def intToAttributeFilter(i: Int): Filter = s"attr$i = val$i"

  implicit def intToFilter(i: Int): RichFilter = intToAttributeFilter(i)

}