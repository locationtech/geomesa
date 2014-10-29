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

package org.locationtech.geomesa.utils.filters

import org.geotools.factory.CommonFactoryFinder
import org.geotools.temporal.`object`.{DefaultInstant, DefaultPeriod, DefaultPosition}
import org.joda.time.{DateTime, Interval}
import org.opengis.filter.expression.Expression

object Filters {
  val ff = CommonFactoryFinder.getFilterFactory2

  def dt2lit(dt: DateTime): Expression = ff.literal(dt.toDate)

  def dts2lit(start: DateTime, end: DateTime): Expression = ff.literal(
    new DefaultPeriod(
      new DefaultInstant(new DefaultPosition(start.toDate)),
      new DefaultInstant(new DefaultPosition(end.toDate))
    ))

  def interval2lit(int: Interval): Expression = dts2lit(int.getStart, int.getEnd)
}
