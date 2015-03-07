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

package org.locationtech.geomesa.utils.geotools

import java.util.Date

import org.geotools.factory.CommonFactoryFinder
import org.joda.time.Interval
import org.locationtech.geomesa.utils.time.Time._
import org.opengis.feature.{Feature, FeatureVisitor}

class MinMaxTimeVisitor(dtg: String) extends FeatureVisitor {
  val factory = CommonFactoryFinder.getFilterFactory(null)
  val expr = factory.property(dtg)

  private var timeBounds: Interval = null

  // TODO?: Convert to CalcResults/etc?
  def getBounds = timeBounds

  def updateBounds(date: Date): Unit = {
    timeBounds = timeBounds.expandByDate(date)
  }

  override def visit(p1: Feature): Unit = {
    val date = expr.evaluate(p1).asInstanceOf[Date]
    if (date != null) {
      updateBounds(date)
    }
  }
}
