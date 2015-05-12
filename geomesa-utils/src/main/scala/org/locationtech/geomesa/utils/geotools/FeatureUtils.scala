/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

import org.geotools.data.DataUtilities
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/** Utilities for re-typing and re-building [[SimpleFeatureType]]s and [[SimpleFeature]]s while
  * preserving user data which the standard Geo Tools utilities do not do.
  */
object FeatureUtils {

  /** Retypes a [[SimpleFeatureType]], preserving the user data.
   *
   * @param orig the original
   * @param propertyNames the property names for the new type
   * @return the new [[SimpleFeatureType]]
   */
  def retype(orig: SimpleFeatureType, propertyNames: Array[String]): SimpleFeatureType = {
    val mod = SimpleFeatureTypeBuilder.retype(orig, propertyNames)
    mod.getUserData.putAll(orig.getUserData)
    mod
  }

  /** Retypes a [[SimpleFeature]], preserving the user data.
    *
    * @param orig the source feature
    * @param targetType the target type
    * @return the new [[SimpleFeature]]
    */
  def retype(orig: SimpleFeature, targetType: SimpleFeatureType): SimpleFeature = {
    val mod = DataUtilities.reType(targetType, orig, false)
    mod.getUserData.putAll(orig.getUserData)
    mod
  }

  /** A new [[SimpleFeatureType]] builder initialized with ``orig`` which, when ``buildFeatureType`` is
    * called will, as a last step, add all user data from ``orig`` to the newly built [[SimpleFeatureType]].
    *
    * @param orig the source feature
    * @return a new [[SimpleFeatureTypeBuilder]]
    */
  def builder(orig: SimpleFeatureType): SimpleFeatureTypeBuilder = {
    val builder = new SimpleFeatureTypeBuilder() {

      override def buildFeatureType(): SimpleFeatureType = {
        val result = super.buildFeatureType()
        result.getUserData.putAll(orig.getUserData)
        result
      }
    }

    builder.init(orig)
    builder
  }
}
