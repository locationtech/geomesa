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

package org.locationtech.geomesa.feature

import org.geotools.data.DataUtilities
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/** Utility methods for encoding and decoding [[org.opengis.feature.simple.SimpleFeature]]s as text.
  *
  * Created by mmatz on 4/2/15.
  */
object TextEncoding {

  private val visPrefix = "|Visibility ["
  private val visSuffix = "]"

  /**
   * @param input the [[String]] to which ``visibility`` will be added
   * @param visibility the visibility to add to ``input``
   * @return a [[String]] containing both ``input`` and ``visibility``
   */
  def addVisibility(input: String, visibility: String): String = s"$input$visPrefix$visibility$visSuffix"

  /**
   * @param input a [[String]] containing a value and a visiblity as produced by ``addVisibility``
   * @return a tuple where the first element is ``input`` with out the visibility and the second is the visibility
   */
  def splitVisibility(input: String): (String, String) = {
    val index = input.indexOf(visPrefix)

    if (index < 0 || !input.endsWith(visSuffix)) {
      throw new IllegalArgumentException(s"Visibility not found in '$input'")
    }

    if (input.indexOf(visPrefix, index + 1) >= 0) {
      throw new IllegalArgumentException(s"Multiple visibilities found in '$input'")
    }

    val remaining = input.substring(0, index)
    val vis = input.substring(index + visPrefix.length, input.length - visSuffix.length)

    (remaining, vis)
  }
}

/**
 * This could be done more cleanly, but the object pool infrastructure already
 * existed, so it was quickest, easiest simply to abuse it.
 */
object ThreadSafeDataUtilities {
  private[this] val dataUtilitiesPool = ObjectPoolFactory(new Object, 1)

  def encodeFeature(feature:SimpleFeature): String = dataUtilitiesPool.withResource {
    _ => DataUtilities.encodeFeature(feature)
  }

  def createFeature(simpleFeatureType:SimpleFeatureType, featureString:String): SimpleFeature =
    dataUtilitiesPool.withResource {
      _ => DataUtilities.createFeature(simpleFeatureType, featureString)
    }
}
