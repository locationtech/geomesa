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

package org.locationtech.geomesa.accumulo

import org.apache.accumulo.core.client.IteratorSetting
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._


package object iterators {
  val FEATURE_ENCODING   = "geomesa.feature.encoding"
  val USER_DATA = ".userdata."

  implicit class RichIteratorSetting(cfg: IteratorSetting) {
    /**
     *  Copy UserData entries taken from a SimpleFeatureType into an IteratorSetting for later transfer back into
     *  a SimpleFeatureType
     *
     *  This works around the fact that SimpleFeatureTypes.encodeType ignores the UserData
     *
     */
    def encodeUserData(userData: java.util.Map[AnyRef,AnyRef], keyPrefix: String)  {
      val fullPrefix = keyPrefix + USER_DATA
      userData.foreach { case (k, v) => cfg.addOption(fullPrefix + k.toString, v.toString)}
    }
  }

  implicit class RichIteratorSimpleFeatureType(sft: SimpleFeatureType) {
    /**
     *  Copy UserData entries taken from an IteratorSetting/Options back into
     *  a SimpleFeatureType
     *
     *  This works around the fact that SimpleFeatureTypes.encodeType ignores the UserData
     *
     */
    def decodeUserData(options: java.util.Map[String,String], keyPrefix:String)  {
      val fullPrefix = keyPrefix + USER_DATA
      options
        .filter {  case (k, _) => k.startsWith(fullPrefix) }
        .foreach { case (k, v) => sft.getUserData.put(k.stripPrefix(fullPrefix), v) }
    }
  }
}
