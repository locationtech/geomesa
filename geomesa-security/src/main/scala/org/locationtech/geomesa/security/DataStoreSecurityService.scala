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
package org.locationtech.geomesa.security

import java.util.ServiceLoader

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.{FeatureReader, FeatureSource, Query}
import org.geotools.feature.FeatureCollection
import org.locationtech.geomesa.utils.geotools.ContentFeatureSourceSupport
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.FR
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/** A service for adding to security to feature readers.
 */
object DataStoreSecurityService extends Logging {

  lazy val provider: DataStoreSecurityProvider = {
    val providers = ServiceLoader.load(classOf[DataStoreSecurityProvider]).iterator()
    
    if (providers.hasNext) {
      val first = providers.next()
      if (providers.hasNext) {
        logger.warn(s"Multiple security providers found.  Using the first: '$first'")
      }
      first
    } else {
      logger.info(s"No security provider found.  No security will be provided.")
      NoSecurityProvider
    }
  }
}

trait DataStoreSecurityProvider {

  /**
   * @param fs the feature source to be secured
   * @return a security decorator wrapping ``fs`` or ``fs`` if there is no registered security provider or if
   *         [[FeatureReader[SimpleFeatureType, FeatureType]]s cannot be secured
   */
  def secure(fs: FeatureSource[SimpleFeatureType, SimpleFeature]): FeatureSource[SimpleFeatureType, SimpleFeature]

  /**
    * @param fr the feature reader to be secured
    * @return a security decorator wrapping ``fr`` or ``fr`` if there is no registered security provider or if
    *         [[FeatureReader[SimpleFeatureType, FeatureType]]s cannot be secured
    */
  def secure(fr: FeatureReader[SimpleFeatureType, SimpleFeature]): FeatureReader[SimpleFeatureType, SimpleFeature]

  /**
   * @param fc the feature collection to be secured
   * @return a security decorator wrapping ``fc`` or ``fc`` if there is no registered security provider or if
   *         [[FeatureReader[SimpleFeatureType, FeatureType]]s cannot be secured
   */
  def secure(fc: FeatureCollection[SimpleFeatureType, SimpleFeature]): FeatureCollection[SimpleFeatureType, SimpleFeature]
}

/** Default implementation provides no security.
  */
object NoSecurityProvider extends DataStoreSecurityProvider {

  override def secure(fs: FeatureSource[SimpleFeatureType, SimpleFeature]) = fs

  override def secure(fc: FeatureCollection[SimpleFeatureType, SimpleFeature]) = fc

  override def secure(fr: FeatureReader[SimpleFeatureType, SimpleFeature]) = fr
}

/** Adds security to a [[FeatureReader]] if a DataStoreSecurityProvider has been registered.
  */
trait ContentFeatureSourceSecuritySupport extends ContentFeatureSourceSupport {

  override def addSupport(query: Query, reader: FR): FR =
    DataStoreSecurityService.provider.secure(super.addSupport(query, reader))
}
