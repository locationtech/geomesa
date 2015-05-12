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
package org.locationtech.geomesa.kafka

import java.util

import com.google.common.cache.{CacheLoader, CacheBuilder}
import org.geotools.data.DataStore
import org.geotools.data.store.ContentEntry
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

abstract class KafkaDataStoreSchemaManager(zookeepers: String, zkPath: String) extends DataStore  {

  private[kafka] val schemaCache = CacheBuilder.newBuilder().build(
    new CacheLoader[String, SimpleFeatureType] {
      override def load(name: String): SimpleFeatureType = getSchema(name)

    })

  override def getSchema(name: Name): SimpleFeatureType = getSchema(name.getLocalPart)
  
  override def getSchema(typeName: String): SimpleFeatureType =
    resolveTopicSchema(typeName)
      .getOrElse(throw new IllegalArgumentException(s"Unable to find schema with name $typeName"))

  override def createSchema(featureType: SimpleFeatureType): Unit = ???

  override def getNames: util.List[Name] = ???

  override def removeSchema(typeName: Name): Unit = removeSchema(typeName.getLocalPart)

  override def removeSchema(typeName: String): Unit = {
    schemaCache.invalidate(typeName)
    ???
  }

  override def updateSchema(typeName: String, featureType: SimpleFeatureType): Unit =
    throw new UnsupportedOperationException

  override def updateSchema(typeName: Name, featureType: SimpleFeatureType): Unit =
    throw new UnsupportedOperationException


  private def resolveTopicSchema(typeName: String): Option[SimpleFeatureType] = ???
}
