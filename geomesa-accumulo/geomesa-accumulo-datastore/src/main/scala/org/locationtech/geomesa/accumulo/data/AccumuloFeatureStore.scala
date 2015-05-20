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

package org.locationtech.geomesa.accumulo.data

import java.util.{List => JList}

import com.google.common.collect.Lists
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.feature._
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.accumulo.index
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.MinMaxTimeVisitor
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.identity.FeatureId

class AccumuloFeatureStore(val dataStore: AccumuloDataStore, val featureName: Name)
    extends AbstractFeatureStore with AccumuloAbstractFeatureSource {
  override def addFeatures(collection: FeatureCollection[SimpleFeatureType, SimpleFeature]): JList[FeatureId] =
    addFeatures(collection, None)


  def addFeatures(collection: FeatureCollection[SimpleFeatureType, SimpleFeature],
                  featureVis: Option[String]): JList[FeatureId] = {
    val fids = Lists.newArrayList[FeatureId]()
    if (collection.size > 0) {
      writeBounds(collection.getBounds)

      val minMaxVisitorO = index.getDtgFieldName(collection.getSchema).map { new MinMaxTimeVisitor(_) }
      val fw = dataStore.getFeatureWriterAppend(featureName.getLocalPart, Transaction.AUTO_COMMIT)

      val updateTimeBounds: SimpleFeature => Unit = { feature => minMaxVisitorO.foreach { _.visit(feature) } }

      val write: SimpleFeature => FeatureId =
        if (minMaxVisitorO.isDefined) { feature =>
          updateTimeBounds(feature)
          writeFeature(fw, feature)
        }
        else { feature =>
          writeFeature(fw, feature)
        }

      val iter = collection.features()
      while (iter.hasNext) {
        val f = iter.next()
        for (fv <- featureVis) SecurityUtils.setFeatureVisibility(f, fv)
        val id = write(f) // keep side effecting code separate for clarity
        fids.add(id)
      }
      iter.close()
      fw.close()

      for {
        mmv <- minMaxVisitorO
        bounds <- Option(mmv.getBounds)
      } dataStore.writeTemporalBounds(featureName.getLocalPart, bounds)
    }
    fids
  }

  def writeFeature(fw: SFFeatureWriter, feature: SimpleFeature): FeatureId = {
    val newFeature = fw.next()

    try {
      newFeature.setAttributes(feature.getAttributes)
      newFeature.getUserData.putAll(feature.getUserData)
    } catch {
      case ex: Exception =>
        throw new DataSourceException(s"Could not create ${featureName.getLocalPart} out of provided feature: ${feature.getID}", ex)
    }

    val useExisting = java.lang.Boolean.TRUE.equals(feature.getUserData.get(Hints.USE_PROVIDED_FID).asInstanceOf[java.lang.Boolean])
    if (getQueryCapabilities().isUseProvidedFIDSupported && useExisting) {
      newFeature.getIdentifier.asInstanceOf[FeatureIdImpl].setID(feature.getID)
    }

    fw.write()
    newFeature.getIdentifier
  }

  def updateTimeBounds(collection: FeatureCollection[SimpleFeatureType, SimpleFeature]) = {
    val sft = collection.getSchema
    val dateField = org.locationtech.geomesa.accumulo.index.getDtgFieldName(sft)

    dateField.flatMap { dtg =>
      val minMax = new MinMaxTimeVisitor(dtg)
      collection.accepts(minMax, null)
      Option(minMax.getBounds)
    }
  }

  def writeTimeBounds(collection: FeatureCollection[SimpleFeatureType, SimpleFeature]) {
    updateTimeBounds(collection).foreach { dataStore.writeTemporalBounds(featureName.getLocalPart, _) }
  }

  def writeBounds(envelope: ReferencedEnvelope) {
    if(envelope != null)
      dataStore.writeSpatialBounds(featureName.getLocalPart, envelope)
  }
}

object MapReduceAccumuloFeatureStore {
  val MAPRED_CLASSPATH_USER_PRECEDENCE_KEY = "mapreduce.task.classpath.user.precedence"
}
