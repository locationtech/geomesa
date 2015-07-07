/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.util.{List => JList}

import com.google.common.collect.Lists
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.feature._
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.MinMaxTimeVisitor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
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

      val minMaxVisitorO = collection.getSchema.getDtgField.map { new MinMaxTimeVisitor(_) }
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
    val dateField = sft.getDtgField

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
