/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.io.{File, Serializable}
import java.util.{Map => JMap}

import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.Try

object GeneralShapefileIngest {
  def shpToDataStoreViaParams(shapefilePath: String, params: JMap[String, Serializable]): DataStore =
   shpToDataStoreViaParams(shapefilePath, params, null)

  def shpToDataStoreViaParams(shapefilePath: String,
                              params: JMap[String, Serializable], featureName: String): DataStore = {
    val shapefile = getShapefileDatastore(shapefilePath)
    val features = shapefile.getFeatureSource.getFeatures
    val newDS = featuresToDataStoreViaParams(features, params, featureName)
    shapefile.dispose()
    newDS
  }

  // The goal of this method is to allow for URL-based look-ups.
  //  This allows for us to ingest files from HDFS, S3, and Azure (WASBS and WASB).
  def getShapefileDatastore(shapefilePath: String): FileDataStore = {
    // NOTE this regex is designed to work for s3a, s3n, etc.
    if (shapefilePath.matches("""\w{3,5}:\/\/.*$""")) {
      DataStoreFinder.getDataStore(Map("url" -> shapefilePath)).asInstanceOf[FileDataStore]
    } else {
      FileDataStoreFinder.getDataStore(new File(shapefilePath))
    }
  }

  def shpToDataStore(shapefilePath: String, ds: DataStore, featureName: String): DataStore = {
    val shapefile = getShapefileDatastore(shapefilePath)
    val features = shapefile.getFeatureSource.getFeatures
    val newDS = featuresToDataStore(features, ds, featureName)
    shapefile.dispose()
    newDS
  }

  def featuresToDataStoreViaParams(features: SimpleFeatureCollection,
                                   params: JMap[String, Serializable],
                                   featureName: String): DataStore = {
    val ds = DataStoreFinder.getDataStore(params)
    featuresToDataStore(features, ds, featureName)
  }

  def featuresToDataStore(features: SimpleFeatureCollection,
                          ds: DataStore,
                          featureName: String): DataStore = {
    // Add the ability to rename this FT
    val featureType: SimpleFeatureType =
      if(featureName != null) {   // Is this line right?
        val originalFeatureType = features.getSchema
        val sftBuilder = new SimpleFeatureTypeBuilder()
        sftBuilder.init(originalFeatureType)
        sftBuilder.setName(featureName)
        sftBuilder.buildFeatureType()
      } else
        features.getSchema

    val featureTypeName = featureType.getName.getLocalPart

    val existingSchema = Try { ds.getSchema(featureTypeName) }.getOrElse(null)
    if (existingSchema == null) {
      ds.createSchema(featureType)
    }

    val newType = ds.getSchema(featureTypeName)

    val reTypedSFC = new TypeUpdatingFeatureCollection(features, newType)

    val fs: FeatureStore[SimpleFeatureType, SimpleFeature] =
      ds.getFeatureSource(featureTypeName).asInstanceOf[FeatureStore[SimpleFeatureType, SimpleFeature]]
    val transaction = new DefaultTransaction("create")
    fs.setTransaction(transaction)

    fs.addFeatures(reTypedSFC)
    transaction.commit()
    transaction.close()

    ds
  }
}


