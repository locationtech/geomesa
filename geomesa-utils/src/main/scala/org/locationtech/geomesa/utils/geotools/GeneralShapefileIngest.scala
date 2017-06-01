/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._
import scala.util.Try
import scala.util.control.NonFatal

object GeneralShapefileIngest extends LazyLogging {

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

  def ingestToDataStore(shapefilePath: String, ds: DataStore, typeName: Option[String]): (Long, Long) = {
    val shapefile = getShapefileDatastore(shapefilePath)
    try {
      ingestToDataStore(shapefile.getFeatureSource.getFeatures, ds, typeName)
    } finally {
      if (shapefile != null) {
        shapefile.dispose()
      }
    }
  }

  def ingestToDataStore(features: SimpleFeatureCollection, ds: DataStore, typeName: Option[String]): (Long, Long) = {
    // Add the ability to rename this FT
    val featureType = {
      val fromCollection = typeName.map { name =>
        val sftBuilder = new SimpleFeatureTypeBuilder()
        sftBuilder.init(features.getSchema)
        sftBuilder.setName(name)
        sftBuilder.buildFeatureType()
      }.getOrElse(features.getSchema)
      val existing = Try(ds.getSchema(fromCollection.getTypeName)).getOrElse(null)
      if (existing != null) { existing } else {
        ds.createSchema(fromCollection)
        ds.getSchema(fromCollection.getTypeName)
      }
    }

    val retype: (SimpleFeature) => SimpleFeature = if (featureType == features.getSchema) {
      (f) => f
    } else {
      (f) => {
        val newFeature = DataUtilities.reType(featureType, f)
        newFeature.setDefaultGeometry(f.getDefaultGeometry)
        newFeature.getUserData.putAll(f.getUserData)
        newFeature
      }
    }

    var count = 0L
    var failed = 0L

    WithClose(ds.getFeatureWriterAppend(featureType.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
      SelfClosingIterator(features.features()).foreach { feature =>
        try {
          FeatureUtils.copyToWriter(writer, retype(feature))
          writer.write()
          count += 1
        } catch {
          case NonFatal(e) => logger.warn(s"Error writing feature: $feature: $e", e); failed += 1
        }
      }
    }

    (count, failed)
  }
}


