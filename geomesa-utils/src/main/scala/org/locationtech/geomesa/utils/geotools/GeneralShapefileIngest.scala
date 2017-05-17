/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType

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

  def ingestToDataStore(shapefilePath: String, ds: DataStore, typeName: Option[String]): Unit = {
    val shapefile = getShapefileDatastore(shapefilePath)
    try {
      ingestToDataStore(shapefile.getFeatureSource.getFeatures, ds, typeName)
    } finally {
      if (shapefile != null) {
        shapefile.dispose()
      }
    }
  }

  def ingestToDataStore(features: SimpleFeatureCollection, ds: DataStore, typeName: Option[String]): Unit = {
    // Add the ability to rename this FT
    val featureType: SimpleFeatureType = typeName.map { name =>
      val sftBuilder = new SimpleFeatureTypeBuilder()
      sftBuilder.init(features.getSchema)
      sftBuilder.setName(name)
      sftBuilder.buildFeatureType()
    }.getOrElse(features.getSchema)

    val featureTypeName = featureType.getName.getLocalPart

    val existingSchema = Try(ds.getSchema(featureTypeName)).getOrElse(null)
    if (existingSchema == null) {
      ds.createSchema(featureType)
    }

    val reTypedSFC = new TypeUpdatingFeatureCollection(features, ds.getSchema(featureTypeName))

    WithClose(ds.getFeatureWriterAppend(featureTypeName, Transaction.AUTO_COMMIT)) { writer =>
      SelfClosingIterator(reTypedSFC.features()).foreach { feature =>
        try {
          FeatureUtils.copyToWriter(writer, feature)
          writer.write()
        } catch {
          case NonFatal(e) => logger.warn(s"Error writing feature: $feature: $e", e)
        }
      }
    }
  }
}


