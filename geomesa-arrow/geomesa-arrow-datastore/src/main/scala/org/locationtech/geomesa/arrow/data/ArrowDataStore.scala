/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.data

import java.io.{FileOutputStream, IOException, OutputStream}
import java.net.URL

import org.apache.arrow.memory.RootAllocator
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.arrow.io.{SimpleFeatureArrowFileReader, SimpleFeatureArrowFileWriter}
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

class ArrowDataStore(val url: URL) extends ContentDataStore with FileDataStore {

  private [data] implicit val allocator = new RootAllocator(Long.MaxValue)

  // TODO check writable?
  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = new ArrowFeatureStore(entry)

  override def createTypeNames(): java.util.List[Name] = {
    import scala.collection.JavaConversions._
    Option(getSchema).map(s => new NameImpl(namespaceURI, s.getTypeName)).toList
  }

  override def createSchema(sft: SimpleFeatureType): Unit = {
    val os = createOutputStream()
    try {
      // just write the schema/metadata
      new SimpleFeatureArrowFileWriter(sft, os, Map.empty).close()
    } finally {
      os.close()
    }
  }

  // FileDataStore methods

  override def getSchema: SimpleFeatureType = {
    // TODO cache?
    var reader: SimpleFeatureArrowFileReader = null
    try {
      val is = url.openStream()
      reader = new SimpleFeatureArrowFileReader(is)
      reader.getSchema
    } catch {
      // TODO handle normal errors vs actual errors
      case e: Exception => null
    } finally {
      if (reader != null) {
        reader.close()
      }
    }
  }

  private [data] def createOutputStream(): OutputStream = {
    try {
      url.openConnection().getOutputStream
    } catch {
      case NonFatal(e) =>
        val file = DataUtilities.urlToFile(url)
        if (file == null) {
          throw new IOException(s"URL is not writable: $url", e)
        }
        new FileOutputStream(file)
    }
  }

  override def updateSchema(featureType: SimpleFeatureType): Unit = updateSchema(getSchema.getTypeName, featureType)

  override def getFeatureSource: SimpleFeatureSource = getFeatureSource(getSchema.getTypeName)

  override def getFeatureWriter(filter: Filter, transaction: Transaction): FeatureWriter[SimpleFeatureType, SimpleFeature] =
    getFeatureWriter(getSchema.getTypeName, filter, transaction)

  override def getFeatureWriter(transaction: Transaction): FeatureWriter[SimpleFeatureType, SimpleFeature] =
    getFeatureWriter(getSchema.getTypeName, transaction)

  override def getFeatureWriterAppend(transaction: Transaction): FeatureWriter[SimpleFeatureType, SimpleFeature] =
    getFeatureWriterAppend(getSchema.getTypeName, transaction)

  override def getFeatureReader: FeatureReader[SimpleFeatureType, SimpleFeature] =
    getFeatureReader(new Query(getSchema.getTypeName), Transaction.AUTO_COMMIT)

  override def dispose(): Unit = CloseQuietly(allocator)
}
