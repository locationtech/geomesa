/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.data

import java.io.{FileOutputStream, IOException, InputStream, OutputStream}
import java.net.URL

import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.feature.NameImpl
import org.geotools.util.URLs
import org.locationtech.geomesa.arrow.io.{SimpleFeatureArrowFileReader, SimpleFeatureArrowFileWriter}
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, HasGeoMesaMetadata, NoOpMetadata}
import org.locationtech.geomesa.index.stats.MetadataBackedStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.Try
import scala.util.control.NonFatal

class ArrowDataStore(val url: URL, caching: Boolean) extends ContentDataStore with FileDataStore
    with HasGeoMesaMetadata[String] with HasGeoMesaStats {

  import org.locationtech.geomesa.arrow.allocator

  private var initialized = false

  // note: to avoid cache issues, don't allow writing if caching is enabled
  private lazy val writable = !caching && Try(createOutputStream()).map(_.close()).isSuccess

  private lazy val reader: SimpleFeatureArrowFileReader = {
    initialized = true
    if (caching) {
      SimpleFeatureArrowFileReader.caching(createInputStream())
    } else {
      SimpleFeatureArrowFileReader.streaming(() => createInputStream())
    }
  }

  lazy val dictionaries: Map[String, ArrowDictionary] = reader.dictionaries

  override val metadata: GeoMesaMetadata[String] = new NoOpMetadata()
  override val stats: GeoMesaStats = new UnoptimizedRunnableStats(this)

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    if (writable) {
      new ArrowFeatureStore(entry, reader)
    } else {
      new ArrowFeatureSource(entry, reader)
    }
  }

  override def createTypeNames(): java.util.List[Name] = {
    import scala.collection.JavaConversions._
    Option(getSchema).map(s => new NameImpl(namespaceURI, s.getTypeName)).toList
  }

  override def createSchema(sft: SimpleFeatureType): Unit = {
    if (!writable) {
      throw new IllegalArgumentException("Can't write to the provided URL, or caching is enabled")
    }
    WithClose(createOutputStream(false)) { os =>
      WithClose(SimpleFeatureArrowFileWriter(sft, os)) { writer =>
        // just write the schema/metadata
        writer.start()
      }
    }
  }

  override def dispose(): Unit = {
    // avoid instantiating the lazy reader if it hasn't actually been used
    if (initialized) {
      reader.close()
    }
  }

  // FileDataStore method

  override def getSchema: SimpleFeatureType = {
    if (WithClose(createInputStream()) { _.available() < 1 }) { null } else {
      try { reader.sft } catch {
        case e: Exception => throw new IOException("Error reading schema", e)
      }
    }
  }

  private def createInputStream(): InputStream = url.openStream()

  private [data] def createOutputStream(append: Boolean = true): OutputStream = {
    Option(URLs.urlToFile(url)).map(new FileOutputStream(_, append)).getOrElse {
      try {
        val connection = url.openConnection()
        connection.setDoOutput(true)
        connection.getOutputStream
      } catch {
        case NonFatal(e) => throw new IOException(s"URL is not writable: $url", e)
      }
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
}
