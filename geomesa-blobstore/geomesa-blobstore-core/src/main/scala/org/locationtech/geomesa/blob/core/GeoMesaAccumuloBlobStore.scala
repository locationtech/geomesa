/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.blob.core

import java.io.File
import java.util.{Iterator => JIterator, Map => JMap}

import com.google.common.io.Files
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.blob.core.GeoMesaBlobStoreSFT._
import org.locationtech.geomesa.blob.core.handlers.{BlobStoreFileHandler, _}
import org.locationtech.geomesa.utils.filters.Filters
import org.locationtech.geomesa.utils.geotools.Conversions.{RichSimpleFeature, _}
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

class GeoMesaAccumuloBlobStore(ds: AccumuloDataStore) extends GeoBlobStore
  with BlobStoreFileName with LazyLogging {

  protected val blobTableName = s"${ds.catalogTable}_blob"

  ds.createSchema(sft)

  protected val bwConf = GeoMesaBatchWriterConfig().setMaxWriteThreads(ds.config.writeThreads)
  protected val fs     = ds.getFeatureSource(BlobFeatureTypeName).asInstanceOf[SimpleFeatureStore]

  protected val accBlobStore = new AccumuloBlobStoreImpl(ds.connector,
    blobTableName,
    ds.authProvider,
    ds.auditProvider,
    bwConf)

  override def put(file: File, params: JMap[String, String]): String = {
    BlobStoreFileHandler.buildSF(file, params.toMap).map { sf =>
      putInternalSF(sf, Files.toByteArray(file))
    }.orNull
  }

  override def put(bytes: Array[Byte], params: JMap[String, String]): String = {
    val sf = BlobStoreByteArrayHandler.buildSF(params)
    putInternalSF(sf, bytes)
  }

  override def getIds(filter: Filter): JIterator[String] = {
    getIds(new Query(BlobFeatureTypeName, filter))
  }

  override def getIds(query: Query): JIterator[String] = {
    fs.getFeatures(query).features.map(_.get[String](IdFieldName))
  }

  override def get(id: String): JMap.Entry[String, Array[Byte]] = {
    accBlobStore.get(id)
  }

  override def delete(id: String): Unit = {
    accBlobStore.deleteBlob(id)
    deleteFeature(id)
  }

  override def deleteBlobStore(): Unit = {
    ds.delete()
    accBlobStore.deleteBlobStore()
  }

  private def putInternalSF(sf: SimpleFeature, bytes: Array[Byte]): String = {
    val id = sf.get[String](IdFieldName)
    val localName = sf.get[String](FilenameFieldName)
    fs.addFeatures(new ListFeatureCollection(sft, List(sf)))
    accBlobStore.put(id, localName, bytes)
    id
  }

  private def deleteFeature(id: String): Unit = {
    val removalFilter = Filters.ff.id(new FeatureIdImpl(id))
    fs.removeFeatures(removalFilter)
  }

  override def close(): Unit = {
    accBlobStore.close()
  }
}

