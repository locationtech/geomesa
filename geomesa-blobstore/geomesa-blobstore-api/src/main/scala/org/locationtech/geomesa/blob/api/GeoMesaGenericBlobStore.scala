/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.api

import java.io.File
import java.util

import com.google.common.io.Files
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, Query}
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.blob.api.GeoMesaBlobStoreSFT._
import org.locationtech.geomesa.blob.api.handlers.{BlobStoreFileHandler, ByteArrayHandler}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

abstract class GeoMesaGenericBlobStore(ds: DataStore, bs: BlobStore) extends GeoMesaIndexedBlobStore {

  ds.createSchema(sft)

  protected val fs = ds.getFeatureSource(BlobFeatureTypeName).asInstanceOf[SimpleFeatureStore]

  override def get(id: String): Blob = {
    bs.get(id)
  }

  override def put(file: File, params: util.Map[String, String]): String = {
    BlobStoreFileHandler.buildSimpleFeature(file, params.toMap).map { sf =>
      val bytes = Files.toByteArray(file)
      putInternalSF(sf, bytes)
    }.orNull
  }

  override def put(bytes: Array[Byte], params: util.Map[String, String]): String = {
    val sf = ByteArrayHandler.buildSimpleFeature(params)
    putInternalSF(sf, bytes)
  }

  private def putInternalSF(sf: SimpleFeature, bytes: Array[Byte]): String = {
    val id = sf.get[String](IdFieldName)
    val localName = sf.get[String](FilenameFieldName)
    fs.addFeatures(new ListFeatureCollection(sft, List(sf)))
    bs.put(id, localName, bytes)
    id
  }

  override def delete(id: String): Unit = {
    val removalFilter = org.locationtech.geomesa.filter.ff.id(new FeatureIdImpl(id))
    fs.removeFeatures(removalFilter)
    bs.deleteBlob(id)
  }

  override def deleteBlobStore(): Unit = {
    ds.removeSchema(BlobFeatureTypeName)
    bs.deleteBlobStore()
  }

  override def getIds(filter: Filter): util.Iterator[String] = {
    getIds(new Query(BlobFeatureTypeName, filter))
  }

  override def getIds(query: Query): util.Iterator[String] = {
    SelfClosingIterator(fs.getFeatures(query).features).map(_.get[String](IdFieldName))
  }

  override def close(): Unit = {
    bs.close()
  }
}
