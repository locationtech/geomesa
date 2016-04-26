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

import com.google.common.collect.Maps
import com.google.common.io.Files
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Key, Mutation, Range, Value}
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, _}
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, SelfClosingIterator}
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._
import org.locationtech.geomesa.blob.core.handlers.{BlobStoreFileHandler, _}
import org.locationtech.geomesa.blob.core.interop.GeoMesaBlobStore
import org.locationtech.geomesa.utils.filters.Filters
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.{SftBuilder, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

class AccumuloBlobStore(ds: AccumuloDataStore) extends GeoMesaBlobStore
  with BlobStoreFileName with LazyLogging {

  private val connector = ds.connector
  private val tableOps = connector.tableOperations()

  val blobTableName = s"${ds.catalogTable}_blob"

  AccumuloVersion.ensureTableExists(connector, blobTableName)
  ds.createSchema(sft)
  val bwc = GeoMesaBatchWriterConfig()
  // TODO: https://geomesa.atlassian.net/browse/GEOMESA-1177
  val bw = connector.createBatchWriter(blobTableName, bwc)
  val fs = ds.getFeatureSource(blobFeatureTypeName).asInstanceOf[SimpleFeatureStore]

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
    getIds(new Query(blobFeatureTypeName, filter))
  }

  override def getIds(query: Query): JIterator[String] = {
    fs.getFeatures(query).features.map(_.getAttribute(idFieldName).asInstanceOf[String])
  }

  override def get(id: String): JMap.Entry[String, Array[Byte]] = {
    val ret = getInternal(id)
    Maps.immutableEntry(ret._1, ret._2)
  }

  override def delete(id: String): Unit = {
    deleteBlob(id)
    deleteFeature(id)
  }

  override def deleteBlobStore(): Unit = {
    try {
      tableOps.delete(blobTableName)
      ds.delete()
    } catch {
      case NonFatal(e) => logger.error("Error when deleting BlobStore", e)
    }
  }

  private def getInternal(id: String): (String, Array[Byte]) = {
    val scanner = connector.createScanner(
      blobTableName,
      ds.authProvider.getAuthorizations
    )
    scanner.setRange(new Range(new Text(id)))

    val iter = SelfClosingIterator(scanner)
    if (iter.hasNext) {
      val ret = buildReturn(iter.next)
      iter.close()
      ret
    } else {
      ("", Array.empty[Byte])
    }
  }

  private def putInternalSF(sf: SimpleFeature, bytes: Array[Byte]): String = {
    val id = sf.getAttribute(idFieldName).asInstanceOf[String]
    val localName = sf.getAttribute(filenameFieldName).asInstanceOf[String]
    fs.addFeatures(new ListFeatureCollection(sft, List(sf)))
    putInternalBlob(id, localName, bytes)
    id
  }

  private def putInternalBlob(id: String, localName: String, bytes: Array[Byte]): Unit = {
    val m = new Mutation(id)
    m.put(EMPTY_COLF, new Text(localName), new Value(bytes))
    bw.addMutation(m)
    bw.flush()
  }

  private def buildReturn(entry: JMap.Entry[Key, Value]): (String, Array[Byte]) = {
    val key = entry.getKey
    val value = entry.getValue

    val filename = key.getColumnQualifier.toString

    (filename, value.get)
  }

  private def deleteFeature(id: String): Unit = {
    val removalFilter = Filters.ff.id(new FeatureIdImpl(id))
    fs.removeFeatures(removalFilter)
  }

  private def deleteBlob(id: String): Unit = {
    val bd = connector.createBatchDeleter(
      blobTableName,
      ds.authProvider.getAuthorizations,
      bwc.getMaxWriteThreads,
      bwc)
    bd.setRanges(List(new Range(new Text(id))))
    bd.delete()
    bd.close()
  }

}

object AccumuloBlobStore {
  val blobFeatureTypeName = "blob"

  val idFieldName = "storeId"
  val geomeFieldName = "geom"
  val filenameFieldName = "filename"
  val dateFieldName = "date"
  val thumbnailFieldName = "thumbnail"

  // TODO: Add metadata hashmap?
  // TODO GEOMESA-1186 allow for configurable geometry types
  val sft = new SftBuilder()
    .stringType(filenameFieldName)
    .stringType(idFieldName, index = true)
    .geometry(geomeFieldName, default = true)
    .date(dateFieldName, default = true)
    .stringType(thumbnailFieldName)
    .userData(SimpleFeatureTypes.MIXED_GEOMETRIES, "true")
    .build(blobFeatureTypeName)
  
}
