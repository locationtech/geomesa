/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.blob.core

import java.io.File
import java.util

import com.google.common.io.{ByteStreams, Files}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Key, Mutation, Range, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.geotools.data.{Transaction, Query}
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, _}
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, SelfClosingIterator}
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._
import org.locationtech.geomesa.blob.core.handlers.BlobStoreFileHandler
import org.locationtech.geomesa.utils.filters.Filters
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.{SftBuilder, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

class AccumuloBlobStore(ds: AccumuloDataStore) extends LazyLogging with BlobStoreFileName {

  private val connector = ds.connector
  private val tableOps = connector.tableOperations()

  val blobTableName = s"${ds.catalogTable}_blob"

  AccumuloVersion.ensureTableExists(connector, blobTableName)
  ds.createSchema(sft)
  val bwc = GeoMesaBatchWriterConfig()
  val bw = connector.createBatchWriter(blobTableName, bwc)
  val fs = ds.getFeatureSource(blobFeatureTypeName).asInstanceOf[SimpleFeatureStore]

  def put(file: File, params: Map[String, String]): Option[String] = {
    BlobStoreFileHandler.buildSF(file, params).map {
      sf =>
        val id = sf.getAttribute(idFieldName).asInstanceOf[String]

        fs.addFeatures(new ListFeatureCollection(sft, List(sf)))
        putInternal(file, id, params)
        id
    }
  }

  def getIds(filter: Filter): Iterator[String] = {
    getIds(new Query(blobFeatureTypeName, filter))
  }

  def getIds(query: Query): Iterator[String] = {
    fs.getFeatures(query).features.map(_.getAttribute(idFieldName).asInstanceOf[String])
  }

  def get(id: String): (Array[Byte], String) = {
    // TODO: Get Authorizations using AuthorizationsProvider interface
    // https://geomesa.atlassian.net/browse/GEOMESA-986
    val scanner = connector.createScanner(blobTableName, new Authorizations())
    scanner.setRange(new Range(new Text(id)))

    val iter = SelfClosingIterator(scanner)
    if (iter.hasNext) {
      val ret = buildReturn(iter.next)
      iter.close()
      ret
    } else {
      (Array.empty[Byte], "")
    }
  }

  def delete(id: String): Unit = {
    // TODO: Get Authorizations using AuthorizationsProvider interface
    // https://geomesa.atlassian.net/browse/GEOMESA-986
    val bd = connector.createBatchDeleter(blobTableName, new Authorizations(), bwc.getMaxWriteThreads, bwc)
    bd.setRanges(List(new Range(new Text(id))))
    bd.delete()
    bd.close()
    deleteFeature(id)
  }

  private def deleteFeature(id: String): Unit = {
    val removalFilter = Filters.ff.id(Filters.ff.featureId(id))
    val fd = ds.getFeatureWriter(blobFeatureTypeName, removalFilter, Transaction.AUTO_COMMIT)
    try {
      fd.remove()
    } catch {
      case e: Exception =>
        logger.error("Couldn't remove feature from blobstore", e)
    } finally {
      fd.close()
    }
  }

  private def buildReturn(entry: java.util.Map.Entry[Key, Value]): (Array[Byte], String) = {
    val key = entry.getKey
    val value = entry.getValue

    val filename = key.getColumnQualifier.toString

    (value.get, filename)
  }

  private def putInternal(file: File, id: String, params: Map[String, String]) {
    val localName = getFileName(file, params)
    val bytes = ByteStreams.toByteArray(Files.asByteSource(file).openBufferedStream())

    val m = new Mutation(id)

    m.put(EMPTY_COLF, new Text(localName), new Value(bytes))
    bw.addMutation(m)
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
  val sftSpec = new SftBuilder()
    .stringType(filenameFieldName)
    .stringType(idFieldName, true)
    .geometry(geomeFieldName, true)
    .date(dateFieldName)
    .withDefaultDtg(dateFieldName)
    .stringType(thumbnailFieldName)
    .getSpec

  val sft: SimpleFeatureType = SimpleFeatureTypes.createType(blobFeatureTypeName, sftSpec)
}

trait BlobStoreFileName {

  def getFileNameFromParams(params: util.Map[String, String]): Option[String] = {
    Option(params.get(filenameFieldName))
  }

  def getFileName(file: File, params: util.Map[String, String]): String = {
    getFileNameFromParams(params).getOrElse(file.getName)
  }

}