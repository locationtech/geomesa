/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.blob.core

import java.io.File

import com.google.common.io.{ByteStreams, Files}
import org.apache.accumulo.core.client.admin.TimeType
import org.apache.accumulo.core.client.{Scanner, TableExistsException}
import org.apache.accumulo.core.data.{Key, Mutation, Range, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, _}
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, SelfClosingIterator}
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._
import org.locationtech.geomesa.blob.core.handlers.BlobStoreFileHandler
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

class AccumuloBlobStore(ds: AccumuloDataStore) {

  private val connector = ds.connector
  private val tableOps = connector.tableOperations()

  val blobTableName = s"${ds.catalogTable}_blob"

  AccumuloVersion.ensureTableExists(connector, blobTableName)
  ds.createSchema(sft)
  val bw = connector.createBatchWriter(blobTableName, GeoMesaBatchWriterConfig())
  val fs = ds.getFeatureSource(blobFeatureTypeName).asInstanceOf[SimpleFeatureStore]

  def put(file: File, params: Map[String, String]): Option[String] = {
    BlobStoreFileHandler.buildSF(file, params).map {
      sf =>
        val id = sf.getAttribute(idFieldName).asInstanceOf[String]

        fs.addFeatures(new ListFeatureCollection(sft, List(sf)))
        putInternal(file, id)
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

  private def buildReturn(entry: java.util.Map.Entry[Key, Value]): (Array[Byte], String) = {
    val key = entry.getKey
    val value = entry.getValue

    val filename = key.getColumnQualifier.toString

    (value.get, filename)
  }

  private def putInternal(file: File, id: String) {
    val localName = file.getName
    val bytes =  ByteStreams.toByteArray(Files.newInputStreamSupplier(file))

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

  // TODO: Add metadata hashmap?
  val sftSpec = s"$filenameFieldName:String,$idFieldName:String,$geomeFieldName:Geometry,$dateFieldName:Date,thumbnail:String"

  val sft: SimpleFeatureType = SimpleFeatureTypes.createType(blobFeatureTypeName, sftSpec)
}