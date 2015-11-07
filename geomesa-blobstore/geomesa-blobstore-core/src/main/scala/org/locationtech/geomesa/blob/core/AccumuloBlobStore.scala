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
import org.opengis.filter.Filter
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, _}
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, SelfClosingIterator}
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._
import org.locationtech.geomesa.blob.core.handlers.BlobStoreFileHander
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.{Conversions, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class AccumuloBlobStore(ds: AccumuloDataStore) {

  private val connector = ds.connector
  private val tableOps = connector.tableOperations()

  val blobTableName = s"${ds.catalogTable}_blob"

  ensureTableExists(blobTableName)
  ds.createSchema(sft)
  val bw = connector.createBatchWriter(blobTableName, GeoMesaBatchWriterConfig())
  val scanner: Scanner = connector.createScanner(blobTableName, new Authorizations())

  val fs = ds.getFeatureSource(blobFeatureTypeName).asInstanceOf[SimpleFeatureStore]

  def put(file: File, params: Map[String, String]): String = {

    val sf = BlobStoreFileHander.buildSF(file, params)
    val id = sf.getAttribute(idFieldName).asInstanceOf[String]

    fs.addFeatures(new ListFeatureCollection(sft, List(sf)))
    putInternal(file, id)
    id
  }

  def getIds(filter: Filter): Iterator[String] = {
    getIds(new Query(blobFeatureTypeName, filter))
  }

  def getIds(query: Query): Iterator[String] = {
    fs.getFeatures(query).features.map(_.getAttribute(idFieldName).asInstanceOf[String])
  }

  def get(id: String): (Array[Byte], String) = {
    scanner.setRange(new Range(new Text(id)))

    val iter = SelfClosingIterator(scanner)
    val ret = buildReturn(iter.next)
    iter.close()
    ret
  }

  def buildReturn(entry: java.util.Map.Entry[Key, Value]): (Array[Byte], String) = {
    val key = entry.getKey
    val value = entry.getValue

    val filename = key.getColumnQualifier.toString

    (value.get, filename)
  }

  def putInternal(file: File, id: String) {
    val localName = file.getName
    val bytes =  ByteStreams.toByteArray(Files.newInputStreamSupplier(file))

    val m = new Mutation(id)

    m.put(EMPTY_COLF, new Text(localName), new Value(bytes))
    bw.addMutation(m)
  }

  private def ensureTableExists(table: String) =
    if (!tableOps.exists(table)) {
      try {
        tableOps.create(table, true, TimeType.LOGICAL)
      } catch {
        case e: TableExistsException => // this can happen with multiple threads but shouldn't cause any issues
      }
    }
}

object AccumuloBlobStore {
  val blobFeatureTypeName = "blob"
  val idFieldName = "storeId"

  // TODO: Add metadata hashmap?
  val sftSpec = s"filename:String,$idFieldName:String,geom:Geometry,time:Date,thumbnail:String"

  val sft: SimpleFeatureType = SimpleFeatureTypes.createType(blobFeatureTypeName, sftSpec)
}