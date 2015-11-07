package org.locationtech.geomesa.blob.core

import java.io.File

import org.apache.accumulo.core.client.TableExistsException
import org.apache.accumulo.core.client.admin.TimeType
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{Transaction, DataStore, Query}
import org.geotools.filter.Filter
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.blob.core.handlers.BlobStoreFileHander
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}
import scala.collection.JavaConversions._

import AccumuloBlobStore._

class AccumuloBlobStore(ds: AccumuloDataStore) {

  //val ds: DataStore = ???
  private val connector = ds.connector
  private val tableOps = connector.tableOperations()

  val blobTableName = s"${ds.catalogTable}_blob"

  ensureTableExists(blobTableName)

  val fs = ds.getFeatureSource(blobFeatureTypeName).asInstanceOf[SimpleFeatureStore]

  def put(file: File, params: Map[String, String]): String = {

    val sf = BlobStoreFileHander.buildSF(file, params)
    val id = sf.getAttribute("storeID").asInstanceOf[String]

    fs.addFeatures(new ListFeatureCollection(sft, List(sf)))
    putInternal(file, id)
    id
  }

  def getIds(filter: Filter): Seq[String] = ???
  def getIds(query: Query): Seq[String] = ???

  def get(id: String): File = {
    ???
  }


  def putInternal(file: File, id: String) {


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
  val sft: SimpleFeatureType = ???

}