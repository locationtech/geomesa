/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.plugin.ui

import java.util

import geomesa.core.data.AccumuloDataStore
import geomesa.core.data.AccumuloDataStoreFactory.params._
import geomesa.plugin.ui.components.DataStoreInfoPanel
import org.apache.accumulo.core.Constants
import org.apache.accumulo.core.client.{Connector, IsolatedScanner}
import org.apache.accumulo.core.data.KeyExtent
import org.apache.hadoop.io.Text
import org.apache.wicket.markup.html.internal.HtmlHeaderContainer
import org.apache.wicket.markup.html.list.{ListItem, ListView}
import org.geoserver.catalog.StoreInfo
import org.geoserver.web.data.store.{StorePanel, StoreProvider}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.geometry.jts.ReferencedEnvelope

import scala.collection.JavaConverters._
import scala.collection.mutable

class GeoMesaDataStoresPage extends GeoMesaBasePage {

  import GeoMesaDataStoresPage._

  // TODO count in results chart shows total stores, not just the filtered ones
  private val storeProvider = new StoreProvider() {
    override def getFilteredItems: java.util.List[StoreInfo] = {
      getItems.asScala.filter(_.getType == "Accumulo Feature Data Store").asJava
    }
  }

  private val table = new StorePanel("storeTable", storeProvider, true)
  table.setOutputMarkupId(true)
  add(table)

  private val connections = getStoreConnections

  @transient
  private val dataStores = for {
    connection <- connections
    params = getDataStoreParams(connection)
  } yield {
    DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
  }

  // TODO use something better than toString to uniquely identify each data store.
  // we need a unique ID for each one to use as the key to the various maps -
  // we could use the data store object itself, but the wicket framework throws exceptions b/c
  // they aren't serializable.
  private val dataStoreIds = dataStores.map(_.toString)

  private val dataStoreNames = mutable.HashMap.empty[String, String]

  private val tables = mutable.HashMap.empty[String, String]

  private val metadata = mutable.HashMap.empty[String, TableMetadata]

  private val features = mutable.HashMap.empty[String, List[String]]

  private val bounds = mutable.HashMap.empty[String, mutable.Map[String, FeatureModel]]

  dataStores.foreach {
    d =>
      val id = d.toString
      dataStoreNames.put(id, s"${d.connector.getInstance.getInstanceName}: ${d.tableName}")
      val table = d.tableName
      tables.put(id, table)
      metadata.put(id, getTableMetadata(d.connector, table))
      val typeNames = d.getTypeNames.toList
      features.put(id, typeNames)
      val boundsPerFeature = mutable.HashMap.empty[String, FeatureModel]
      typeNames.foreach(typeName => boundsPerFeature.put(typeName, FeatureModel(typeName, d.getBounds(new Query(typeName)))))
      bounds.put(id, boundsPerFeature)
  }

  add(new ListView[String]("storeDetails", dataStoreIds.toList.asJava) {
    override def populateItem(item: ListItem[String]) = {
      val dataStore = item.getModelObject
      item.add(new DataStoreInfoPanel("storeMetadata",
                                       dataStoreNames.get(dataStore).get,
                                       metadata.get(dataStore).get,
                                       features.get(dataStore).get,
                                       bounds.get(dataStore).get.toMap))
    }
  })

  /**
   * Injects the data needed by d3 for charts
   *
   * @param container
   */
  override def renderHead(container: HtmlHeaderContainer): Unit = {
    super.renderHead(container);
    val values = metadata.values
                   .map(entry => s"{name: '${entry.tableName}', value: ${entry.numEntries}}")
                   .toList
                   .sortWith(_ > _)
    val string = s"var data = [ ${values.mkString(",")} ]"
    container.getHeaderResponse.renderJavascript(string, "rawData")
  }

  /**
   * Gets the connection info for each unique store registered in GeoServer
   *
   * @return
   */
  private def getStoreConnections : Set[AccumuloConnectionInfo] = {
    val stores = storeProvider.iterator(0, storeProvider.fullSize).asScala

    stores.map {
      provider =>
        val params = provider.getConnectionParameters.asScala

        val instanceId = params.getOrElse(instanceIdParam.key, "").toString
        val zookeepers = params.getOrElse(zookeepersParam.key, "").toString
        val user = params.getOrElse(userParam.key, "").toString
        val password = params.getOrElse(passwordParam.key, "").toString
        val table = params.getOrElse(tableNameParam.key, "").toString

        AccumuloConnectionInfo(zookeepers, instanceId, user, password, table)
    }.toSet
  }

  /**
   *
   * @param connectionInfo
   * @return
   */
  private def getDataStoreParams(connectionInfo: AccumuloConnectionInfo): Map[String, String] =
    Map(
         zookeepersParam.key  -> connectionInfo.zookeepers,
         instanceIdParam.key  -> connectionInfo.instanceId,
         userParam.key        -> connectionInfo.user,
         passwordParam.key    -> connectionInfo.password,
         tableNameParam.key   -> connectionInfo.table
       )

}

object GeoMesaDataStoresPage {

  /**
   * Gets table metadata from accumulo.
   *
   * Metadata rows have the format:
   *    <table-id>;<tablet-end-row> : file /<tablet-id>/<filename> <fileSize>,<numEntries>
   * each row is a tablet, each column family is the word 'file', each column qualifier is the tablet
   * and file name, and the value is the file size and the number of entries in that file
   *
   * @param connector
   * @param table
   * @return
   *   (tableName, number of tablets, number of splits, total number of entries, total file size)
   */
  def getTableMetadata(connector: Connector, table: String): TableMetadata = {
    // TODO move this to core utility class where it can be re-used
    val tableId = Option(connector.tableOperations.tableIdMap.get(table))

    tableId match {
      case Some(id) => getTableMetadata(connector, table, id)
      case None => TableMetadata(table, 0, 0, 0, 0)
    }
  }

  /**
   * Gets table metadata from accumulo.
   *
   * Metadata rows have the format:
   *    <table-id>;<tablet-end-row> : file /<tablet-id>/<filename> <fileSize>,<numEntries>
   * each row is a tablet, each column family is the word 'file', each column qualifier is the tablet
   * and file name, and the value is the file size and the number of entries in that file
   * @param connector
   * @param table
   * @param tableId
   * @return (tableName, number of tablets, number of splits, total number of entries, total file size)
   */
  private def getTableMetadata(connector: Connector, table: String, tableId: String): TableMetadata = {

    val scanner = new IsolatedScanner(connector.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS))
    scanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY)
    scanner.setRange(new KeyExtent(new Text(tableId), null, null).toMetadataRange())

    var fileSize:Long = 0
    var numEntries:Long = 0
    var numSplits:Long = 0
    var numTablets:Long = 0

    var lastTablet = ""

    scanner.asScala.foreach {
      case entry =>
        //  example cq: /t-0005bta/F0005bum.rf
        val cq = entry.getKey.getColumnQualifier.toString
        val tablet = cq.split("/")(1)
        if (lastTablet != tablet) {
          numTablets = numTablets + 1
          lastTablet = tablet
        }
        // example value: 79362732,2171839
        val components = entry.getValue.toString.split(",")
        fileSize = fileSize + components(0).toLong
        numEntries = numEntries + components(1).toLong
        numSplits = numSplits + 1
    }

    TableMetadata(table, numTablets, numSplits, numEntries, fileSize / 1048576.0)
  }
}

case class AccumuloConnectionInfo(zookeepers: String,
                                  instanceId: String,
                                  user: String,
                                  password: String,
                                  table: String)

case class TableMetadata(tableName: String,
                         numTablets: Long,
                         numSplits: Long,
                         numEntries: Long,
                         fileSize: Double)

case class FeatureModel (name: String, bounds: ReferencedEnvelope)