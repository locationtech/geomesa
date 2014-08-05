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

import scala.collection.JavaConverters._
import scala.collection.mutable

class GeoMesaDataStoresPage extends GeoMesaBasePage {

  import geomesa.plugin.ui.GeoMesaDataStoresPage._

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

  private val dataStoreNames = mutable.ListBuffer.empty[String]

  private val features = mutable.HashMap.empty[String, List[String]]

  private val metadata = mutable.HashMap.empty[String, Map[String, List[TableMetadata]]]

  private val bounds = mutable.HashMap.empty[String, Map[String, String]]

  private val connectionParams = mutable.HashMap.empty[String, Map[String, String]]

  // compile data store info
  for {
    connection <- connections
    params = getDataStoreParams(connection)
  } {
    val name = connection.name
    val dataStore = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
    dataStoreNames.append(name)

    val featureNames = dataStore.getTypeNames.toList
    features.put(name, featureNames)

    val metadataPerFeature = featureNames.map { typeName =>
      typeName -> getFeatureMetadata(dataStore, typeName, dataStore.catalogTable)
    }.toMap[String, List[TableMetadata]]
    metadata.put(name, metadataPerFeature)

    val boundsPerFeature = featureNames.map { typeName =>
      val bounds = dataStore.getBounds(new Query(typeName))
      typeName -> s"${bounds.getMinX}:${bounds.getMaxX}, ${bounds.getMinY}:${bounds.getMaxY}"
    }.toMap[String, String]
    bounds.put(name, boundsPerFeature)

    connectionParams.put(name, params)
  }

  add(new ListView[String]("storeDetails", dataStoreNames.toList.asJava) {
    override def populateItem(item: ListItem[String]) = {
      val dataStore = item.getModelObject
      val featureData = features.get(dataStore).get.map { feature =>
        // data store name is workspace:name
        val name = dataStore.split(":")
        FeatureData(name(0),
                    name(1),
                    feature,
                    metadata.get(dataStore).get(feature),
                    bounds.get(dataStore).get(feature))
      }
      item.add(new DataStoreInfoPanel("storeMetadata", dataStore, featureData))
    }
  })

  /**
   * Injects the data needed by d3 for charts
   *
   * @param container
   */
  override def renderHead(container: HtmlHeaderContainer): Unit = {
    super.renderHead(container)

    // filter out the main record tables to display in the chart
    val values =
      for {
        map <- metadata.values
        (_, metadataList) <- map
        metadata <- metadataList
        if (metadata.tableName.contains("Record"))
      } yield {
        s"{name: '${metadata.featureName}', value: ${metadata.numEntries}}"
      }

    // sort reverse so that they show up alphabetically on the UI
    val data = values.toList.sorted(Ordering[String].reverse).mkString(",")

    val string = s"var data = [ $data ]"
    container.getHeaderResponse.renderJavascript(string, "rawData")
  }

  /**
   * Gets the connection info for each unique store registered in GeoServer
   *
   * @return
   */
  private def getStoreConnections : List[AccumuloConnectionInfo] = {
    val stores = storeProvider.iterator(0, storeProvider.fullSize).asScala

    stores.map { store =>
      val name = s"${store.getWorkspace.getName}:${store.getName}"
      val params = store.getConnectionParameters.asScala

      val instanceId = params.getOrElse(instanceIdParam.key, "").toString
      val zookeepers = params.getOrElse(zookeepersParam.key, "").toString
      val user = params.getOrElse(userParam.key, "").toString
      val password = params.getOrElse(passwordParam.key, "").toString
      val table = params.getOrElse(tableNameParam.key, "").toString

      AccumuloConnectionInfo(name, zookeepers, instanceId, user, password, table)
    }.toList
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
   * @param dataStore
   * @param featureName
   * @param table
   * @return
   *   (tableName, number of tablets, number of splits, total number of entries, total file size)
   */
  def getFeatureMetadata(dataStore: AccumuloDataStore, featureName: String, table: String): List[TableMetadata] = {
    val connector = dataStore.connector

    val tables =
      if (dataStore.catalogTableFormat(featureName)) {
        List(("Record Table", dataStore.getRecordTableForType(featureName)),
             ("GeoSpatial Index", dataStore.getSpatioTemporalIdxTableName(featureName)),
             ("Attribute Index", dataStore.getAttrIdxTableName(featureName)))
      } else {
        List(("Record Table/GeoSpatial Index", table))
      }

    tables.map { case (displayName, table) =>
      val tableId = Option(connector.tableOperations.tableIdMap.get(table))
      tableId match {
        case Some(id) => getTableMetadata(connector, featureName, displayName, id)
        case None => TableMetadata(featureName, displayName, 0, 0, 0, 0)
      }
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
   * @param featureName
   * @param tableName
   * @param tableId
   * @return (tableName, number of tablets, number of splits, total number of entries, total file size)
   */
  def getTableMetadata(connector: Connector, featureName: String, tableName: String, tableId: String): TableMetadata = {
    // TODO move this to core utility class where it can be re-used

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

    TableMetadata(featureName, tableName, numTablets, numSplits, numEntries, fileSize / 1048576.0)
  }
}

case class AccumuloConnectionInfo(name: String,
                                  zookeepers: String,
                                  instanceId: String,
                                  user: String,
                                  password: String,
                                  table: String)

case class TableMetadata(featureName: String,
                         tableName: String,
                         numTablets: Long,
                         numSplits: Long,
                         numEntries: Long,
                         fileSize: Double)

case class FeatureData(workspace: String,
                       dataStore: String,
                       featureName: String,
                       metadata: List[TableMetadata],
                       bounds: String)
