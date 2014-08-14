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

package org.locationtech.geomesa.tools

import java.io.FileOutputStream
import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.hadoop.fs.Path
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core.data.{AccumuloDataStore, AccumuloFeatureReader, AccumuloFeatureStore}
import org.locationtech.geomesa.core.index.SF_PROPERTY_START_TIME
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import scala.collection.JavaConversions._
import scala.util.Try
import scala.xml.XML

class FeaturesTool(config: ScoptArguments, password: String) extends Logging {
  val accumuloConfPath = s"${System.getenv("ACCUMULO_HOME")}/conf/accumulo-site.xml"
  val accumuloConf = XML.loadFile(s"${System.getenv("ACCUMULO_HOME")}/conf/accumulo-site.xml")
  val zookeepers = (accumuloConf \\ "property")
    .filter(x => (x \ "name")
    .text == "instance.zookeeper.host")
    .map(y => (y \ "value").text)
    .head
  val instanceDfsDir = Try((accumuloConf \\ "property")
      .filter(x => (x \ "name")
      .text == "instance.dfs.dir")
      .map(y => (y \ "value").text)
      .head)
    .getOrElse("/accumulo")
  val instanceIdDir = new Path(instanceDfsDir, "instance_id")
  val instanceName = new ZooKeeperInstance(UUID.fromString(ZooKeeperInstance.getInstanceIDFromHdfs(instanceIdDir)), zookeepers).getInstanceName
  val ds: AccumuloDataStore = Try({
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> instanceName,
      "zookeepers" -> zookeepers,
      "user"       -> config.username,
      "password"   -> password,
      "tableName"  -> config.catalog)).asInstanceOf[AccumuloDataStore]
  }).getOrElse{
    logger.error("Incorrect username or password. Please try again.")
    sys.exit()
  }

  def listFeatures() {
    val featureCount = if (ds.getTypeNames.size == 1) {
      s"1 feature exists on '${config.catalog}'. It is: "
    } else if (ds.getTypeNames.size == 0) {
      s"0 features exist on '${config.catalog}'. This catalog table might not yet exist."
    } else {
      s"${ds.getTypeNames.size} features exist on '${config.catalog}'. They are: "
    }
    logger.info(s"$featureCount")
    ds.getTypeNames.foreach(name =>
      logger.info(s" - $name")
    )
  }

  def describeFeature() {
    try {
      ds.getSchema(config.featureName).getAttributeDescriptors.foreach(attr => {
        val isIndexed = attr.getUserData.getOrElse("index", false).asInstanceOf[java.lang.Boolean]
        val defaultValue = attr.getDefaultValue
        val attrType = attr.getType.getBinding.getSimpleName
        var attrString = s" - ${attr.getLocalName}: $attrType "
        if (isIndexed) {
          if (attrType == "Date") {
            attrString = attrString.concat("(Time-index) ")
          } else if (attrType == "Geometry") {
            attrString = attrString.concat("(Geo-index) ")
          } else {
            attrString = attrString.concat("(Indexed) ")
          }
        }
        if (defaultValue != null) {
          attrString = attrString.concat(s"- Default Value: $defaultValue")
        }
        logger.info(s"$attrString")
      })
    } catch {
      case npe: NullPointerException => logger.error("Error: feature not found. Please ensure " +
        "that all arguments are correct in the previous command.")
      case e: Exception => logger.error("Error describing feature")
    }
  }

  def createFeatureType(sftName: String, sftString: String, defaultDate: String = null): Boolean = {
    val sft = SimpleFeatureTypes.createType(sftName, sftString)
    if (defaultDate != null) { sft.getUserData.put(SF_PROPERTY_START_TIME, defaultDate) }
    ds.createSchema(sft)
    ds.getSchema(sftName) != null
  }

  def exportFeatures() {
    val sftCollection = getFeatureCollection
    val outputPath = s"${System.getProperty("user.dir")}/${config.catalog}_${config.featureName}.${config.format}"
    config.format.toLowerCase match {
      case "csv" | "tsv" =>
        val loadAttributes = new LoadAttributes(config.featureName,
                                                config.catalog,
                                                config.attributes,
                                                null,
                                                config.latAttribute,
                                                config.lonAttribute,
                                                config.dtField,
                                                config.query)
        val de = new DataExporter(loadAttributes, Map(
          "instanceId" -> instanceName,
          "zookeepers" -> zookeepers,
          "user"       -> config.username,
          "password"   -> password,
          "tableName"  -> config.catalog), config.format)
        de.writeFeatures(sftCollection.features())
      case "shp" =>
        val shapeFileExporter = new ShapefileExport
        shapeFileExporter.write(outputPath, config.featureName, sftCollection, ds.getSchema(config.featureName))
        logger.info(s"Successfully wrote features to '$outputPath'")
      case "geojson" =>
        val os = new FileOutputStream(s"$outputPath")
        val geojsonExporter = new GeoJsonExport
        geojsonExporter.write(sftCollection, os)
        logger.info(s"Successfully wrote features to '$outputPath'")
      case "gml" =>
        val os = new FileOutputStream(s"$outputPath")
        val gmlExporter = new GmlExport
        gmlExporter.write(sftCollection, os)
        logger.info(s"Successfully wrote features to '$outputPath'")
      case _ =>
        logger.error("Unsupported export format. Supported formats are shp, geojson, csv, and gml.")
    }
    //necessary to avoid StatsWriter exception when exporting
    Thread.sleep(1000)
  }

  def deleteFeature(): Boolean = {
    try {
      ds.deleteSchema(config.featureName)
      !ds.getNames.contains(config.featureName)
    } catch {
      case re: RuntimeException => false
      case e: Exception => false
    }
  }

  def explainQuery() = {
    val q = new Query()
    val t = Transaction.AUTO_COMMIT
    q.setTypeName(config.featureName)

    val f = ECQL.toFilter(config.filterString)
    q.setFilter(f)

    try {
      val afr = ds.getFeatureReader(q, t).asInstanceOf[AccumuloFeatureReader]

      afr.explainQuery(q)
    } catch {
      case re: RuntimeException => logger.error(s"Error: Could not explain the query. Please " +
        s"ensure that all arguments are correct in the previous command.")
      case e: Exception => logger.error(s"Error: Could not explain the query.")
    }
  }

  def getFeatureCollection: SimpleFeatureCollection = {
    val filter = if (config.query != null) { CQL.toFilter(config.query) } else { CQL.toFilter("include") }
    val q = new Query(config.featureName, filter)

    if (config.maxFeatures > 0) { q.setMaxFeatures(config.maxFeatures) }
    if (config.attributes != null) { q.setPropertyNames(config.attributes.split(',')) }
    logger.info(s"$q")

    // get the feature store used to query the GeoMesa data
    val fs = ds.getFeatureSource(config.featureName).asInstanceOf[AccumuloFeatureStore]

    // and execute the query
    Try(fs.getFeatures(q)).getOrElse{
      logger.error("Error: Could not create a SimpleFeatureCollection to export. Please ensure " +
        "that all arguments are correct in the previous command.")
      sys.exit()
    }
  }
}