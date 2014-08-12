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

package geomesa.tools

import java.io.FileOutputStream
import java.nio.file.{Files, Paths}

import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core
import geomesa.core.data.{AccumuloDataStore, AccumuloFeatureReader, AccumuloFeatureStore}
import geomesa.utils.geotools.SimpleFeatureTypes
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL

import scala.collection.JavaConversions._
import scala.util.Try
import scala.xml.XML

class FeaturesTool(config: ScoptArguments, password: String) extends Logging {
  val user = config.username
  val pass = password
  val table = config.catalog
  val accumuloConf = XML.loadFile(s"${System.getenv("ACCUMULO_HOME")}/conf/accumulo-site.xml")
  val zookeepers = (accumuloConf \\ "property").filter(x => (x \ "name").text == "instance.zookeeper.host").map(y => (y \ "value").text).head
  val instanceId = (accumuloConf \\ "property").filter(x => (x \ "name").text == "instance.instanceId").map(y => (y \ "value").text).head

  val ds: AccumuloDataStore = {
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> instanceId,
      "zookeepers" -> zookeepers,
      "user"       -> user,
      "password"   -> pass,
      "tableName"  -> table)).asInstanceOf[AccumuloDataStore]
  }

  def listFeatures() {
    val featureCount = if (ds.getTypeNames.size == 1) {
      s"1 feature exists on ${config.catalog}. It is: "
    } else if (ds.getTypeNames.size == 0) {
      s"0 features exist on ${config.catalog}. This catalog table might not yet exist."
    } else {
      s"${ds.getTypeNames.size} features exist on ${config.catalog}. They are: "
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
        val typeString = attr.getType.toString
        val attrType = typeString.substring(typeString.indexOf("<"), typeString.length)
        var attrString = s" - ${attr.getLocalName}: $attrType "
        if (isIndexed) {
          if (attrType == "<Date>") {
            attrString = attrString.concat("(Time-index) ")
          } else if (attrType == "<Geometry>") {
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
    if (defaultDate != null) { sft.getUserData.put(core.index.SF_PROPERTY_START_TIME, defaultDate) }
    ds.createSchema(sft)
    ds.getSchema(sftName) != null
  }

  def exportFeatures() {
    val sftCollection = getFeatureCollection getOrElse{
      logger.error("Error: Could not create a SimpleFeatureCollection to export. Please ensure " +
        "that all arguments are correct in the previous command.")
      sys.exit()
    }
    val folderPath = Paths.get(s"${System.getProperty("user.dir")}/export")
    if (Files.notExists(folderPath)) {
      Files.createDirectory(folderPath)
    }
    config.format.toLowerCase match {
      case "csv" | "tsv" =>
        val loadAttributes = new LoadAttributes(config.featureName, table, config.attributes, null, config.latAttribute, config.lonAttribute, config.dateAttribute, config.query)
        val de = new DataExporter(loadAttributes, Map(
          "instanceId" -> instanceId,
          "zookeepers" -> zookeepers,
          "user"       -> user,
          "password"   -> pass,
          "tableName"  -> table), config.format)
        de.writeFeatures(sftCollection.features())
      case "shp" =>
        val shapeFileExporter = new ShapefileExport
        shapeFileExporter.write(s"$folderPath/${config.featureName}.shp", config.featureName, sftCollection, ds.getSchema(config.featureName))
        logger.info(s"Successfully wrote features to '${System.getProperty("user.dir")}/export/${config.featureName}.shp'")
      case "geojson" =>
        val os = new FileOutputStream(s"$folderPath/${config.featureName}.geojson")
        val geojsonExporter = new GeoJsonExport
        geojsonExporter.write(sftCollection, os)
        logger.info(s"Successfully wrote features to '$folderPath/${config.featureName}.geojson'")
      case "gml" =>
        val os = new FileOutputStream(s"$folderPath/${config.featureName}.gml")
        val gmlExporter = new GmlExport
        gmlExporter.write(sftCollection, os)
        logger.info(s"Successfully wrote features to '$folderPath/${config.featureName}.gml'")
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
      case re: RuntimeException =>
        false
      case e: Exception =>
        false
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

  def getFeatureCollection: Try[SimpleFeatureCollection] = {
    val filter = if (config.query != null) { CQL.toFilter(config.query) } else { CQL.toFilter("include") }
    val q = new Query(config.featureName, filter)

    if (config.maxFeatures > 0) { q.setMaxFeatures(config.maxFeatures) }
    if (config.attributes != null) { q.setPropertyNames(config.attributes.split(',')) }
    logger.info(s"$q")

    // get the feature store used to query the GeoMesa data and execute the query
    Try(ds.getFeatureSource(config.featureName).asInstanceOf[AccumuloFeatureStore].getFeatures(q))
  }
}