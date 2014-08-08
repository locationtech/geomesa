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
import geomesa.core.data.{AccumuloDataStore, AccumuloFeatureReader, AccumuloFeatureStore}
import geomesa.utils.geotools.SimpleFeatureTypes
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL

import scala.collection.JavaConversions._

class FeaturesTool(config: ScoptArguments) extends Logging {
  val user = config.username
  val pass = config.password
  val instanceId = sys.env.getOrElse("GEOMESA_INSTANCEID", "instanceId")
  val zookeepers = sys.env.getOrElse("GEOMESA_ZOOKEEPERS", "zoo1:2181,zoo2:2181,zoo3:2181")
  val table = config.catalog

  val ds: AccumuloDataStore = {
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> instanceId,
      "zookeepers" -> zookeepers,
      "user"       -> user,
      "password"   -> pass,
      "tableName"  -> table)).asInstanceOf[AccumuloDataStore]
  }

  def listFeatures() {
    ds.getTypeNames.foreach(name => {
      logger.info(s"Feature $name: ")
      ds.getSchema(name).getAttributeDescriptors.foreach( attr => {
        val indexed = attr.getUserData.getOrElse("index", false).asInstanceOf[java.lang.Boolean]
        val attrType = attr.getType.toString.replace(attr.getLocalName, "")
        val attrString = if (indexed) { s"         |_ ${attr.getLocalName}: $attrType (Indexed)" } else { s"         |_ ${attr.getLocalName}: $attrType" }
        logger.info(s"$attrString")
      })
    })
  }

  def createFeatureType(sftName: String, sftString: String): Boolean = {
    val sft = SimpleFeatureTypes.createType(sftName, sftString)
    ds.createSchema(sft)
    ds.getSchema(sftName) != null
  }

  def exportFeatures() {
    val sftCollection = getFeatureCollection
    val folderPath = Paths.get(s"${System.getProperty("user.dir")}/export")
    if (Files.notExists(folderPath)) {
      Files.createDirectory(folderPath)
    }
    config.format.toLowerCase match {
      case "csv" | "tsv" =>
        val loadAttributes = new LoadAttributes(config.typeName, table, config.attributes, null, config.latAttribute, config.lonAttribute, config.dateAttribute, config.query)
        val de = new DataExporter(loadAttributes, Map(
          "instanceId" -> instanceId,
          "zookeepers" -> zookeepers,
          "user"       -> user,
          "password"   -> pass,
          "tableName"  -> table), config.format)
        de.writeFeatures(sftCollection.features())
      case "shp" =>
        val shapeFileExporter = new ShapefileExport
        shapeFileExporter.write(s"$folderPath/${config.typeName}.shp", config.typeName, sftCollection, ds.getSchema(config.typeName))
        logger.info(s"Successfully wrote features to '${System.getProperty("user.dir")}/export/${config.typeName}.shp'")
      case "geojson" =>
        val os = new FileOutputStream(s"$folderPath/${config.typeName}.geojson")
        val geojsonExporter = new GeoJsonExport
        geojsonExporter.write(sftCollection, os)
        logger.info(s"Successfully wrote features to '$folderPath/${config.typeName}.geojson'")
      case "gml" =>
        val os = new FileOutputStream(s"$folderPath/${config.typeName}.gml")
        val gmlExporter = new GmlExport
        gmlExporter.write(sftCollection, os)
        logger.info(s"Successfully wrote features to '$folderPath/${config.typeName}.gml'")
      case _ =>
        logger.error("Unsupported export format. Supported formats are shp, geojson, csv, and gml.")
    }
  }

  def deleteFeature(): Boolean = {
    ds.deleteSchema(config.typeName)
    !ds.getNames.contains(config.typeName)
  }

  def explainQuery() = {
    val q = new Query()
    val t = Transaction.AUTO_COMMIT
    q.setTypeName(config.typeName)

    val f = ECQL.toFilter(config.filterString)
    q.setFilter(f)

    val afr = ds.getFeatureReader(q, t).asInstanceOf[AccumuloFeatureReader]

    afr.explainQuery(q)
  }

  def getFeatureCollection: SimpleFeatureCollection = {
    val filter = if (config.query != null) { CQL.toFilter(config.query) } else { CQL.toFilter("include") }
    val q = new Query(config.typeName, filter)

    if (config.maxFeatures > 0) { q.setMaxFeatures(config.maxFeatures) }
    if (config.attributes != null) { q.setPropertyNames(config.attributes.split(',')) }
    logger.info(s"$q")

    // get the feature store used to query the GeoMesa data
    val featureStore = ds.getFeatureSource(config.typeName).asInstanceOf[AccumuloFeatureStore]
    // execute the query
    featureStore.getFeatures(q)
  }
}