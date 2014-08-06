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

class FeaturesTool(catalogTable: String) extends Logging {
  val user = sys.env.getOrElse("GEOMESA_USER", "admin")
  val password = sys.env.getOrElse("GEOMESA_PASSWORD", "admin")
  val instanceId = sys.env.getOrElse("GEOMESA_INSTANCEID", "instanceId")
  val zookeepers = sys.env.getOrElse("GEOMESA_ZOOKEEPERS", "zoo1:2181,zoo2:2181,zoo3:2181")
  val table = catalogTable

  val ds: AccumuloDataStore = {
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> instanceId,
      "zookeepers" -> zookeepers,
      "user"       -> user,
      "password"   -> password,
      "tableName"  -> table)).asInstanceOf[AccumuloDataStore]
  }

  def listFeatures() {
    ds.getTypeNames.foreach(name =>
      logger.info(s"$name: ${ds.getSchema(name).getAttributeDescriptors}")
    )
  }

  def createFeatureType(sftName: String, sftString: String): Boolean = {
    val sft = SimpleFeatureTypes.createType(sftName, sftString)
    ds.createSchema(sft)
    ds.getSchema(sftName) != null
  }

  def exportFeatures(feature: String,
                     attributes: String,
                     idAttribute: String,
                     latAttribute: Option[String],
                     lonAttribute: Option[String],
                     dateAttribute: Option[String],
                     format: String,
                     query: String,
                     maxFeatures: Int) {
    val sftCollection = getFeatureCollection(feature, query, attributes, maxFeatures)
    val folderPath = Paths.get(s"${System.getProperty("user.dir")}/export")
    if (Files.notExists(folderPath)) {
      Files.createDirectory(folderPath)
    }
    format.toLowerCase match {
      case "csv" | "tsv" =>
        val loadAttributes = new LoadAttributes(feature, table, attributes, idAttribute, latAttribute, lonAttribute, dateAttribute, query)
        val de = new DataExporter(loadAttributes, Map(
          "instanceId" -> instanceId,
          "zookeepers" -> zookeepers,
          "user"       -> user,
          "password"   -> password,
          "tableName"  -> table), format)
        de.writeFeatures(sftCollection.features())
      case "shp" =>
        val shapeFileExporter = new ShapefileExport
        shapeFileExporter.write(s"$folderPath/$feature.shp", feature, sftCollection, ds.getSchema(feature))
        logger.info(s"Successfully wrote features to '${System.getProperty("user.dir")}/export/$feature.shp'")
      case "geojson" =>
        val os = new FileOutputStream(s"$folderPath/$feature.geojson")
        val geojsonExporter = new GeoJsonExport
        geojsonExporter.write(sftCollection, os)
        logger.info(s"Successfully wrote features to '$folderPath/$feature.geojson'")
      case "gml" =>
        val os = new FileOutputStream(s"$folderPath/$feature.gml")
        val gmlExporter = new GmlExport
        gmlExporter.write(sftCollection, os)
        logger.info(s"Successfully wrote features to '$folderPath/$feature.gml'")
      case _ =>
        logger.error("Unsupported export format. Supported formats are shp, geojson, csv, and gml.")
    }
  }

  def deleteFeature(sftName: String): Boolean = {
    ds.deleteSchema(sftName)
    !ds.getNames.contains(sftName)
  }

  def explainQuery(featureName: String, filterString: String) = {
    val q = new Query()
    val t = Transaction.AUTO_COMMIT
    q.setTypeName(featureName)

    val f = ECQL.toFilter(filterString)
    q.setFilter(f)

    val afr = ds.getFeatureReader(q, t).asInstanceOf[AccumuloFeatureReader]

    afr.explainQuery(q)
  }

  def getFeatureCollection(feature: String, query: String, attributes: String, maxFeatures: Int): SimpleFeatureCollection = {
    val filter = if (query != null) CQL.toFilter(query) else CQL.toFilter("include")
    val q = new Query(feature, filter)

    if (maxFeatures > 0) q.setMaxFeatures(maxFeatures)
    if (attributes != null) q.setPropertyNames(attributes.split(','))
    logger.info(s"$q")

    // get the feature store used to query the GeoMesa data
    val featureStore = ds.getFeatureSource(feature).asInstanceOf[AccumuloFeatureStore]
    // execute the query
    featureStore.getFeatures(q)
  }
}