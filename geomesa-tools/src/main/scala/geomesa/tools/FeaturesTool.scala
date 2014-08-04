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

import geomesa.core.data.{AccumuloDataStore, AccumuloFeatureReader, AccumuloFeatureStore}
import geomesa.core.integration.data.{DataExporter, LoadAttributes}
import geomesa.utils.geotools.SimpleFeatureTypes
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL

import scala.collection.JavaConversions._

class FeaturesTool(catalogTable: String) {
  val user = sys.env.getOrElse("GEOMESA_USER", "admin")
  val password = sys.env.getOrElse("GEOMESA_PASSWORD", "admin")
  val instanceId = sys.env.getOrElse("GEOMESA_INSTANCEID", "instanceId")
  val zookeepers = sys.env.getOrElse("GEOMESA_ZOOKEEPERS", "zoo1:2181,zoo2:2181,zoo3:2181")
  val table = catalogTable

  val ds: AccumuloDataStore = {
    DataStoreFinder.getDataStore(Map(
      "instanceId"      -> instanceId,
      "zookeepers"      -> zookeepers,
      "user"            -> user,
      "password"        -> password,
      "tableName"       -> table,
      "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
  }

  def listFeatures() {
    ds.getTypeNames foreach {name =>
      println(s"$name: ${ds.getSchema(name).getAttributeDescriptors}")
    }
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
                     query: String) {
    format.toLowerCase match {
      case "csv" =>
        val loadAttributes = new LoadAttributes(feature, table, attributes, idAttribute, latAttribute, lonAttribute, dateAttribute, query)
        val de = new DataExporter(loadAttributes, Map(
          "instanceId"      -> instanceId,
          "zookeepers"      -> zookeepers,
          "user"            -> user,
          "password"        -> password,
          "tableName"       -> table,
          "featureEncoding" -> "avro"), format)
        de.writeFeatures(de.queryFeatures())
      case "shp" =>
        val sftCollection = getFeatureCollection(feature, query)
        val shapeFileExporter = new ShapefileExport
        shapeFileExporter.write("/tmp/export.shp", feature, sftCollection, ds.getSchema(feature))
      case "geojson" =>
        val sftCollection = getFeatureCollection(feature, query)
        val os = new FileOutputStream("/tmp/export.geojson")
        val geojsonExporter = new GeoJsonExport
        geojsonExporter.write(sftCollection, os)
      case "gml" =>
        val sftCollection = getFeatureCollection(feature, query)
        val os = new FileOutputStream("/tmp/export.gml")
        val gmlExporter = new GmlExport
        gmlExporter.write(sftCollection, os)
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

  def getFeatureCollection(feature: String, query: String): SimpleFeatureCollection = {
    val q = new Query(feature, CQL.toFilter(query))
    // get the feature store used to query the GeoMesa data
    val featureStore = ds.getFeatureSource(feature).asInstanceOf[AccumuloFeatureStore]
    // execute the query
    featureStore.getFeatures(q)
  }
}