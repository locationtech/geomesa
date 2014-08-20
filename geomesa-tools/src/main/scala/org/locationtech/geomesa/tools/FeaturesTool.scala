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

import java.io.{File, FileOutputStream}

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.DateTime
import org.locationtech.geomesa.core.data.{AccumuloDataStore, AccumuloFeatureReader, AccumuloFeatureStore}
import org.locationtech.geomesa.core.index.SF_PROPERTY_START_TIME
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import scala.collection.JavaConversions._
import scala.util.Try

class FeaturesTool(config: ScoptArguments, password: String) extends Logging with AccumuloProperties {

  val ds: AccumuloDataStore = Try({
    DataStoreFinder.getDataStore(Map(
      "instanceId"   -> instanceName,
      "zookeepers"   -> zookeepers,
      "user"         -> config.username,
      "password"     -> password,
      "tableName"    -> config.catalog,
      "collectStats" -> "false")).asInstanceOf[AccumuloDataStore]
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
    if (!config.toStdOut) { logger.info(s"$featureCount") }
    ds.getTypeNames.foreach(name =>
      logger.info(s"$name")
    )
  }

  def describeFeature() {
    try {
      val sft = ds.getSchema(config.featureName)
      sft.getAttributeDescriptors.foreach(attr => {
        val defaultValue = attr.getDefaultValue
        val attrType = attr.getType.getBinding.getSimpleName
        var attrString = s"${attr.getLocalName}"
        if (!config.toStdOut) {
          attrString = attrString.concat(s": $attrType ")
          if (sft.getUserData.getOrElse(SF_PROPERTY_START_TIME, "") == attr.getLocalName) {
            attrString = attrString.concat("(Time-index) ")
          } else if (sft.getGeometryDescriptor == attr) {
            attrString = attrString.concat("(Geo-index) ")
          } else if (attr.getUserData.getOrElse("index", false).asInstanceOf[java.lang.Boolean]) {
            attrString = attrString.concat("(Indexed) ")
          }
          if (defaultValue != null) {
            attrString = attrString.concat(s"- Default Value: $defaultValue")
          }
        }
        logger.info(s"$attrString")
      })
    } catch {
      case npe: NullPointerException => logger.error("Error: feature not found. Please ensure " +
        "that all arguments are correct in the previous command.")
      case e: Exception => logger.error("Error describing feature")
    }
  }

  def createFeatureType(): Boolean = {
    if (ds.getSchema(config.featureName) == null) {
      val sft = SimpleFeatureTypes.createType(config.featureName, config.spec)
      if (config.dtField.orNull != null) {
        sft.getUserData.put(SF_PROPERTY_START_TIME, config.dtField.get)
      }
      ds.createSchema(sft)
      ds.getSchema(config.featureName) != null
    } else {
      logger.error(s"A feature named '${config.featureName}' already exists in the data store with catalog table '${config.catalog}'.")
      sys.exit()
    }
  }

  def exportFeatures() {
    val sftCollection = getFeatureCollection
    var outputPath: File = null
    do {
      if (outputPath != null) { Thread.sleep(1) }
      outputPath = new File(s"${System.getProperty("user.dir")}/${config.catalog}_${config.featureName}_${DateTime.now()}.${config.format}")
    } while (outputPath.exists)
    config.format.toLowerCase match {
      case "csv" | "tsv" =>
        val loadAttributes = new LoadAttributes(config.featureName,
                                                config.catalog,
                                                config.attributes,
                                                config.idFields.orNull,
                                                config.latAttribute,
                                                config.lonAttribute,
                                                config.dtField,
                                                config.query,
                                                config.format,
                                                config.toStdOut,
                                                outputPath)
        val de = new DataExporter(loadAttributes, Map(
          "instanceId"   -> instanceName,
          "zookeepers"   -> zookeepers,
          "user"         -> config.username,
          "password"     -> password,
          "tableName"    -> config.catalog,
          "collectStats" -> "false"))
        de.writeFeatures(sftCollection.features())
      case "shp" =>
        val shapeFileExporter = new ShapefileExport
        shapeFileExporter.write(outputPath, config.featureName, sftCollection, ds.getSchema(config.featureName))
        logger.info(s"Successfully wrote features to '${outputPath.toString}'")
      case "geojson" =>
        val os = if (config.toStdOut) { System.out } else { new FileOutputStream(outputPath) }
        val geojsonExporter = new GeoJsonExport
        geojsonExporter.write(sftCollection, os)
        if (!config.toStdOut) { logger.info(s"Successfully wrote features to '${outputPath.toString}'") }
      case "gml" =>
        val os = if (config.toStdOut) { System.out } else { new FileOutputStream(outputPath) }
        val gmlExporter = new GmlExport
        gmlExporter.write(sftCollection, os)
        if (!config.toStdOut) { logger.info(s"Successfully wrote features to '${outputPath.toString}'") }
      case _ =>
        logger.error("Unsupported export format. Supported formats are shp, geojson, csv, and gml.")
    }
  }

  def deleteFeature(): Boolean = {
    try {
      ds.removeSchema(config.featureName)
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
    if (!config.toStdOut) { logger.info(s"$q") }

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