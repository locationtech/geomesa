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
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.{DataStoreFinder, _}
import org.geotools.filter.text.cql2.CQL
import org.joda.time.DateTime
import org.locationtech.geomesa.core.data.{AccumuloDataStore, AccumuloFeatureStore}

import scala.collection.JavaConversions._
import scala.util.Try

class Export(config: ExportArguments, password: String) extends Logging with AccumuloProperties {

  val instance = config.instanceName.getOrElse(instanceName)
  val zookeepersString = config.zookeepers.getOrElse(zookeepers)

  val ds: AccumuloDataStore = Try({
    DataStoreFinder.getDataStore(Map(
      "instanceId"   -> instance,
      "zookeepers"   -> zookeepersString,
      "user"         -> config.username,
      "password"     -> password,
      "tableName"    -> config.catalog,
      "visibilities" -> config.visibilities.orNull,
      "auths"        -> config.auths.orNull)).asInstanceOf[AccumuloDataStore]
  }).getOrElse{
    logger.error("Cannot connect to Accumulo. Please check your configuration and try again.")
    sys.exit()
  }

  def exportFeatures() {
    var outputPath: File = null
    do {
      if (outputPath != null) { Thread.sleep(1) }
      outputPath = new File(s"${System.getProperty("user.dir")}/${config.catalog}_${config.featureName}_${DateTime.now()}.${config.format}")
    } while (outputPath.exists)
    config.format.toLowerCase match {
      case "csv" | "tsv" =>
        val sftCollection = getFeatureCollection()
        val loadAttributes = new LoadAttributes(config.featureName,
          config.catalog,
          config.attributes.orNull,
          config.idFields.orNull,
          config.latAttribute,
          config.lonAttribute,
          config.dtField,
          config.query.orNull,
          config.format,
          config.toStdOut,
          outputPath)
        val de = new SVExport(loadAttributes, Map(
          "instanceId"   -> instance,
          "zookeepers"   -> zookeepersString,
          "user"         -> config.username,
          "password"     -> password,
          "tableName"    -> config.catalog,
          "visibilities" -> config.visibilities.orNull,
          "auths"        -> config.auths.orNull))
        de.writeFeatures(sftCollection.features())
      case "shp" =>
        // When exporting to Shapefile, we must rename the Geometry Attribute Descriptor to "the_geom", per
        // the requirements of Geotools' ShapefileDataStore and ShapefileFeatureWriter. The easiest way to do this
        // is transform the attribute when retrieving the SimpleFeatureCollection.
        val attrDescriptors = config.attributes.getOrElse(
          ds.getSchema(config.featureName).getAttributeDescriptors.map(_.getLocalName).mkString(","))
        val geomDescriptor = ds.getSchema(config.featureName).getGeometryDescriptor.getLocalName
        val renamedGeomAttrs = if (attrDescriptors.contains(geomDescriptor)) {
          attrDescriptors.replace(geomDescriptor, s"the_geom=$geomDescriptor")
        } else {
          attrDescriptors.concat(s",the_geom=$geomDescriptor")
        }
        val shpCollection = getFeatureCollection(Some(renamedGeomAttrs))
        val shapeFileExporter = new ShapefileExport
        shapeFileExporter.write(outputPath, config.featureName, shpCollection, shpCollection.getSchema)
        logger.info(s"Successfully wrote features to '${outputPath.toString}'")
      case "geojson" =>
        val sftCollection = getFeatureCollection()
        val os = if (config.toStdOut) { System.out } else { new FileOutputStream(outputPath) }
        val geojsonExporter = new GeoJsonExport
        geojsonExporter.write(sftCollection, os)
        if (!config.toStdOut) { logger.info(s"Successfully wrote features to '${outputPath.toString}'") }
      case "gml" =>
        val sftCollection = getFeatureCollection()
        val os = if (config.toStdOut) { System.out } else { new FileOutputStream(outputPath) }
        val gmlExporter = new GmlExport
        gmlExporter.write(sftCollection, os)
        if (!config.toStdOut) { logger.info(s"Successfully wrote features to '${outputPath.toString}'") }
      case _ =>
        logger.error("Unsupported export format. Supported formats are shp, geojson, csv, and gml.")
    }
    Thread.sleep(1000)
  }

  def getFeatureCollection(overrideAttributes: Option[String] = None): SimpleFeatureCollection = {
    val filter = CQL.toFilter(config.query.getOrElse("include"))
    val q = new Query(config.featureName, filter)

    q.setMaxFeatures(config.maxFeatures.getOrElse(Query.DEFAULT_MAX))
    val attributesO = if (overrideAttributes.isDefined) overrideAttributes
                      else if (config.attributes.isDefined) config.attributes
                      else None
    //Split attributes by "," meanwhile allowing to escape it by "\,".
    attributesO.foreach { attributes =>
      q.setPropertyNames(attributes.split("""(?<!\\),""").map(_.trim.replace("\\,", ",")))
    }

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

object Export extends App with Logging with GetPassword {
  val parser = new scopt.OptionParser[ExportArguments]("geomesa-tools export") {
    implicit val optionStringRead: scopt.Read[Option[String]] = scopt.Read.reads(Option[String])
    implicit val optionIntRead: scopt.Read[Option[Int]] = scopt.Read.reads(i => Option(i.toInt))
    def catalogOpt = opt[String]('c', "catalog").action { (s, c) =>
      c.copy(catalog = s) } required() hidden() text "the name of the Accumulo table to use"
    def featureOpt = opt[String]('f', "feature-name").action { (s, c) =>
      c.copy(featureName = s) } required() text "the name of the feature to export"
    def userOpt = opt[String]('u', "username") action { (x, c) =>
      c.copy(username = x) } text "the Accumulo username" required()
    def passOpt = opt[Option[String]]('p', "password") action { (x, c) =>
      c.copy(password = x) } text "the Accumulo password. This can also be provided after entering a command." optional()
    def instanceNameOpt = opt[Option[String]]('i', "instance-name") action { (x, c) =>
      c.copy(instanceName = x) } text "Accumulo instance name" optional()
    def zookeepersOpt = opt[Option[String]]('z', "zookeepers") action { (x, c) =>
      c.copy(zookeepers = x) } text "Zookeepers comma-separated instances string" optional()
    def visibilitiesOpt = opt[Option[String]]('v', "visibilities") action { (x, c) =>
      c.copy(visibilities = x) } text "Accumulo visibilities string" optional()
    def authsOpt = opt[Option[String]]('a', "auths") action { (x, c) =>
      c.copy(auths = x) } text "Accumulo authorizations string" optional()

    head("GeoMesa Tools", "1.0")
    help("help").text("show help command")
    userOpt
    passOpt
    catalogOpt
    featureOpt
    opt[Unit]('s', "stdOut").action { (_, c) =>
      c.copy(toStdOut = true) } optional() text "Add this flag to export to stdout"
    opt[String]('o', "format").action { (s, c) =>
      c.copy(format = s) } required() text "The format to export to (CSV, TSV, GML, GeoJSON, SHP)"
    opt[Option[String]]('a', "attributes").action { (s, c) =>
      c.copy(attributes = s) } optional() text "Names of the attributes to return in the export. default: ALL"
    opt[String]("idAttribute").action { (s, c) =>
      c.copy(idFields = Option(s)) } optional() hidden()
    opt[String]("latAttribute").action { (s, c) =>
      c.copy(latAttribute = Option(s)) } optional() hidden()
    opt[String]("lonAttribute").action { (s, c) =>
      c.copy(lonAttribute = Option(s)) } optional() hidden()
    opt[String]("dateAttribute").action { (s, c) =>
      c.copy(dtField = Option(s)) } optional() hidden()
    opt[Option[Int]]('m', "maxFeatures").action { (s, c) =>
      c.copy(maxFeatures = s) } optional() text "Maximum number of features to return. default: 2147483647"
    opt[Option[String]]('q', "query").action { (s, c) =>
      c.copy(query = s )} optional() text "ECQL query to run on the features. default: INCLUDE"
    instanceNameOpt
    zookeepersOpt
    visibilitiesOpt
    authsOpt
  }

  parser.parse(args, ExportArguments()).map(config => {
    val pw = password(config.password)
    val export = new Export(config, pw)
    export.exportFeatures()
  }).getOrElse(
      logger.error("Error: command not recognized.")
    )
}