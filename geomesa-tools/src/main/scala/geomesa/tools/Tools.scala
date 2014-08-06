/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package geomesa.tools

import com.typesafe.scalalogging.slf4j.Logging

object Tools extends App with Logging {
  val parser = new scopt.OptionParser[ScoptArguments]("geomesa-tools") {
    head("GeoMesa Tools", "1.0")
    help("help").text("show help command")

    cmd("export") action { (_, c) =>
      c.copy(mode = "export") } text "Export all or a set of features in csv, geojson, gml, or shp format" children(
      opt[String]("catalog").action { (s, c) =>
        c.copy(catalog = s) } text "the name of the Accumulo table to use -- or create, " +
        "if it does not already exist -- to contain the new data" required(),
      opt[String]("typeName").action { (s, c) =>
        c.copy(typeName = s) } text "the name of the feature to export" required(),
      opt[String]("format").action { (s, c) =>
        c.copy(format = s) } text "the format to export to (e.g. csv, tsv)" required(),
      opt[String]("attributes").action { (s, c) =>
        c.copy(attributes = s) } text "attributes to return in the export" optional(),
      opt[String]("idAttribute").action { (s, c) =>
        c.copy(idAttribute = s) } text "feature ID attribute to query on" optional(),
      opt[String]("latAttribute").action { (s, c) =>
        c.copy(latAttribute = Option(s)) } text "latitude attribute to query on" optional(),
      opt[String]("lonAttribute").action { (s, c) =>
        c.copy(lonAttribute = Option(s)) } text "longitude attribute to query on" optional(),
      opt[String]("dateAttribute").action { (s, c) =>
        c.copy(dateAttribute = Option(s)) } text "date attribute to query on" optional(),
      opt[Int]("maxFeatures").action { (s, c) =>
        c.copy(maxFeatures = s) } text "max number of features to return" optional(),
      opt[String]("query").action { (s, c) =>
        c.copy(query = s )} text "ECQL query to run on the features" optional()
      )

    cmd("list") action { (_, c) =>
      c.copy(mode = "list") } text "List the features in the specified Catalog Table" children(
      opt[String]("catalog").action { (s, c) =>
        c.copy(catalog = s) } text "the name of the Accumulo table to use" required()
      )

    cmd("explain") action { (_, c) =>
      c.copy(mode = "explain") } text "Explain and plan a query in Geomesa" children(
      opt[String]("catalog").action { (s, c) =>
        c.copy(catalog = s) } text "the name of the Accumulo table to use" required(),
      opt[String]("typeName").action { (s, c) =>
        c.copy(typeName = s) } text "the name of the new feature to be create" required(),
      opt[String]("filter").action { (s, c) =>
        c.copy(filterString = s) } text "the filter string" required()
      )

    cmd("delete") action { (_, c) =>
      c.copy(mode = "delete") } text "Delete a feature from the specified Catalog Table in Geomesa" children(
      opt[String]("catalog").action { (s, c) =>
        c.copy(catalog = s) } text "the name of the Accumulo table to use" required(),
      opt[String]("typeName").action { (s, c) =>
        c.copy(typeName = s) } text "the name of the new feature to be create" required()
      )

    cmd("create") action { (_, c) =>
      c.copy(mode = "create") } text "Create a feature in Geomesa" children(
      opt[String]("catalog").action { (s, c) =>
        c.copy(catalog = s) } text "the name of the Accumulo table to use -- or create, " +
        "if it does not already exist -- to contain the new data" required(),
      opt[String]("typeName").action { (s, c) =>
        c.copy(typeName = s) } text "the name of the new feature to be create" required(),
      opt[String]("sft").action { (s, c) =>
        c.copy(sft = s) } text "the string representation of the SimpleFeatureType" required()
      )

    cmd("ingest") action { (_, c) =>
      c.copy(mode = "ingest") } text "Ingest a feature into GeoMesa" children (
      opt[String]("file").action { (s, c) =>
        c.copy(file = s) } text "the file you wish to ingest, e.g.: ~/capelookout.csv" required(),
      opt[String]("format").action { (s, c) =>
        c.copy(format = s.toUpperCase) } text "the format of the file, it must be csv or tsv" required(),
      opt[String]("table").action { (s, c) =>
        c.copy(table = s) } text "the name of the Accumulo table to use -- or create, " +
        "if it does not already exist -- to contain the new data" required(),
      opt[String]("typeName").action { (s, c) =>
        c.copy(typeName = s) } text "the name of the feature type to be ingested" required(),
      opt[String]('s', "spec").action { (s, c) =>
        c.copy(spec = s) } text "the sft specification for the file" required(),
      opt[String]("datetime").action { (s, c) =>
        c.copy(dtField = s) } text "the name of the datetime field in the sft" required(),
      opt[String]("dtformat").action { (s, c) =>
        c.copy(dtFormat = s) } text "the format of the datetime field" required()
      )
  }

  parser.parse(args, ScoptArguments()).map(config =>
    config.mode match {
      case "export" =>
        logger.info(s"Exporting '${config.typeName}' from '${config.catalog}'. Just a few moments...")
        val ft = new FeaturesTool(config.catalog)
        ft.exportFeatures(
          config.typeName,
          config.attributes,
          config.idAttribute,
          config.latAttribute,
          config.lonAttribute,
          config.dateAttribute,
          config.format,
          config.query,
          config.maxFeatures)
      case "list" =>
        logger.info(s"Listing features on '${config.catalog}'. Just a few moments...")
        val ft = new FeaturesTool(config.catalog)
        ft.listFeatures()
      case "explain" =>
        val ft = new FeaturesTool(config.catalog)
        ft.explainQuery(config.typeName, config.filterString)
      case "delete" =>
        val ft = new FeaturesTool(config.catalog)
        logger.info(s"Deleting '${config.typeName}.' Just a few moments...")
        if (ft.deleteFeature(config.typeName)) {
          logger.info(s"Feature '${config.typeName}' successfully deleted.")
        } else {
          logger.error(s"There was an error deleting feature '${config.typeName}'." +
            " Please check that your configuration settings are correct and try again.")
        }
      case "create" =>
        val ft = new FeaturesTool(config.catalog)
        logger.info(s"Creating '${config.typeName}' with schema '${config.sft}'. Just a few moments...")
        if (ft.createFeatureType(config.typeName, config.sft)) {
          logger.info(s"Feature '${config.typeName}' with schema '${config.sft}' successfully created.")
        } else {
          logger.error(s"There was an error creating feature '${config.typeName}' with featureType '${config.sft}'." +
            " Please check that your configuration settings are correct and try again.")
        }
      case "ingest" =>
        val ingest = new Ingest()
        ingest.defineIngestJob(config) match {
          case true => logger.info(s"Successful ingest of file: \'${config.file}\'")
          case false => logger.error(s"Error: could not successfully ingest file: \'${config.file}\'")
        }
    }
  ).getOrElse(
      logger.error("Error: command not recognized.")
    )
}

/*  ScoptArguments is a case Class used by scopt, args are stored in it and default values can be set in Config also.*/
case class ScoptArguments(mode: String = null, table: String = null, spec: String = null,
                          idFields: String = null, latField: String = null, lonField: String = null,
                          dtField: String = null, dtFormat: String = null, method: String = "local",
                          file: String = null, typeName: String = null, format: String = null,
                          catalog: String = null, sft: String = null, maxFeatures: Int = -1,
                          filterString: String = null, attributes: String = null, idAttribute: String = null,
                          lonAttribute: Option[String] = None, latAttribute: Option[String] = None,
                          dateAttribute: Option[String] = None, query: String = null)



