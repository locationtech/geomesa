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

import scala.util.Try

object Tools extends App with Logging {
  val parser = new scopt.OptionParser[ScoptArguments]("geomesa-tools") {
    def catalogOpt = opt[String]('c', "catalog").action { (s, c) =>
      c.copy(catalog = s) } required() hidden()
    def featureOpt = opt[String]('f', "feature_name").action { (s, c) =>
      c.copy(featureName = s) } required() hidden()
    def specOpt = opt[String]('s', "spec").action { (s, c) =>
      c.copy(spec = s) } required() hidden()
    def userOpt = opt[String]('u', "username") action { (x, c) =>
      c.copy(username = x) } text "Accumulo username" required()
    def passOpt = opt[String]('p', "password") action { (x, c) =>
      c.copy(password = x) } text "Accumulo password" optional()

    def export = cmd("export") action { (_, c) =>
      c.copy(mode = "export") } text "Export all or a set of features in csv, geojson, gml, or shp format" children(
      userOpt,
      passOpt,
      catalogOpt,
      featureOpt,
      opt[String]('o', "format").action { (s, c) =>
        c.copy(format = s) } required() hidden(),
      opt[String]('a', "attributes").action { (s, c) =>
        c.copy(attributes = s) } optional() hidden(),
      opt[String]("latAttribute").action { (s, c) =>
        c.copy(latAttribute = Option(s)) } optional() hidden(),
      opt[String]("lonAttribute").action { (s, c) =>
        c.copy(lonAttribute = Option(s)) } optional() hidden(),
      opt[String]("dateAttribute").action { (s, c) =>
        c.copy(dateAttribute = Option(s)) } optional() hidden(),
      opt[Int]('m', "maxFeatures").action { (s, c) =>
        c.copy(maxFeatures = s) } optional() hidden(),
      opt[String]('q', "query").action { (s, c) =>
        c.copy(query = s )} optional() hidden()
      )

    def describe = cmd("describe") action { (_, c) =>
      c.copy(mode = "describe") } text "Describe the specified feature" children(
      userOpt,
      passOpt,
      catalogOpt,
      featureOpt
      )

    def list = cmd("list") action { (_, c) =>
      c.copy(mode = "list") } text "List the features in the specified Catalog Table" children(
      userOpt,
      passOpt,
      catalogOpt
      )

    def explain = cmd("explain") action { (_, c) =>
      c.copy(mode = "explain") } text "Explain and plan a query in Geomesa" children(
      userOpt,
      passOpt,
      catalogOpt,
      featureOpt,
      opt[String]('q', "filter").action { (s, c) =>
        c.copy(filterString = s) } required() hidden()
      )

    def delete = cmd("delete") action { (_, c) =>
      c.copy(mode = "delete") } text "Delete a feature from the specified Catalog Table in Geomesa" children(
      userOpt,
      passOpt,
      catalogOpt,
      featureOpt
      )

    def create = cmd("create") action { (_, c) =>
      c.copy(mode = "create") } text "Create a feature in Geomesa" children(
      userOpt,
      passOpt,
      catalogOpt,
      featureOpt,
      specOpt,
      opt[String]('d', "default_date").action { (s, c) =>
        c.copy(defaultDate = s) } optional() hidden()
      )

    def ingest = cmd("ingest") action { (_, c) =>
      c.copy(mode = "ingest") } text "Ingest a feature into GeoMesa" children (
      userOpt,
      passOpt,
      opt[String]("file").action { (s, c) =>
        c.copy(file = s) } required() hidden(),
      opt[String]("format").action { (s, c) =>
        c.copy(format = s.toUpperCase) } required() hidden(),
      catalogOpt,
      featureOpt,
      specOpt,
      opt[String]("datetime").action { (s, c) =>
        c.copy(dtField = s) } required() hidden(),
      opt[String]("dtformat").action { (s, c) =>
        c.copy(dtFormat = s) } required() hidden()
      )

    head("GeoMesa Tools", "1.0")
    help("help").text("show help command")
    create
    delete
    describe
    explain
    export
    ingest
    list
  }

  /**
   * The default scopt help/usage is too verbose and isn't helpful if you only want the help text for a single command,
   * so we roll our own.
   *
   * This will look to see which command is contained in the arguments passed to geomesa-tools. Then, it will print
   * out that usage text. If a suitable command isn't found, it will show the default usage texts for geomesa-tools.
   */
  def printHelp(): Unit = {
    val help = if (args.contains("create")) {
      "\tCreate a feature in Geomesa\n" +
        "\t-u, --username : required\n" +
        "\t\tthe Accumulo username\n" +
        "\t-p, --password : optional\n" +
        "\t\tthe Accumulo password. This can also be provided after entering a command.\n" +
        "\t-c, --catalog : required\n" +
        "\t\tthe name of the Accumulo table to use -- or create, if it does not already exist -- to contain the new data\n" +
        "\t-f, --feature_name : required\n" +
        "\t\t\"the name of the new feature to be created\n" +
        "\t-s, --spec : required\n" +
        "\t\tthe SFT specification for the new feature\n" +
        "\t-d, --default_date : optional\n" +
        "\t\tthe default date of the sft"
    } else if (args.contains("delete")) {
      "\tDelete a feature from the specified Catalog Table in Geomesa\n" +
        "\t-u, --username : required\n" +
        "\t\tthe Accumulo username\n" +
        "\t-p, --passwor : optionald\n" +
        "\t\tthe Accumulo password. This can also be provided after entering a command.\n" +
        "\t-c, --catalog : required\n" +
        "\t\tthe name of the Accumulo table to use\n" +
        "\t-f, --feature_name : required\n" +
        "\t\tthe name of the feature to be deleted"
    } else if (args.contains("describe")) {
      "\tDescribe the attributes of a specified feature\n" +
        "\t-u, --username : required\n" +
        "\t\tthe Accumulo username\n" +
        "\t-p, --password : optional\n" +
        "\t\tthe Accumulo password. This can also be provided after entering a command.\n" +
        "\t-c, --catalog : required\n" +
        "\t\tthe name of the Accumulo table to use\n" +
        "\t-f, --feature_name : required\n" +
        "\t\tthe name of the feature to be described"
    } else if (args.contains("explain")) {
      "\tExplain and plan a query in Geomesa\n" +
        "\t-u, --username : required\n" +
        "\t\tthe Accumulo username\n" +
        "\t-p, --password : optional\n" +
        "\t\tthe Accumulo password. This can also be provided after entering a command.\n" +
        "\t-c, --catalog : required\n" +
        "\t\tthe name of the Accumulo table to use\n" +
        "\t-f, --feature_name : required\n" +
        "\t\tthe name of the feature to use\n" +
        "\t-q, --filter : required\n" +
        "\t\tthe filter string to apply, plan, and explain"
    } else if (args.contains("export")) {
      "\tExport all or a set of features in csv, tsv, geojson, gml, or shp format\n" +
        "\t-u, --username : required\n" +
        "\t\tthe Accumulo username\n" +
        "\t-p, --password : optional\n" +
        "\t\tthe Accumulo password. This can also be provided after entering a command.\n" +
        "\t-c, --catalog : required\n" +
        "\t\tthe name of the Accumulo table to use\n" +
        "\t-f, --feature_name : required\n" +
        "\t\tthe name of the feature to export\n" +
        "\t-o, --format : required\n" +
        "\t\tthe format to export to (csv, tsv, gml, geojson, shp)\n" +
        "\t-a, --attributes : optional\n" +
        "\t\tattributes to return in the export. default: ALL\n"
        "\t-m, --maxFeatures : optional\n" +
        "\t\tmax number of features to return. default: 2147483647\n" +
        "\t-q, --query : optional\n" +
        "\t\tECQL query to run on the features. default: INCLUDE\n"
      //commenting out for now because these aren't implemented yet, but will be in the future
//        "\t--latAttribute : optional\n" +
//        "\t\tlatitude attribute to query on\n" +
//        "\t--lonAttribute : optional\n" +
//        "\t\tlongitude attribute to query on\n" +
//        "\t--dateAttribute : optional\n" +
//        "\t\tdate attribute to query on"
    } else if (args.contains("ingest")) {
      "\tIngest a feature into GeoMesa\n" +
        "\t-u, --username : required\n" +
        "\t\tthe Accumulo username\n" +
        "\t-p, --password : optional\n" +
        "\t\tthe Accumulo password. This can also be provided after entering a command.\n" +
        "\t--file : required\n" +
        "\t\tthe file you wish to ingest, e.g.: ~/capelookout.csv\n" +
        "\t--format : required\n" +
        "\t\tthe format of the file, it must be csv or tsv\n" +
        "\t-c, --catalog : required\n" +
        "\t\tthe name of the Accumulo table to use -- or create, if it does not already exist -- to contain the new data\n" +
        "\t-f, --feature_name : required\n" +
        "\t\tthe name of the feature to export\n" +
        "\t-s, --spec : required\n" +
        "\t\tthe sft specification for the file\n" +
        "\t--datetime : required\n" +
        "\t\tthe name of the datetime field in the sft\n" +
        "\t--dtformat : required\n" +
        "\t\tthe format of the datetime field"
    } else if (args.contains("list")) {
      "\tList the features in the specified Catalog Table\n" +
        "\t-u, --username : required\n" +
        "\t\tthe Accumulo username\n" +
        "\t-p, --password : optional\n" +
        "\t\tthe Accumulo password. This can also be provided after entering a command.\n" +
        "\t-c, --catalog : required\n" +
        "\t\tthe name of the Accumulo table to use"
    } else {
      "Geomesa Tools 1.0\n" +
        "Required for each command:\n" +
        "\t-u, --username: the Accumulo username : required\n" +
        "Optional parameters:\n" +
        "\t-p, --password: the Accumulo password. This can also be provided after entering a command.\n" +
        "\thelp, -help, --help: show this help dialog or the help dialog for a specific command (e.g. geomesa create help)\n" +
        "Supported commands are:\n" +
        "\t create: Create a feature in Geomesa\n" +
        "\t delete: Delete a feature from the specified Catalog Table in Geomesa\n" +
        "\t describe: Describe the attributes of a specified feature\n" +
        "\t explain: Explain and plan a query in Geomesa\n" +
        "\t export: Export all or a set of features in csv, geojson, gml, or shp format\n" +
        "\t ingest: Ingest a feature into GeoMesa\n" +
        "\t list: List the features in the specified Catalog Table"
    }
    logger.info(s"$help")
  }

  if (args.contains("help") || args.contains("--help") || args.contains("-help")) {
    printHelp()
  } else {
    parser.parse(args, ScoptArguments()).map(config => {
      val password = if (config.password == null) {
        val standardIn = System.console()
        print("Password> ")
        standardIn.readPassword().mkString
      } else {
        config.password
      }
      val ftTry = Try(new FeaturesTool(config, password))
      val ft: FeaturesTool = ftTry.getOrElse{
        logger.error("Incorrect username or password. Please try again.")
        sys.exit()
      }
      config.mode match {
        case "export" =>
          logger.info(s"Exporting '${config.catalog}_${config.featureName}'. Just a few moments...")
          ft.exportFeatures()
        case "list" =>
          logger.info(s"Listing features on '${config.catalog}'. Just a few moments...")
          ft.listFeatures()
        case "describe" =>
          logger.info(s"Describing attributes of feature '${config.catalog}_${config.featureName}'. Just a few moments...")
          ft.describeFeature()
        case "explain" =>
          ft.explainQuery()
        case "delete" =>
          logger.info(s"Deleting '${config.catalog}_${config.featureName}'. This may be a good time to grab a coffee, as this will take a few moments...")
          if (ft.deleteFeature()) {
            logger.info(s"Feature '${config.catalog}_${config.featureName}' successfully deleted.")
          } else {
            logger.error(s"There was an error deleting feature '${config.catalog}_${config.featureName}'" +
              "Please check that all arguments are correct in the previous command.")
          }
        case "create" =>
          logger.info(s"Creating '${config.catalog}_${config.featureName}' with spec '${config.spec}'. Just a few moments...")
          if (ft.createFeatureType(config.featureName, config.spec, config.defaultDate)) {
            logger.info(s"Feature '${config.catalog}_${config.featureName}' with spec '${config.spec}' successfully created.")
          } else {
            logger.error(s"There was an error creating feature '${config.catalog}_${config.featureName}' with spec '${config.spec}'." +
              " Please check that all arguments are correct in the previous command.")
          }
        case "ingest" =>
          val ingest = new Ingest()
          ingest.defineIngestJob(config, password) match {
            case true => logger.info(s"Successful ingest of file: \'${config.file}\'")
            case false => logger.error(s"Error: could not successfully ingest file: \'${config.file}\'")
          }
      }
    }
    ).getOrElse(
        logger.error("Error: command not recognized.")
      )
  }
}

/*  ScoptArguments is a case Class used by scopt, args are stored in it and default values can be set in Config also.*/
case class ScoptArguments(username: String = null, password: String = null, mode: String = null, spec: String = null,
                          idFields: String = null, latField: String = null, lonField: String = null,
                          dtField: String = null, dtFormat: String = null, method: String = "local",
                          file: String = null, featureName: String = null, format: String = null,
                          catalog: String = null, maxFeatures: Int = -1, defaultDate: String = null,
                          filterString: String = null, attributes: String = null,
                          lonAttribute: Option[String] = None, latAttribute: Option[String] = None,
                          dateAttribute: Option[String] = None, query: String = null)



