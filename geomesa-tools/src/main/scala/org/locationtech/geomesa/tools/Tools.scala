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
package org.locationtech.geomesa.tools

import com.typesafe.scalalogging.slf4j.Logging

object Tools extends App with Logging with GetPassword {
  val parser = new scopt.OptionParser[ScoptArguments]("geomesa-tools") {
    def catalogOpt = opt[String]('c', "catalog").action { (s, c) =>
      c.copy(catalog = s) } required() hidden()
    def featureOpt = opt[String]('f', "feature-name").action { (s, c) =>
      c.copy(featureName = s) } required() hidden()
    def specOpt = opt[String]('s', "spec").action { (s, c) =>
      c.copy(spec = s) } required() hidden()
    def userOpt = opt[String]('u', "username") action { (x, c) =>
      c.copy(username = x) } text "Accumulo username" required()
    def passOpt = opt[String]('p', "password") action { (x, c) =>
      c.copy(password = x) } text "Accumulo password" optional()
    def instanceNameOpt = opt[String]('i', "instance-name") action { (x, c) =>
      c.copy(instanceName = x) } text "Accumulo instance name" optional()
    def zookeepersOpt = opt[String]('z', "zookeepers") action { (x, c) =>
      c.copy(zookeepers = x) } text "Zookeepers comma-separated instances string" optional()
    def visibilitiesOpt = opt[String]('v', "visibilities") action { (x, c) =>
      c.copy(visibilities = x) } text "Accumulo visibilities string" optional()
    def authsOpt = opt[String]('a', "auths") action { (x, c) =>
      c.copy(auths = x) } text "Accumulo auths string" optional()

    def describe = cmd("describe") action { (_, c) =>
      c.copy(mode = "describe") } text "Describe the specified feature" children(
      userOpt,
      passOpt,
      catalogOpt,
      featureOpt,
      opt[Unit]('q', "quiet").action { (_, c) =>
        c.copy(toStdOut = true) } optional() hidden(),
      instanceNameOpt,
      zookeepersOpt,
      visibilitiesOpt,
      authsOpt
      )

    def list = cmd("list") action { (_, c) =>
      c.copy(mode = "list") } text "List the features in the specified Catalog Table" children(
      userOpt,
      passOpt,
      catalogOpt,
      opt[Unit]('q', "quiet").action { (_, c) =>
        c.copy(toStdOut = true) } optional() hidden(),
      instanceNameOpt,
      zookeepersOpt,
      visibilitiesOpt,
      authsOpt
      )

    def explain = cmd("explain") action { (_, c) =>
      c.copy(mode = "explain") } text "Explain and plan a query in GeoMesa" children(
      userOpt,
      passOpt,
      catalogOpt,
      featureOpt,
      opt[String]('q', "filter").action { (s, c) =>
        c.copy(query = s) } required() hidden(),
      instanceNameOpt,
      zookeepersOpt,
      visibilitiesOpt,
      authsOpt
      )

    def delete = cmd("delete") action { (_, c) =>
      c.copy(mode = "delete") } text "Delete a feature from the specified Catalog Table in GeoMesa" children(
      userOpt,
      passOpt,
      catalogOpt,
      featureOpt,
      instanceNameOpt,
      zookeepersOpt,
      visibilitiesOpt,
      authsOpt
      )

    def create = cmd("create") action { (_, c) =>
      c.copy(mode = "create") } text "Create a feature in GeoMesa" children(
      userOpt,
      passOpt,
      catalogOpt,
      featureOpt,
      specOpt,
      opt[String]('d', "default-date").action { (s, c) =>
        c.copy(dtField = Option(s)) } optional() hidden(),
      instanceNameOpt,
      zookeepersOpt,
      visibilitiesOpt,
      authsOpt
      )

    def tableconf = cmd("tableconf") action { (_, c) =>
      c.copy(mode = "tableconf") } text "Configure a GeoMesa table" children(
      cmd("list") action { (_, c) => c.copy(method = "list") } text "List all configuration parameters of a given table" children(
        userOpt,
        passOpt,
        catalogOpt,
        featureOpt,
        opt[String]('s', "suffix").action { (s, c) =>
          c.copy(suffix = s) } required() hidden()),
        instanceNameOpt,
        zookeepersOpt,
        visibilitiesOpt,
        authsOpt,
      cmd("describe") action { (_, c) => c.copy(method = "describe") } text "Return the value of a single parameter of the given table" children(
        userOpt,
        passOpt,
        catalogOpt,
        featureOpt,
        opt[String]("param").action { (s, c) =>
          c.copy(param = s) } required() hidden(),
        opt[String]('s', "suffix").action { (s, c) =>
          c.copy(suffix = s) } required() hidden()),
        instanceNameOpt,
        zookeepersOpt,
        visibilitiesOpt,
        authsOpt,
      cmd("update") action { (_, c) => c.copy(method = "update") } text "Update a configuration parameter of the given table" children(
        userOpt,
        passOpt,
        catalogOpt,
        featureOpt,
        opt[String]("param").action { (s, c) =>
          c.copy(param = s) } required() hidden(),
        opt[String]('n', "new-value").action { (s, c) =>
          c.copy(newValue = s) } required() hidden(),
        opt[String]('s', "suffix").action { (s, c) =>
          c.copy(suffix = s) } required() hidden()
        ),
        instanceNameOpt,
        zookeepersOpt,
        visibilitiesOpt,
        authsOpt
      )
    head("GeoMesa Tools", "1.0")
    help("help").text("show help command")
    create
    delete
    describe
    explain
    list
    tableconf
  }

  /**
   * The default scopt help/usage is too verbose and isn't helpful if you only want the help text for a single command,
   * so we roll our own.
   *
   * This will look to see which command is contained in the arguments passed to geomesa-tools. Then, it will print
   * out that usage text. If a suitable command isn't found, it will show the default usage texts for geomesa-tools.
   */
  def printHelp(): Unit = {
    val usernameHelp = "\t-u, --username : required\n\t\tthe Accumulo username\n"
    val passwordHelp = "\t-p, --password : optional\n\t\tthe Accumulo password. This can also be provided after entering a command.\n"
    val catalogHelp = "\t-c, --catalog : required\n\t\tthe name of the Accumulo table to use\n"
    val featureHelp = "\t-f, --feature-name : required\n\t\tthe name of the feature\n"
    val specHelp = "\t-s, --spec : required\n\t\tthe SFT specification for the new feature\n"
    val suffixHelp = "\t-s, --suffix : required\n\t\tthe table suffix (attr_idx, st_idx, or records)\n"
    val help = if (args.contains("create")) {
      "Create a feature in GeoMesa\n" + usernameHelp + passwordHelp +
        "\t-c, --catalog : required\n" +
        "\t\tthe name of the Accumulo table to use -- or create, if it does not already exist -- to contain the new data\n" +
        "\t-f, --feature-name : required\n" +
        "\t\tthe name of the new feature to be created\n" +
        specHelp +
        "\t-d, --default-date : optional\n" +
        "\t\tthe default date of the sft"
    } else if (args.contains("delete")) {
      "Delete a feature from the specified Catalog Table in GeoMesa\n" + usernameHelp + passwordHelp + catalogHelp +
        "\t-f, --feature-name : required\n" +
        "\t\tthe name of the feature to be deleted"
    } else if (args.contains("describe") && !args.contains("tableconf")) {
      "Describe the attributes of a specified feature\n" + usernameHelp + passwordHelp + catalogHelp +
        "\t-f, --feature-name : required\n" +
        "\t\tthe name of the feature to be described\n" +
        "\t-q, --quiet : optional\n" +
        "\t\tget output from the command without any info statements. useful for piping output\n"
    } else if (args.contains("explain")) {
      "Explain and plan a query in GeoMesa\n" + usernameHelp + passwordHelp + catalogHelp + featureHelp +
        "\t-q, --filter : required\n" +
        "\t\tthe filter string to apply, plan, and explain"
    } else if (args.contains("list") && !args.contains("tableconf")) {
      "List the features in the specified Catalog Table\n" + usernameHelp + passwordHelp + catalogHelp +
        "\t-q, --quiet : optional\n" +
        "\t\tget output from the command without any info statements. useful for piping output\n"
    } else if (args.contains("tableconf") && args.contains("list")) {
        "List all table configuration parameters.\n" + usernameHelp + passwordHelp + catalogHelp + featureHelp + suffixHelp
    } else if (args.contains("tableconf") && args.contains("describe")) {
      "Print the value of a single table configuration parameter\n" + usernameHelp + passwordHelp + catalogHelp + featureHelp +
        "\t--param : required\n" +
        "\t\tthe table configuration parameter to describe\n" +
        suffixHelp
    } else if (args.contains("tableconf") && args.contains( "update")) {
      "Update a table configuration parameter to the new specified value\n" + usernameHelp + passwordHelp + catalogHelp + featureHelp +
        "\t--param : required\n" +
        "\t\tthe table configuration parameter to update\n" +
        "\t-n, --new-value : required\n" +
        "\t\tthe new value for the table configuration parameter\n" +
        suffixHelp
    } else {
      "GeoMesa Tools 1.0\n" +
        "Required for each command:\n" +
        "\t-u, --username: the Accumulo username : required\n" +
        "Optional parameters:\n" +
        "\t-p, --password: the Accumulo password. This can also be provided after entering a command.\n" +
        "\thelp, -help, --help: show this help dialog or the help dialog for a specific command (e.g. geomesa create help)\n" +
        "Supported commands are:\n" +
        "\tcreate: Create a feature in GeoMesa\n" +
        "\tdelete: Delete a feature from the specified Catalog Table in GeoMesa\n" +
        "\tdescribe: Describe the attributes of a specified feature\n" +
        "\texplain: Explain and plan a query in GeoMesa\n" +
        "\texport: Export all or a set of features in CSV, TSV, GeoJSON, GML, or SHP format\n" +
        "\tingest: Ingest features into GeoMesa" +
        "\tlist: List the features in the specified Catalog Table\n" +
        "\ttableconf: List, describe, and update table configuration parameters"
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
      config.mode match {
        case "list" =>
          val ft = new FeaturesTool(config, password)
          if (!config.toStdOut) { logger.info(s"Listing features on '${config.catalog}'. Just a few moments...") }
          ft.listFeatures()
        case "describe" =>
          val ft = new FeaturesTool(config, password)
          if (!config.toStdOut) { logger.info(s"Describing attributes of feature '${config.featureName}' on catalog table '${config.catalog}'. Just a few moments...") }
          ft.describeFeature()
        case "explain" =>
          val ft = new FeaturesTool(config, password)
          ft.explainQuery()
        case "delete" =>
          val ft = new FeaturesTool(config, password)
          logger.info(s"Deleting '${config.featureName}' on catalog table '${config.catalog}'. This will take longer " +
            "than other commands to complete. Just a few moments...")
          if (ft.deleteFeature()) {
            logger.info(s"Feature '${config.featureName}' on catalog table '${config.catalog}' successfully deleted.")
          } else {
            logger.error(s"There was an error deleting feature '${config.featureName}' on catalog table '${config.catalog}'" +
              "Please check that all arguments are correct in the previous command.")
          }
        case "create" =>
          val ft = new FeaturesTool(config, password)
          logger.info(s"Creating '${config.featureName}' on catalog table '${config.catalog}' with spec '${config.spec}'. Just a few moments...")
          if (ft.createFeatureType()) {
            logger.info(s"Feature '${config.featureName}' on catalog table '${config.catalog}' with spec '${config.spec}' successfully created.")
          } else {
            logger.error(s"There was an error creating feature '${config.featureName}' on catalog table '${config.catalog}' with spec '${config.spec}'." +
              " Please check that all arguments are correct in the previous command.")
          }
        case "tableconf" =>
          val tt = new TableTools(config, password)
          config.method match {
            case "list" => tt.listConfig()
            case "describe" => tt.describeConfig()
            case "update" => tt.updateConfig()
          }
      }
    }
    ).getOrElse(
        logger.error("Error: command not recognized.")
      )
  }
}
