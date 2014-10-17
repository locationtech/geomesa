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
  val parser = new scopt.OptionParser[FeatureArguments]("geomesa-tools") {
    implicit val optionStringRead: scopt.Read[Option[String]] = scopt.Read.reads(Option[String])
    implicit val optionBooleanRead: scopt.Read[Option[Boolean]] = scopt.Read.reads(b => Option(b.toBoolean))
    implicit val optionIntRead: scopt.Read[Option[Int]] = scopt.Read.reads(i => Option(i.toInt))
    def catalogOpt = opt[String]('c', "catalog").action { (s, c) =>
      c.copy(catalog = s) } required()
    def featureOpt = opt[String]('f', "feature-name").action { (s, c) =>
      c.copy(featureName = s) } required()
    def specOpt = opt[String]('s', "spec").action { (s, c) =>
      c.copy(spec = s) } required()
    def userOpt = opt[String]('u', "username") action { (x, c) =>
      c.copy(username = x) } text "Accumulo username" required()
    def passOpt = opt[Option[String]]('p', "password") action { (x, c) =>
      c.copy(password = x) } text "Accumulo password" optional()
    def instanceNameOpt = opt[Option[String]]('i', "instance-name") action { (x, c) =>
      c.copy(instanceName = x) } text "Accumulo instance name" optional()
    def zookeepersOpt = opt[Option[String]]('z', "zookeepers") action { (x, c) =>
      c.copy(zookeepers = x) } text "Zookeepers comma-separated instances string" optional()
    def visibilitiesOpt = opt[Option[String]]('v', "visibilities") action { (x, c) =>
      c.copy(visibilities = x) } text "Accumulo visibilities string" optional()
    def authsOpt = opt[Option[String]]("auths") action { (x, c) =>
      c.copy(auths = x) } text "Accumulo auths string" optional()
    def suffixOpt = opt[String]('s', "suffix").action { (s, c) =>
      c.copy(suffix = s) } required()
    def paramOpt = opt[String]("param").action { (s, c) =>
      c.copy(param = s) } required()
    //Todo: shared-tables or share-tables?
    def sharingOpt = opt[Option[Boolean]]("shared-tables") action { (x, c) =>
      c.copy(sharedTable = x) } text "Set the Accumulo table sharing (default true)" optional()
    def shardOpt = opt[Option[Int]]("shards") action { (i, c) =>
      c.copy(maxShards = i) } text "Accumulo number of shards to use (optional)" optional()

    def describe = cmd("describe") action { (_, c) =>
      c.copy(mode = "describe") } text "Describe the specified feature" children(
      userOpt,
      passOpt,
      catalogOpt,
      featureOpt,
      opt[Unit]('q', "quiet").action { (_, c) =>
        c.copy(toStdOut = true) } optional(),
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
        c.copy(toStdOut = true) } optional(),
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
        c.copy(query = s) } required(),
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
      authsOpt,
      opt[Unit]("force").action { (b, c) =>
        c.copy(forceDelete = true) } text "delete without prompt for confirmation" optional()
      )

    def create = cmd("create") action { (_, c) =>
      c.copy(mode = "create") } text "Create a feature in GeoMesa" children(
      userOpt,
      passOpt,
      catalogOpt,
      featureOpt,
      specOpt,
      opt[String]('d', "dt-field").action { (s, c) =>
        c.copy(dtField = Option(s)) } optional(),
      instanceNameOpt,
      zookeepersOpt,
      visibilitiesOpt,
      authsOpt,
      sharingOpt,
      shardOpt
      )

    def tableconf = cmd("tableconf") action { (_, c) =>
      c.copy(mode = "tableconf") } text "Configure a GeoMesa table" children(
      cmd("list") action { (_, c) => c.copy(method = "list") } text "List all configuration parameters of a given table" children(
        userOpt,
        passOpt,
        catalogOpt,
        featureOpt,
        suffixOpt,
        instanceNameOpt,
        zookeepersOpt,
        visibilitiesOpt,
        authsOpt),
      cmd("describe") action { (_, c) => c.copy(method = "describe") } text "Return the value of a single parameter of the given table" children(
        userOpt,
        passOpt,
        catalogOpt,
        featureOpt,
        paramOpt,
        suffixOpt,
        instanceNameOpt,
        zookeepersOpt,
        visibilitiesOpt,
        authsOpt),
      cmd("update") action { (_, c) => c.copy(method = "update") } text "Update a configuration parameter of the given table" children(
        userOpt,
        passOpt,
        catalogOpt,
        featureOpt,
        paramOpt,
        opt[String]('n', "new-value").action { (s, c) =>
          c.copy(newValue = s) } required(),
        suffixOpt,
        instanceNameOpt,
        zookeepersOpt,
        visibilitiesOpt,
        authsOpt
        // Todo: maybe here add sharingTable opt and shards opt?
        )
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

  def buildHelpText(char: Option[Char], string: String, required: Boolean, description: String): String = {
    val requiredOrOptional = if (required) { "required" } else { "optional" }
    val flags = if (char.isDefined) { s"-${char.get}, --$string" } else { s"--$string" }
    s"\t$flags : $requiredOrOptional\n\t\t$description\n"
  }

  /**
   * The default scopt help/usage is too verbose and isn't helpful if you only want the help text for a single command,
   * so we roll our own.
   *
   * This will look to see which command is contained in the arguments passed to geomesa-tools. Then, it will print
   * out that usage text. If a suitable command isn't found, it will show the default usage texts for geomesa-tools.
   */
  def printHelp(): Unit = {
    //Todo: update help to include shards and shared table args where needed
    val usernameHelp = buildHelpText(Some('u'), "username", required = true, "the Accumulo username")
    val passwordHelp = buildHelpText(Some('p'), "password", required = false,
      "the Accumulo password. This can also be provided after entering a command")
    val instanceNameHelp = buildHelpText(Some('i'), "instance-name", required = false,
      "Accumulo instance name. This can usually be discovered by GeoMesa-Tools automatically.")
    val zookeeperHelp = buildHelpText(Some('z'), "zookeepers", required = false,
      "Zookeeper comma-separated instances string. This can usually be discovered by GeoMesa-Tools automatically.")
    val authsHelp = buildHelpText(None, "auths", required = false, "Accumulo authorizations string")
    val visibilitiesHelp = buildHelpText(Some('v'), "visibilities", required = false, "Accumulo visibilities string")
    val catalogHelp = buildHelpText(Some('c'), "catalog", required = true, "the name of the Accumulo table to use")
    val featureHelp =  buildHelpText(Some('f'), "feature-name", required = true, "the name of the feature")
    val specHelp = buildHelpText(Some('s'), "spec", required = true, "the SFT specification for the new feature")
    val suffixHelp = buildHelpText(Some('s'), "suffix", required = true, "the table suffix (attr_idx, st_idx, or records)")
    val help = if (args.contains("create")) {
      "Create a feature in GeoMesa\n" + usernameHelp + passwordHelp +
        buildHelpText(Some('c'), "catalog", required = true,
          "the name of the Accumulo table to use -- or create, if it does not already exist -- to contain the new data") +
        buildHelpText(Some('f'), "feature-name", required = true, "the name of the new feature to be created") +
        specHelp +
        buildHelpText(Some('d'), "dt-field", required = false, "the default date attribute of the sft") +
        instanceNameHelp + zookeeperHelp + visibilitiesHelp + authsHelp
    } else if (args.contains("delete")) {
      "Delete a feature from the specified Catalog Table in GeoMesa\n" + usernameHelp + passwordHelp + catalogHelp +
        buildHelpText(Some('f'), "feature-name", required = true, "the name of the new feature to be delete") +
        buildHelpText(None, "force", required = false, "force-delete a table and skip confirmation prompt") +
        instanceNameHelp + zookeeperHelp + visibilitiesHelp + authsHelp
    } else if (args.contains("describe") && !args.contains("tableconf")) {
      "Describe the attributes of a specified feature\n" + usernameHelp + passwordHelp + catalogHelp +
        buildHelpText(Some('f'), "feature-name", required = true, "the name of the feature to be described") +
        buildHelpText(Some('q'), "quiet", required = false,
          "get output from the command without any info statements. useful for piping output") +
        instanceNameHelp + zookeeperHelp + visibilitiesHelp + authsHelp
    } else if (args.contains("explain")) {
      "Explain and plan a query in GeoMesa\n" + usernameHelp + passwordHelp + catalogHelp + featureHelp +
        buildHelpText(Some('q'), "filter", required = true, "the filter string to apply, plan, and explain") +
        instanceNameHelp + zookeeperHelp + visibilitiesHelp + authsHelp
    } else if (args.contains("list") && !args.contains("tableconf")) {
      "List the features in the specified Catalog Table\n" + usernameHelp + passwordHelp + catalogHelp +
        buildHelpText(Some('q'), "quiet", required = false,
          "get output from the command without any info statements. useful for piping output") +
        instanceNameHelp + zookeeperHelp + visibilitiesHelp + authsHelp
    } else if (args.contains("tableconf") && args.contains("list")) {
      "List all table configuration parameters.\n" + usernameHelp + passwordHelp + catalogHelp + featureHelp + suffixHelp +
      instanceNameHelp + zookeeperHelp + visibilitiesHelp + authsHelp
    } else if (args.contains("tableconf") && args.contains("describe")) {
      "Print the value of a single table configuration parameter\n" + usernameHelp + passwordHelp + catalogHelp + featureHelp +
        buildHelpText(None, "param", required = true, "the table configuration parameter to describe") +
        suffixHelp +
        instanceNameHelp + zookeeperHelp + visibilitiesHelp + authsHelp
    } else if (args.contains("tableconf") && args.contains( "update")) {
      "Update a table configuration parameter to the new specified value\n" + usernameHelp + passwordHelp + catalogHelp +
        buildHelpText(None, "param", required = true, "the table configuration parameter to update") +
        buildHelpText(Some('n'), "new-value", required = true, "the new value for the table configuration parameter") +
        suffixHelp +
        instanceNameHelp + zookeeperHelp + visibilitiesHelp + authsHelp
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
        "\tingest: Ingest features into GeoMesa\n" +
        "\tlist: List the features in the specified Catalog Table\n" +
        "\ttableconf: List, describe, and update table configuration parameters"
    }
    logger.info(s"$help")
  }

  if (args.contains("help") || args.contains("--help") || args.contains("-help")) {
    printHelp()
  } else {
    parser.parse(args, FeatureArguments()).map(config => {
      val pw = password(config.password)
      config.mode match {
        case "list" =>
          val ft = new FeaturesTool(config, pw)
          ft.listFeatures()
        case "describe" =>
          val ft = new FeaturesTool(config, pw)
          ft.describeFeature()
        case "explain" =>
          val ft = new FeaturesTool(config, pw)
          ft.explainQuery()
        case "delete" =>
          val ft = new FeaturesTool(config, pw)
          ft.deleteFeature()
        case "create" =>
          val ft = new FeaturesTool(config, pw)
          ft.createFeature()
        case "tableconf" =>
          val tt = new TableTools(config, pw)
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
