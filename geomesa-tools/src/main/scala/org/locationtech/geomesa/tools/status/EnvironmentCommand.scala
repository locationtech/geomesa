/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.status

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.config.{Config, ConfigRenderOptions}
import org.locationtech.geomesa.convert.ConverterConfigLoader
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureTypeLoader, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeatureType

class EnvironmentCommand extends Command {

  override val name = "env"
  override val params = new EnvironmentParameters()
  // TODO accumulo environment?
  override def execute(): Unit = {
    import scala.collection.JavaConversions._

    if (params.sfts == null && params.converters == null && !params.listSfts &&
      !params.listConverters && !params.describeSfts && !params.describeConverters) {
      Command.output.info("No flags given; displaying list of SFTs and Converters.")
      Command.output.info(s"Use 'help $name' to see complete command options.")

      listSftsNames()
      listConverterNames()
    } else {
      if (params.listSfts) {
        listSftsNames()
      }
      if (params.listConverters){
        listConverterNames()
      }
      if (params.describeSfts) {
        listSfts()
      }
      if (params.describeConverters) {
        listConverters()
      }
      if (params.sfts != null) {
        listSfts(params.sfts.toList)
      }
      if (params.converters != null) {
        listConverters(params.converters.toList)
      }
    }
  }

  def listSfts(names: List[String] = List.empty): Unit = {
    val all = SimpleFeatureTypeLoader.sfts
    val filtered = if (names.isEmpty) all else names.flatMap(n => all.find(_.getTypeName == n))
    Command.output.info("Simple Feature Types:")
    if (filtered.isEmpty) {
      Command.output.info("None available")
    } else {
      val paramsLower = params.format.toLowerCase
      if (paramsLower == "typesafe" || paramsLower == "spec") {
        val formatFn = paramsLower match {
          case "typesafe" =>
            (sft: SimpleFeatureType) => s"${SimpleFeatureTypes.toConfigString(sft, !params.excludeUserData, params.concise)}"
          case "spec" =>
            (sft: SimpleFeatureType) => s"${SimpleFeatureTypes.encodeType(sft, !params.excludeUserData)}"
        }
        filtered.sortBy(_.getTypeName).map(s => s"${s.getTypeName} = ${formatFn(s)}").foreach(Command.output.info)
      } else {
        throw new ParameterException(s"Unknown format '${params.format}'. Valid values are 'typesafe' or 'spec'")
      }
    }
  }

  def listConverters(names: List[String] = List.empty): Unit = {
    val all = ConverterConfigLoader.confs
    val filtered = if (names.isEmpty) all else names.flatMap(n => all.find(_._1 == n))
    Command.output.info("Simple Feature Type Converters:")
    if (filtered.isEmpty) {
      Command.output.info("None available")
    } else {
      val options = ConfigRenderOptions.defaults().setJson(false).setOriginComments(false)
      def render(c: Config) = c.root().render(options)
      val strings = filtered.map { case (cname, conf)=> s"converter-name=$cname\n${render(conf)}\n" }
      strings.toArray.sorted.foreach(Command.output.info)
    }
  }

  def listSftsNames(): Unit = {
    Command.output.info("Simple Feature Types:")
    val all = SimpleFeatureTypeLoader.sfts
    all.sortBy(_.getTypeName).map(s => s"${s.getTypeName}").foreach(Command.output.info)
  }
  def listConverterNames(): Unit = {
    Command.output.info("Simple Feature Type Converters:")
    val all = ConverterConfigLoader.confs
    all.map { case (cname, conf) => s"$cname"}.toArray.sorted.foreach(Command.output.info)
  }
}

@Parameters(commandDescription = "Examine the current GeoMesa environment")
class EnvironmentParameters {
  @Parameter(names = Array("-s", "--sfts"), description = "Describe specific simple feature types", variableArity = true)
  var sfts: java.util.List[String] = null

  @Parameter(names = Array("-c", "--converters"), description = "Describe specific GeoMesa converters", variableArity = true)
  var converters: java.util.List[String] = null

  @Parameter(names = Array("--list-sfts"), description = "List all the Simple Feature Types")
  var listSfts: Boolean = false

  @Parameter(names = Array("--list-converters"), description = "List all the Converter Names")
  var listConverters: Boolean = false

  @Parameter(names = Array("--describe-sfts"), description = "Describe all the Simple Feature Types")
  var describeSfts: Boolean = false

  @Parameter(names = Array("--describe-converters"), description = "Describe all the Simple Feature Type Converters")
  var describeConverters: Boolean = false

  @Parameter(names = Array("--format"), description = "Formats for sft (comma separated string, allowed values are typesafe, spec)", required = false)
  var format: String = "typesafe"

  @Parameter(names = Array("--concise"), description = "Render in concise format", required = false)
  var concise: Boolean = false

  @Parameter(names = Array("--exclude-user-data"), description = "Exclude user data", required = false)
  var excludeUserData: Boolean = false
}
