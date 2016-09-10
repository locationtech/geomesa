/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.ConverterConfigLoader
import org.locationtech.geomesa.tools.accumulo.commands.EnvironmentCommand.EnvironmentParameters
import org.locationtech.geomesa.tools.common.commands.Command
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureTypeLoader, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class EnvironmentCommand(parent: JCommander) extends Command(parent) with LazyLogging {
  override val command = "env"
  override val params = new EnvironmentParameters()

  // TODO accumulo environment?
  override def execute(): Unit = {
    if (params.sfts == null && params.converters == null && !params.listSfts &&
      !params.listConverters && !params.describeSfts && !params.describeConverters) {
      // default - print out parameters
      println
      parent.usage(command)
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
    println("\nSimple Feature Types:")
    if (filtered.isEmpty) {
      println("None available")
    } else {
      val paramsLower = params.format.toLowerCase
      if (paramsLower == "typesafe" || paramsLower == "spec") {
        val formatFn = paramsLower match {
          case "typesafe" =>
            (sft: SimpleFeatureType) => s"${SimpleFeatureTypes.toConfigString(sft, !params.excludeUserData, params.concise)}"
          case "spec" =>
            (sft: SimpleFeatureType) => s"${SimpleFeatureTypes.encodeType(sft, !params.excludeUserData)}"
        }
        filtered.sortBy(_.getTypeName).map(s => s"${s.getTypeName} = ${formatFn(s)}").foreach(println)
      } else {
        logger.error(s"Unknown config format: ${params.format}")
      }
    }
  }

  def listConverters(names: List[String] = List.empty): Unit = {
    val all = ConverterConfigLoader.confs
    val filtered = if (names.isEmpty) all else names.flatMap(n => all.find(_._1 == n))
    println("\nSimple Feature Type Converters:")
    if (filtered.isEmpty) {
      println("None available")
    } else {
      val options = ConfigRenderOptions.defaults().setJson(false).setOriginComments(false)
      def render(c: Config) = c.root().render(options)
      filtered.map { case (name, conf)=> s"converter-name=$name\n${render(conf)}\n"}.toArray.sortBy(_.self).foreach(println)
    }
  }

  def listSftsNames(): Unit = {
    println("\nSimple Feature Types:")
    val all = SimpleFeatureTypeLoader.sfts
    all.sortBy(_.getTypeName).map(s => s"${s.getTypeName}").foreach(println)
  }
  def listConverterNames(): Unit = {
    println("\nSimple Feature Type Converters:")
    val all = ConverterConfigLoader.confs
    all.map { case (name, conf) => s"$name"}.toArray.sortBy(_.self).foreach(println)
  }
}

object EnvironmentCommand {
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
}
