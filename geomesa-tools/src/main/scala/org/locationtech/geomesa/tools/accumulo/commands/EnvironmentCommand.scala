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

import scala.collection.JavaConversions._

class EnvironmentCommand(parent: JCommander) extends Command(parent) with LazyLogging {
  override val command = "env"
  override val params = new EnvironmentParameters()

  // TODO accumulo environment?
  override def execute(): Unit = {
    if (params.sfts == null && params.converters == null) {
      // default - list all
      listSfts()
      println
      listConverters()
    } else if (params.sfts != null) {
      // only list specified
      listSfts(params.sfts.toList)
      if (params.converters != null) {
        println
        listConverters(params.converters.toList)
      }
    } else {
      listConverters(params.converters.toList)
    }
  }

  def listSfts(names: List[String] = List.empty): Unit = {
    val all = SimpleFeatureTypeLoader.sfts
    val filtered = if (names.isEmpty) all else names.flatMap(n => all.find(_.getTypeName == n))
    println("Simple Feature Types:")
    if (filtered.isEmpty) {
      println("\tNone available")
    } else {
      filtered.map(s => s"\t${s.getTypeName} = ${SimpleFeatureTypes.encodeType(s)}").foreach(println)
    }
  }

  def listConverters(names: List[String] = List.empty): Unit = {
    val all = ConverterConfigLoader.confs
    val filtered = if (names.isEmpty) all else names.flatMap(n => all.find(_._1 == n))
    println("Simple Feature Type Converters:")
    if (filtered.isEmpty) {
      println("\tNone available")
    } else {
      val options = ConfigRenderOptions.defaults().setJson(false).setOriginComments(false)
      def render(c: Config) = c.root().render(options).replaceAll("\n", "\n\t")
      filtered.map { case (name, conf)=> s"\t$name\n\t${render(conf)}\n"}.foreach(println)
    }
  }
}

object EnvironmentCommand {
  @Parameters(commandDescription = "Examine the current GeoMesa environment")
  class EnvironmentParameters {
    @Parameter(names = Array("-s", "--sfts"), description = "Examine simple feature types", variableArity = true)
    var sfts: java.util.List[String] = null

    @Parameter(names = Array("-c", "--converters"), description = "Examine GeoMesa converters", variableArity = true)
    var converters: java.util.List[String] = null
  }
}
