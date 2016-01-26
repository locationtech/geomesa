/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools

import java.io.File

import com.beust.jcommander.ParameterException
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.ConverterConfigLoader

import scala.util.{Failure, Success, Try}

/**
 * Attempts to parse Converter config from arguments as either a string or
 * as a filename containing the converter config
 */
object ConverterConfigParser extends LazyLogging {

  // Important to setAllowMissing to false bc else you'll get a config but it will be empty
  val parseOpts =
    ConfigParseOptions.defaults()
      .setAllowMissing(false)
      .setClassLoader(null)
      .setIncluder(null)
      .setOriginDescription(null)
      .setSyntax(null)

  /**
   * @throws ParameterException if the config cannot be parsed
   * @return the converter config parsed from the args
   */
  @throws[ParameterException]
  def getConfig(configArg: String): Config =
    getLoadedConf(configArg)
      .orElse(parseFile(configArg))
      .orElse(parseString(configArg))
      .getOrElse(throw new ParameterException(s"Unable to parse Converter config from argument $configArg"))

  private[ConverterConfigParser] def getLoadedConf(configArg: String): Option[Config] = {
    val ret = ConverterConfigLoader.confs.find(_._1 == configArg).map(_._2)
    ret
  }

  private[ConverterConfigParser] def parseString(configArg: String): Option[Config] =
    Try(ConfigFactory.parseString(configArg, parseOpts)) match {
      case Success(config) => Some(config)
      case Failure(ex) =>
        logger.debug(s"Unable to parse config from string $configArg")
        None
    }

  private[ConverterConfigParser] def parseFile(configArg: String): Option[Config] =
    Try(ConfigFactory.parseFile(new File(configArg), parseOpts)) match {
      case Success(config) => Some(config)
      case Failure(ex) =>
        logger.debug(s"Unable to parse config from file $configArg")
        None
    }

}