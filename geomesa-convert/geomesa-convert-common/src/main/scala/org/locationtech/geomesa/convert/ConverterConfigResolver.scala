/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

/**
 * Attempts to resolve Converter config from arguments as either a string or
 * as a filename containing the converter config
 */
object ConverterConfigResolver extends LazyLogging {

  // Important to setAllowMissing to false bc else you'll get a config but it will be empty
  val parseOpts =
    ConfigParseOptions.defaults()
      .setAllowMissing(false)
      .setClassLoader(null)
      .setIncluder(null)
      .setOriginDescription(null)
      .setSyntax(null)

  /**
   * @return the converter config parsed from the args
   */
  def getConfig(configArg: String): Option[Config] =
    getLoadedConf(configArg)
      .orElse(parseFile(configArg))
      .orElse(parseString(configArg))

  private[ConverterConfigResolver] def getLoadedConf(configArg: String): Option[Config] = {
    val ret = ConverterConfigLoader.confs.find(_._1 == configArg).map(_._2)
    ret
  }

  private[ConverterConfigResolver] def parseString(configArg: String): Option[Config] =
    Try {
      val confs = SimpleConverterConfigParser.parseConf(ConfigFactory.parseString(configArg, parseOpts))
      if (confs.size > 1) logger.warn(s"Found more than one SFT conf in arg '$configArg'")
      confs.values.head
    } match {
      case Success(config) => Some(config)
      case Failure(ex) =>
        logger.debug(s"Unable to parse config from string $configArg")
        None
    }

  private[ConverterConfigResolver] def parseFile(configArg: String): Option[Config] =
    Try {
      val confs = SimpleConverterConfigParser.parseConf(ConfigFactory.parseFile(new File(configArg), parseOpts))
      if (confs.size > 1) logger.warn(s"Found more than one SFT conf in arg '$configArg'")
      confs.values.head
    } match {
      case Success(config) => Some(config)
      case Failure(ex) =>
        logger.debug(s"Unable to parse config from file $configArg")
        None
    }

}