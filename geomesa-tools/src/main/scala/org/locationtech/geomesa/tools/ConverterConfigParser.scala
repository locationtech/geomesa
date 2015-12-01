/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools

import java.io.File

import com.beust.jcommander.ParameterException
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.Logging

import scala.util.{Failure, Success, Try}

/**
 * Attempts to parse Converter config from arguments as either a string or
 * as a filename containing the converter config
 */
object ConverterConfigParser extends Logging {
  type ConfigParser = String => Option[Config]

  /**
   * @throws ParameterException if the config cannot be parsed
   * @return the converter config parsed from the args
   */
  @throws[ParameterException]
  def getConfig(configArg: String): Config =
    Seq(parseString, parseFile)
      .view.flatMap(_(configArg))
      .headOption
      .getOrElse(throw new ParameterException(s"Unable to parse Converter config from argument $configArg"))

  private[ConverterConfigParser] val parseString: ConfigParser = (configArg: String) =>
    Try(ConfigFactory.parseString(configArg)) match {
      case Success(config) => Some(config)
      case Failure(ex) =>
        logger.debug(s"Unable to parse config from string $configArg")
        None
    }

  private[ConverterConfigParser] val parseFile: ConfigParser = (configArg: String) =>
    Try(ConfigFactory.parseFile(new File(configArg))) match {
      case Success(config) => Some(config)
      case Failure(ex) =>
        logger.debug(s"Unable to parse config from file $configArg")
        None
    }
}