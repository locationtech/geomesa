/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.conf.ArgResolver

import scala.util.{Failure, Success, Try}

/**
 * Attempts to resolve Converter config from arguments as either a string or
 * as a filename containing the converter config
 */
object ConverterConfigResolver extends ArgResolver[Config, ConfArgs] with LazyLogging {

  import ArgTypes._

  override def argType(args: ConfArgs) = {
    /*
     * Here we use rudimentary checking to guess as what kind of configArg was passed in.
     * We use this to decide which error message to display to the user, since the
     * parsers fail frequently. The rest of the errors are logged.
     */
    val fileNameReg = """([^.]*)\.([^.]*)""" // e.g. "foo.bar"

    args.config match {
      // Order is important here
      case s if s.contains("geomesa{")
             || s.contains("geomesa {")
             || s.contains("geomesa.sfts")  => CONFSTR
      case s if s.matches(fileNameReg)
             || s.contains("/")             => PATH
      case _                                => NAME
    }
  }

  override val parseMethodList = List[ConfArgs => ResEither](
    getLoadedConf,
    parseFile,
    parseString
  )

  private[ConverterConfigResolver] def getLoadedConf(args: ConfArgs): ResEither = {
    ConverterConfigLoader.confs.find(_._1 == args.config).map(_._2) match {
      case Some(conf) => Right(conf)
      case None => Left((s"Unable to get loaded conf ${args.config}",
        new Throwable(s"${args.config} was not found in the loaded confs"), NAME))
    }
  }

  private[ConverterConfigResolver] def parseString(args: ConfArgs): ResEither =
    Try {
      val confs = SimpleConverterConfigParser.parseConf(ConfigFactory.parseString(args.config, parseOpts))
      if (confs.size > 1) logger.warn(s"Found more than one SFT conf in arg '${args.config}'")
      confs.values.head
    } match {
      case Success(config) => Right(config)
      case Failure(e) => Left((s"Unable to parse config from string ${args.config}", e, CONFSTR))
    }

  private[ConverterConfigResolver] def parseFile(args: ConfArgs): ResEither =
    Try {
      val confs = SimpleConverterConfigParser.parseConf(ConfigFactory.parseFile(new File(args.config), parseOpts))
      if (confs.size > 1) logger.warn(s"Found more than one SFT conf in arg '${args.config}'")
      confs.values.head
    } match {
      case Success(config) => Right(config)
      case Failure(e) => Left((s"Unable to parse config from file ${args.config}", e, PATH))
    }
}

case class ConfArgs(config: String)
