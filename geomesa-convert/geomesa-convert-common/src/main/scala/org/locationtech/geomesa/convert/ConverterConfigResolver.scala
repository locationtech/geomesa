/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.conf.ArgResolver
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}

import scala.util.control.NonFatal

/**
 * Attempts to resolve Converter config from arguments as either a string or
 * as a filename containing the converter config
 */
object ConverterConfigResolver extends ArgResolver[Config, ConfArgs] with LazyLogging {

  import org.locationtech.geomesa.utils.conf.ArgResolver.ArgTypes._

  private val fileNameReg = """([^.]*)\.([^.]*)""" // e.g. "foo.bar"
  private val confStrings = Seq("geomesa{", "geomesa {", "geomesa.converters")

  override def argType(args: ConfArgs): ArgTypes = {
    // guess the type we are trying to parse, to determine which error we show for failures
    // order is important here
    if (confStrings.exists(args.config.contains)) {
      CONFSTR
    } else if (args.config.matches(fileNameReg) || args.config.contains("/")) {
      PATH
    } else {
      NAME
    }
  }

  override val parseMethodList: List[ConfArgs => ResEither] = List[ConfArgs => ResEither](
    getLoadedConf,
    parseFile,
    parseString
  )

  private [ConverterConfigResolver] def getLoadedConf(args: ConfArgs): ResEither = {
    ConverterConfigLoader.confs.find(_._1 == args.config).map(_._2) match {
      case Some(conf) => Right(conf)
      case None => Left((s"Unable to get loaded conf ${args.config}",
        new RuntimeException(s"${args.config} was not found in the loaded confs"), NAME))
    }
  }

  private [ConverterConfigResolver] def parseString(args: ConfArgs): ResEither = {
    try {
      val confs = SimpleConverterConfigParser.parseConf(ConfigFactory.parseString(args.config, parseOpts).resolve())
      if (confs.size > 1) {
        logger.warn(s"Found more than one SFT conf in arg '${args.config}'")
      }
      Right(confs.values.head)
    } catch {
      case NonFatal(e) => Left((s"Unable to parse config from string ${args.config}", e, CONFSTR))
    }
  }

  private [ConverterConfigResolver] def parseFile(args: ConfArgs): ResEither = {
    try {
      val handle = PathUtils.interpretPath(args.config).headOption.getOrElse {
        throw new RuntimeException(s"Could not read file at ${args.config}")
      }
      WithClose(handle.open) { is =>
        if (is.hasNext) {
          val reader = new InputStreamReader(is.next._2, StandardCharsets.UTF_8)
          val confs = SimpleConverterConfigParser.parseConf(ConfigFactory.parseReader(reader, parseOpts).resolve())
          if (confs.size > 1) {
            logger.warn(s"Found more than one SFT conf in arg '${args.config}'")
          }
          Right(confs.values.head)
        } else {
          throw new RuntimeException(s"Could not read file at ${args.config}")
        }
      }

    } catch {
      case NonFatal(e) => Left((s"Unable to parse config from file ${args.config}", e, PATH))
    }
  }
}

case class ConfArgs(config: String)
