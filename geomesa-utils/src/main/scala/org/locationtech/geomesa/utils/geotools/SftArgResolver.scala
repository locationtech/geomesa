/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.io.{File, InputStreamReader, Reader, StringReader}
import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.locationtech.geomesa.utils.conf.ArgResolver
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

/**
 * Resolves SimpleFeatureType specification from a variety of arguments
 * including sft strings (e.g. name:String,age:Integer,*geom:Point)
 * and typesafe config.
 */
object SftArgResolver extends ArgResolver[SimpleFeatureType, SftArgs] with LazyLogging {

  import org.locationtech.geomesa.utils.conf.ArgResolver.ArgTypes._

  private val confStrings = Seq("geomesa{", "geomesa {", "geomesa.sfts")
  private val specStrReg = """^[a-zA-Z0-9]+[:][String|Integer|Double|Point|Date|Map|List].*""" // e.g. "foo:String..."
  private val fileNameReg = """([^.]*)\.([^.]*)""" // e.g. "foo.bar"
  private val specStrRegError = """^[a-zA-Z0-9]+[:][a-zA-Z0-9]+.*""" // e.g. "foo:Sbartring..."

  override def argType(args: SftArgs): ArgTypes = {
    // guess the type we are trying to parse, to determine which error we show for failures
    // order is important here
    if (confStrings.exists(args.spec.contains)) {
      CONFSTR
    } else if (args.spec.matches(specStrReg)) {
      SPECSTR
    } else if (args.spec.matches(fileNameReg) || args.spec.contains("/")) {
      PATH
    } else if (args.spec.matches(specStrRegError)) {
      SPECSTR
    } else {
      NAME
    }
  }

  override val parseMethodList: Seq[SftArgs => ResEither] = Seq[SftArgs => ResEither](
    getLoadedSft,
    parseSpecString,
    parseConfStr,
    parseSpecStringFile,
    parseConfFile
  )

  // gets an sft from simple feature type providers on the classpath
  private [SftArgResolver] def getLoadedSft(args: SftArgs): ResEither = {
    SimpleFeatureTypeLoader.sfts.find(_.getTypeName == args.spec).map { sft =>
      if (args.featureName == null || args.featureName == sft.getTypeName) { sft } else {
        SimpleFeatureTypes.renameSft(sft, args.featureName)
      }
    } match {
      case Some(sft) => Right(sft)
      case None => Left((s"Unable to get loaded SFT using ${args.spec}.",
        new RuntimeException(s"${args.spec} was not found in the loaded SFTs."), NAME))
    }
  }

  // gets an sft based on a spec string
  private [SftArgResolver] def parseSpecString(args: SftArgs): ResEither = {
    try {
      val name = Option(args.featureName).getOrElse {
        throw new RuntimeException("Feature name was not provided.")
      }
      Right(SimpleFeatureTypes.createType(name, args.spec))
    } catch {
      case NonFatal(e) => Left((s"Unable to parse sft spec from string ${args.spec}.", e, SPECSTR))
    }
  }

  // gets an sft based on a spec string
  private [SftArgResolver] def parseSpecStringFile(args: SftArgs): ResEither = {
    try {
      val name = Option(args.featureName).getOrElse {
        throw new RuntimeException("Feature name was not provided.")
      }
      val file = Option(args.spec).getOrElse(throw new RuntimeException("No input file specified."))
      val spec = FileUtils.readFileToString(new File(file), StandardCharsets.UTF_8)
      Right(SimpleFeatureTypes.createType(name, spec))
    } catch {
      case NonFatal(e) => Left((s"Unable to parse sft spec from file ${args.spec}.", e, PATH))
    }
  }

  private [SftArgResolver] def parseConf(input: Reader, name: String): Either[Throwable, SimpleFeatureType] = {
    import scala.collection.JavaConverters._
    try {
      val sfts = SimpleSftParser.parseConf(ConfigFactory.parseReader(input, parseOpts).resolve())
      if (sfts.isEmpty) {
        throw new RuntimeException("No feature types parsed from config string")
      }
      if (name == null) {
        if (sfts.size > 1) {
          logger.warn(s"Found more than one SFT conf in input arg")
        }
        Right(sfts.get(0))
      } else {
        sfts.asScala.find(_.getTypeName == name) match {
          case Some(sft) => Right(sft)
          case None =>
            if (sfts.size > 1) {
              logger.warn("Found more than one SFT conf in input arg")
            }
            Right(SimpleFeatureTypes.renameSft(sfts.get(0), name))
        }
      }
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  // gets an sft based on a spec conf string
  private [SftArgResolver] def parseConfStr(args: SftArgs): ResEither =
    parseConf(new StringReader(args.spec), args.featureName).left.map { e =>
      (s"Unable to parse sft spec from string '${args.spec}' as conf.", e, CONFSTR)
    }

  // parse spec conf file
  private [SftArgResolver] def parseConfFile(args: SftArgs): ResEither = {
    try {
      val handle = PathUtils.interpretPath(args.spec).headOption.getOrElse {
        throw new RuntimeException(s"Could not read file at ${args.spec}")
      }
      WithClose(handle.open) { streams =>
        if (streams.hasNext) {
          val reader = new InputStreamReader(streams.next._2, StandardCharsets.UTF_8)
          parseConf(reader, args.featureName).left.map { e =>
            (s"Unable to parse sft spec from file '${args.spec}'.", e, PATH)
          }
        } else {
          throw new RuntimeException(s"Could not read file at ${args.spec}")
        }
      }
    } catch {
      case NonFatal(e) => Left((s"Unable to load '${args.spec}' as file.", e, PATH))
    }
  }
}

case class SftArgs(spec: String, featureName: String)
