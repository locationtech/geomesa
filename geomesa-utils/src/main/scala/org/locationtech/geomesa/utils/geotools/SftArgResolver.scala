/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.io.File

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.locationtech.geomesa.utils.conf.AbstractArgResolver
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.{Failure, Success, Try}

/**
 * Resolves SimpleFeatureType specification from a variety of arguments
 * including sft strings (e.g. name:String,age:Integer,*geom:Point)
 * and typesafe config.
 */
object SftArgResolver extends AbstractArgResolver[SimpleFeatureType, (String, String)] with LazyLogging {

  import ArgTypes._

  override def argType(args: argTuple) = {
    /**
     * Here we use rudimentary checking to guess as what kind of specArg was passed in.
     * We use this to decide which error message to display to the user, since the
     * parsers fail frequently. The rest of the errors are logged.
     */
    val fileNameReg = """([^.]*)\.([^.]*)""" // e.g. "foo.bar"
    val specStrReg = """^[a-zA-Z0-9]+[:][String|Integer|Double|Point|Date|Map|List].*""" // e.g. "foo:String..."
    val specStrRegWError = """^[a-zA-Z0-9]+[:][a-zA-Z0-9]+.*""" // e.g. "foo:Sbartring..."
    args._1 match {
      // Order is important here
      case s if s.contains("geomesa{")
             || s.contains("geomesa {")
             || s.contains("geomesa.sfts")  => CONFSTR
      case s if s.matches(specStrReg)       => SPECSTR
      case s if s.matches(fileNameReg)
             || s.contains("/")             => PATH
      case s if s.matches(specStrRegWError) => SPECSTR
      case _                                => NAME
    }
  }

  override def parseOption(args: argTuple): Option[SimpleFeatureType] = getLoadedSft(args)

  override val parseMethodList = List[argTuple => resEither](
    parseSpecString,
    parseConfStr,
    parseSpecStringFile,
    parseConfFile
  )

  // gets an sft from simple feature type providers on the classpath
  private[SftArgResolver] def getLoadedSft(args: argTuple): Option[SimpleFeatureType] = {
    SimpleFeatureTypeLoader.sfts.find(_.getTypeName == args._1).map { sft =>
      if (args._2 == null || args._2 == sft.getTypeName) sft else SimpleFeatureTypes.renameSft(sft, args._2)
    }
  }

  // gets an sft based on a spec string
  private[SftArgResolver] def parseSpecString(args: argTuple): resEither = {
    Try(SimpleFeatureTypes.createType(
        Option(args._2).getOrElse(throw new Throwable("Null feature name provided. " +
          "Unable to parse spec string from null value.")),
      args._1)
    ) match {
      case Success(sft) => Right(sft)
      case Failure(e) => Left((s"Unable to parse sft spec from string ${args._1}.", e, SPECSTR))
    }
  }

  // gets an sft based on a spec string
  private[SftArgResolver] def parseSpecStringFile(args: argTuple): resEither =
    Try(SimpleFeatureTypes.createType(
      Option(args._2).getOrElse(throw new Throwable("Null feature name provided. " +
        "Unable to parse spec string from null value.")),
      FileUtils.readFileToString(new File(Option(args._1).getOrElse(""))))
    ) match {
      case Success(sft) => Right(sft)
      case Failure(e) => Left((s"Unable to parse sft spec from string ${args._1} with error ${e.getMessage}", e, PATH))
    }

  private[SftArgResolver] def parseConf(configStr: String, name: String): Either[Throwable, SimpleFeatureType] = {
    Try {
      val sfts = SimpleSftParser.parseConf(ConfigFactory.parseString(configStr, parseOpts))
      if (sfts.size > 1) logger.warn(s"Found more than one SFT conf in arg '$configStr'")
      sfts.get(0)
    } match {
      case Success(sft) if name == null || name == sft.getTypeName => Right(sft)
      case Success(sft) => Right(SimpleFeatureTypes.renameSft(sft, name))
      case Failure(e) => Left(e)
    }
  }

  // gets an sft based on a spec conf string
  private[SftArgResolver] def parseConfStr(args: argTuple): resEither =
    parseConf(args._1, args._2) match {
      case Right(sftOpt) => Right(sftOpt)
      case Left(e) => Left((s"Unable to parse sft spec from string ${args._1} as conf.", e, CONFSTR))
    }

  // parse spec conf file
  private[SftArgResolver] def parseConfFile(args: argTuple): resEither =
    Try {
      FileUtils.readFileToString(new File(args._1))
    } match {
      case Success(file) => parseConf(file, args._2) match {
          case Right(sftOpt) => Right(sftOpt)
          case Left(e) => Left((s"Unable to parse sft spec from file ${args._1} as conf.", e, PATH))
        }
      case Failure(e) => Left((s"Unable to parse sft spec as filename ${args._1}.", e, PATH))
    }

}
