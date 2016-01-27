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
import com.typesafe.config.{ConfigFactory, ConfigParseOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureTypeLoader, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.{Failure, Success, Try}

/**
 * Parsers SimpleFeatureType specification from a variety of arguments
 * including sft strings (e.g. name:String,age:Integer,*geom:Point)
 * and typesafe config.
 */
object SftArgParser extends LazyLogging {

  // Important to setAllowMissing to false bc else you'll get a config but it will be empty
  val parseOpts =
    ConfigParseOptions.defaults()
      .setAllowMissing(false)
      .setClassLoader(null)
      .setIncluder(null)
      .setOriginDescription(null)
      .setSyntax(null)
  /**
   * @throws ParameterException if the SFT cannot be parsed
   * @return the SFT parsed from the Args
   */
  @throws[ParameterException]
  def getSft(specArg: String, featureName: String = null): SimpleFeatureType =
    getLoadedSft(specArg, featureName)
        .orElse(parseSpecString(specArg, featureName))
        .orElse(parseSpecConf(specArg, featureName))
        .orElse(parseSpecStringFile(specArg, featureName))
        .orElse(parseSpecConfFile(specArg, featureName))
        .getOrElse {
          throw new ParameterException("Unable to parse Simple Feature type from sft config or string")
        }

  // gets an sft from simple feature type providers on the classpath
  private[SftArgParser] def getLoadedSft(specArg: String, name: String): Option[SimpleFeatureType] = {
    SimpleFeatureTypeLoader.sfts.find(_.getTypeName == specArg).map { sft =>
      if (name == null || name == sft.getTypeName) sft else SimpleFeatureTypes.renameSft(sft, name)
    }
  }

  // gets an sft based on a spec string
  private[SftArgParser] def parseSpecString(specArg: String, name: String): Option[SimpleFeatureType] =
    Option(name).flatMap { featureName =>
      Try(SimpleFeatureTypes.createType(featureName, specArg)) match {
        case Success(sft) => Some(sft)
        case Failure(e) =>
          logger.debug(s"Unable to parse sft spec from string $specArg with error ${e.getMessage}")
          None
      }
    }

  // gets an sft based on a spec string
  private[SftArgParser] def parseSpecStringFile(specArg: String, name: String): Option[SimpleFeatureType] =
    Option(specArg).map(new File(_)).flatMap { file =>
      Try(SimpleFeatureTypes.createType (name, FileUtils.readFileToString(file))) match {
        case Success(sft) => Some(sft)
        case Failure(e) =>
          logger.debug(s"Unable to parse sft spec from string $specArg with error ${e.getMessage}")
          None
      }
    }

  // gets an sft based on a spec conf string
  private[SftArgParser] def parseSpecConf(specArg: String, name: String): Option[SimpleFeatureType] = {
    Try(SimpleFeatureTypes.createType(ConfigFactory.parseString(specArg, parseOpts))) match {
      case Success(sft) if name == null || name == sft.getTypeName => Some(sft)
      case Success(sft) => Some(SimpleFeatureTypes.renameSft(sft, name))
      case Failure(e) =>
        logger.debug(s"Unable to parse sft spec from string $specArg as conf with error ${e.getMessage}")
        None
    }
  }

  // parse spec conf file
  private[SftArgParser] def parseSpecConfFile(specArg: String, name: String): Option[SimpleFeatureType] = {
    Try(SimpleFeatureTypes.createType(ConfigFactory.parseFile(new File(specArg)))) match {
      case Success(sft) if name == null || name == sft.getTypeName => Some(sft)
      case Success(sft) => Some(SimpleFeatureTypes.renameSft(sft, name))
      case Failure(e) =>
        logger.debug(s"Unable to parse sft spec from file $specArg as conf with error ${e.getMessage}")
        None
    }
  }

}
