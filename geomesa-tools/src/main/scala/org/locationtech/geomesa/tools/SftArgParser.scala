/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}

import com.beust.jcommander.ParameterException
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.{Failure, Success, Try}

/**
 * Parsers SimpleFeatureType specification from a variety of arguments
 * including sft strings (e.g. name:String,age:Integer,*geom:Point)
 * and typesafe config. Will handle string arguments or filename args.
 */
object SftArgParser extends Logging {
  type SpecParser = () => Option[SimpleFeatureType]

  /**
   * @throws ParameterException if the SFT cannot be parsed
   * @return the SFT parsed from the Args
   */
  @throws[ParameterException]
  def getSft(specArg: String, featureName: String = null, convertArg: String = null): SimpleFeatureType = {
    val configParsers =
      Seq(Option(specArg), Option(convertArg))
        .flatten
        .flatMap(s => List(readFile(s),Some(s)).flatten)
        .map(s => getConfParser(s))

    (configParsers ++ Seq(getSpecParser(specArg, Option(featureName))))
      .view.map(_())
      .find(_.nonEmpty)
      .getOrElse(throw new ParameterException("Unable to parse Simple Feature type from sft config or string"))
      .get
  }

  private[SftArgParser] def getSpecParser (specArg: String, nameOpt: Option[String]) : SpecParser = () => {
    nameOpt.map[Option[SimpleFeatureType]] { featureName =>
        Try { SimpleFeatureTypes.createType(featureName, specArg) }
        match {
          case Success(sft) => Some(sft)
          case Failure(ex)  =>
            logger.debug(s"Unable to parse sft spec from string $specArg with error ${ex.getMessage}")
            None
        }
      }.getOrElse(Option.empty)
  }

  private[SftArgParser] def getConfParser(str: String): SpecParser = () =>
    Try { SimpleFeatureTypes.createType(ConfigFactory.parseString(str)) }
    match {
      case Success(sft) => Some(sft)
      case Failure(ex)  =>
        logger.debug(s"Unable to parse sft conf from string $str with error ${ex.getMessage}")
        Option.empty
    }

  def readFile(s: String): Option[String] = {
    val f = new File(s)
    if (f.exists && f.canRead && f.isFile) {
      val reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)))
      val contents = try {
        IOUtils.toString(reader)
      } finally {
        reader.close()
      }
      Some(contents)
    } else None
  }

}