/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.io.File

import com.typesafe.config.{ConfigFactory, ConfigParseOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.opengis.feature.simple.SimpleFeatureType

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
 * Resolves SimpleFeatureType specification from a variety of arguments
 * including sft strings (e.g. name:String,age:Integer,*geom:Point)
 * and typesafe config.
 */
object SftArgResolver extends LazyLogging {

  // Important to setAllowMissing to false bc else you'll get a config but it will be empty
  val parseOpts =
    ConfigParseOptions.defaults()
      .setAllowMissing(false)
      .setClassLoader(null)
      .setIncluder(null)
      .setOriginDescription(null)
      .setSyntax(null)

  object SpecArgTypes extends Enumeration {
    val  NAME, SPECSTR, CONFSTR, PATH = Value
  }
  import SpecArgTypes._
  type sftEither = Either[(String, Throwable, Value), SimpleFeatureType]

  /**
   * @return the SFT parsed from the Args
   */
  def getSft(specArg: String, featureName: String = null): Either[Throwable, SimpleFeatureType] = {
    /**
     * Here we use rudimentary checking to guess as what kind of specArg was passed in.
     * We use this to decide which error message to display to the user, since the
     * parsers fail frequently. The rest of the errors are logged.
     */
    val fileNameReg = """([^.]*)\.([^.]*)""" // e.g. "foo.bar"
    val specStrReg = """^[a-zA-Z0-9]+[:][String|Integer|Double|Point|Date|Map|List].*""" // e.g. "foo:String..."
    val specStrRegWError = """^[a-zA-Z0-9]+[:][a-zA-Z0-9]+.*""" // e.g. "foo:Sbartring..."
    lazy val specArgType = specArg match {
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

    // Object for holding relevant error to return
    object ErrorData {
      var message: String = _
      var error: Throwable = _
      def apply(msg: String, e: Throwable) = { message = msg; error = e }
    }

    /**
     * Recursively run through parseMethodList attempting to parse the sft.
     * The most relevant error message is saved in ErrorData and all error are sent to log.
     * ErrorData is sent back to the CLArgResolver for display to user.
     */
    @tailrec
    def parseMethods(specArg: String,
               featureName: String,
               tryMethod: (String, String) => sftEither = null,
               methodArray: List[(String, String) => sftEither] = null): SimpleFeatureType = {
      if (tryMethod == null) parseMethods(specArg, featureName, methodArray.head, methodArray.drop(1))
      else tryMethod(specArg, featureName) match {
          case Right(sft) => sft
          case Left((msg, error, value)) =>
            //logger.debug(msg, error)
            logger.info(msg, error)
            if (specArgType == value) { ErrorData(msg, error) }
            if (methodArray == null) null
            else parseMethods(specArg, featureName, methodArray.head, methodArray.drop(1))
        }
    }

    val parseMethodList = List[(String, String) => sftEither](
      parseSpecString,
      parseConfStr,
      parseSpecStringFile,
      parseConfFile
    )

    val sft = getLoadedSft(specArg, featureName).getOrElse(
      parseMethods(specArg, featureName, null, parseMethodList)
    )

    if (sft != null) Right(sft)
    else {
      val e = new Throwable(ErrorData.message + "\n" + ErrorData.error.getMessage, ErrorData.error)
      e.setStackTrace(ErrorData.error.getStackTrace)
      Left(e)
    }
  }

  // gets an sft from simple feature type providers on the classpath
  private[SftArgResolver] def getLoadedSft(specArg: String, name: String): Option[SimpleFeatureType] = {
    SimpleFeatureTypeLoader.sfts.find(_.getTypeName == specArg).map { sft =>
      if (name == null || name == sft.getTypeName) sft else SimpleFeatureTypes.renameSft(sft, name)
    }
  }

  // gets an sft based on a spec string
  private[SftArgResolver] def parseSpecString(specArg: String, name: String): sftEither = {
    Try(SimpleFeatureTypes.createType(
        Option(name).getOrElse(throw new Throwable("Null feature name provided. " +
          "Unable to parse spec string from null value.")),
        specArg)
    ) match {
      case Success(sft) => Right(sft)
      case Failure(e) => Left((s"Unable to parse sft spec from string $specArg.", e, SPECSTR))
    }
  }

  // gets an sft based on a spec string
  private[SftArgResolver] def parseSpecStringFile(specArg: String, name: String): sftEither =
    Try(SimpleFeatureTypes.createType(
      Option(name).getOrElse(throw new Throwable("Null feature name provided. " +
        "Unable to parse spec string from null value.")),
      FileUtils.readFileToString(new File(Option(specArg).getOrElse(""))))
    ) match {
      case Success(sft) => Right(sft)
      case Failure(e) => Left((s"Unable to parse sft spec from string $specArg with error ${e.getMessage}", e, PATH))
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
  private[SftArgResolver] def parseConfStr(specArg: String, name: String): sftEither =
    parseConf(specArg, name) match {
      case Right(sftOpt) => Right(sftOpt)
      case Left(e) => Left((s"Unable to parse sft spec from string $specArg as conf.", e, CONFSTR))
    }

  // parse spec conf file
  private[SftArgResolver] def parseConfFile(specArg: String, name: String): sftEither =
    parseConf(FileUtils.readFileToString(new File(specArg)), name) match {
      case Right(sftOpt) => Right(sftOpt)
      case Left(e) => Left((s"Unable to parse sft spec from file $specArg as conf.", e, PATH))
    }
}
