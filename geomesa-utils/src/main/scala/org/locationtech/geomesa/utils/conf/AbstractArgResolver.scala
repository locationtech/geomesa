/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.conf

import com.typesafe.config.ConfigParseOptions
import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec

/**
 * Abstract trait for resolving sft/config parameters and handling errors around
 * parsing.
 */
trait AbstractArgResolver[returnType, parseMethodArgTuple] extends LazyLogging {

  // Object for holding relevant error to return
  object ErrorData {
    var message: String = _
    var error: Throwable = _
    def apply(msg: String, e: Throwable) = { message = msg; error = e }
  }

  // Type Names for arg type guesses
  object ArgTypes extends Enumeration {
    val  NAME, SPECSTR, CONFSTR, PATH = Value
  }
  import ArgTypes._

  type resEither = Either[(String, Throwable, Value), returnType]
  type argTuple = parseMethodArgTuple

  // Important to setAllowMissing to false bc else you'll get a config but it will be empty
  val parseOpts =
    ConfigParseOptions.defaults()
      .setAllowMissing(false)
      .setClassLoader(null)
      .setIncluder(null)
      .setOriginDescription(null)
      .setSyntax(null)

  // Should return a guess of the argType without using the parsers (e.g. regex, contains, etc)
  def argType(args: parseMethodArgTuple): ArgTypes.Value
  // Get the arg from a provider on the cp
  def parseOption(args: parseMethodArgTuple): Option[returnType]
  // (Ordered) List of parse methods to attempt
  def parseMethodList: List[parseMethodArgTuple => resEither]

  /**
   * @return the SFT parsed from the Args
   */
  def getArg(args: parseMethodArgTuple): Either[Throwable, returnType] = {
    val res = parseOption(args).orElse(parseMethods(args, None, parseMethodList))

    if (res.isDefined) Right(res.get)
    else {
      val e = new Throwable(ErrorData.message + "\n" + ErrorData.error.getMessage, ErrorData.error)
      e.setStackTrace(ErrorData.error.getStackTrace)
      Left(e)
    }
  }

  /**
   * Recursively run through parseMethodList attempting to parse the sft.
   * The most relevant error message is saved in ErrorData and all error are sent to log.
   * ErrorData is sent back to the CLArgResolver for display to user.
   */
  @tailrec
  final def parseMethods(args: parseMethodArgTuple,
                   tryMethod: Option[parseMethodArgTuple => resEither],
                   methodArray: List[parseMethodArgTuple => resEither]): Option[returnType] = {
    tryMethod match {
      case Some(method) =>
        method(args) match {
          case Right(res) => Some(res) // parse method succeeded, return result
          case Left((msg, error, value)) =>
            logger.debug(msg, error)
            if (argType(args) == value) { ErrorData(msg, error) }
            methodArray.length match {
              case 0 => None // no more parse methods to try, return None
              case _ => parseMethods(args, Some(methodArray.head), methodArray.drop(1))
            }
        }
      case None =>
        require(methodArray != null, "Empty method array given to parseMethods. No parseMethod to run.")
        parseMethods(args, Some(methodArray.head), methodArray.drop(1))
    }
  }
}
