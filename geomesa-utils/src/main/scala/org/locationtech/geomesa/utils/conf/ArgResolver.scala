/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import com.typesafe.config.ConfigParseOptions
import com.typesafe.scalalogging.LazyLogging

/**
 * Trait for resolving sft/config parameters and handling errors around
 * parsing.
 */
trait ArgResolver[ReturnType, ParseMethodArgs] extends LazyLogging {

  import ArgResolver.ArgTypes.ArgTypes

  type ResEither = Either[(String, Throwable, ArgTypes), ReturnType]

  // Important to setAllowMissing to false bc else you'll get a config but it will be empty
  protected val parseOpts =
    ConfigParseOptions.defaults()
      .setAllowMissing(false)
      .setClassLoader(null)
      .setIncluder(null)
      .setOriginDescription(null)
      .setSyntax(null)

  // Should return a guess of the argType without using the parsers (e.g. regex, contains, etc)
  protected def argType(args: ParseMethodArgs): ArgTypes

  // (Ordered) List of parse methods to attempt
  protected def parseMethodList: Seq[ParseMethodArgs => ResEither]

  /**
   * @return the instance of ReturnType parsed from the Args
   */
  def getArg(args: ParseMethodArgs): Either[Throwable, ReturnType] = {
    var error: Exception = null

    var methods = parseMethodList
    assert(methods.nonEmpty, "subclass did not define any parse methods")

    do {
      methods.head.apply(args) match {
        case Right(res) => return Right(res) // parse method succeeded, re-cast and return result

        case Left((msg, e, value)) =>
          logger.debug(msg, e)
          if (error == null || argType(args) == value) {
            error = new RuntimeException(s"$msg\n${e.getMessage}", e)
            error.setStackTrace(e.getStackTrace)
          }
      }
      methods = methods.tail
    } while (methods.nonEmpty)

    // nothing passed, return the most relevant error
    Left(error)
  }
}

object ArgResolver {
  // Type Names for arg type guesses
  object ArgTypes extends Enumeration {
    val NAME, SPECSTR, CONFSTR, PATH = Value
    type ArgTypes = Value
  }
}
