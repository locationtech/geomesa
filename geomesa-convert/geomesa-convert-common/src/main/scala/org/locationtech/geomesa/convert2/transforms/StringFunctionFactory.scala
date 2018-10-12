/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.apache.commons.lang3.StringUtils

import scala.util.matching.Regex


class StringFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] =
    Seq(stripQuotes, strip, stripPrefix, stripSuffix, replace, remove,
      strLen, trim, capitalize, lowercase, uppercase, regexReplace, concat,
      substr, string, mkstring, emptyToNull, printf)

  private val string = TransformerFunction("toString") { args =>
    args(0).toString
  }

  private val stripQuotes = TransformerFunction("stripQuotes") { args =>
    StringUtils.strip(args(0).asInstanceOf[String], "'\"")
  }

  private val strip = TransformerFunction("strip") { args =>
    if (args.length == 1) {
      StringUtils.strip(args(0).asInstanceOf[String])
    } else {
      val toStrip = args(1).asInstanceOf[String]
      StringUtils.strip(args(0).asInstanceOf[String], toStrip)
    }
  }

  private val stripPrefix = TransformerFunction("stripPrefix") { args =>
    val toStrip = args(1).asInstanceOf[String]
    StringUtils.stripStart(args(0).asInstanceOf[String], toStrip)
  }

  private val stripSuffix = TransformerFunction("stripSuffix") { args =>
    val toStrip = args(1).asInstanceOf[String]
    StringUtils.stripEnd(args(0).asInstanceOf[String], toStrip)
  }

  private val replace = TransformerFunction("replace") { args =>
    val toRemove = args(1).asInstanceOf[String]
    val replacement = args(2).asInstanceOf[String]
    args(0).asInstanceOf[String].replaceAllLiterally(toRemove, replacement)
  }

  private val remove = TransformerFunction("remove") { args =>
    val toRemove = args(1).asInstanceOf[String]
    StringUtils.remove(args(0).asInstanceOf[String], toRemove)
  }

  private val trim = TransformerFunction("trim") { args =>
    args(0).asInstanceOf[String].trim
  }

  private val capitalize = TransformerFunction("capitalize") { args =>
    args(0).asInstanceOf[String].capitalize
  }

  private val lowercase = TransformerFunction("lowercase") { args =>
    args(0).asInstanceOf[String].toLowerCase
  }

  private val uppercase = TransformerFunction("uppercase") { args =>
    args(0).asInstanceOf[String].toUpperCase
  }

  private val concat = TransformerFunction("concat", "concatenate") { args =>
    args.map(_.toString).mkString
  }

  private val mkstring = TransformerFunction("mkstring") { args =>
    args.drop(1).map(_.toString).mkString(args(0).toString)
  }

  private val emptyToNull = TransformerFunction("emptyToNull") { args =>
    Option(args(0)).map(_.toString).filterNot(_.trim.isEmpty).orNull
  }

  private val regexReplace = TransformerFunction("regexReplace") { args =>
    args(0).asInstanceOf[Regex].replaceAllIn(args(2).asInstanceOf[String], args(1).asInstanceOf[String])
  }

  private val substr = TransformerFunction("substr", "substring") { args =>
    args(0).asInstanceOf[String].substring(args(1).asInstanceOf[Int], args(2).asInstanceOf[Int])
  }

  private val strLen = TransformerFunction("strlen", "stringLength", "length") { args =>
    args(0).asInstanceOf[String].length
  }

  private val printf = TransformerFunction("printf") { args =>
    String.format(args(0).toString, args.drop(1).asInstanceOf[Array[AnyRef]]: _*)
  }
}
