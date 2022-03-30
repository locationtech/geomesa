/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
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

  private val string = TransformerFunction.pure("toString") { args =>
    if (args(0) == null) { null } else { args(0).toString }
  }

  private val stripQuotes = TransformerFunction.pure("stripQuotes") { args =>
    if (args(0) == null) { null } else {
      val s = args(0) match {
        case s: String => s
        case s => s.toString
      }
      StringUtils.strip(s, "'\"")
    }
  }

  private val strip = TransformerFunction.pure("strip") { args =>
    if (args(0) == null) { null } else {
      val s = args(0) match {
        case s: String => s
        case s => s.toString
      }
      val toStrip = if (args.length == 1) { null } else { args(1).asInstanceOf[String] }
      StringUtils.strip(s, toStrip)
    }
  }

  private val stripPrefix = TransformerFunction.pure("stripPrefix") { args =>
    if (args(0) == null) { null } else {
      val s = args(0) match {
        case s: String => s
        case s => s.toString
      }
      val toStrip = args(1).asInstanceOf[String]
      StringUtils.stripStart(s, toStrip)
    }
  }

  private val stripSuffix = TransformerFunction.pure("stripSuffix") { args =>
    if (args(0) == null) { null } else {
      val s = args(0) match {
        case s: String => s
        case s => s.toString
      }
      val toStrip = args(1).asInstanceOf[String]
      StringUtils.stripEnd(s, toStrip)
    }
  }

  private val replace = TransformerFunction.pure("replace") { args =>
    if (args(0) == null) { null } else {
      val s = args(0) match {
        case s: String => s
        case s => s.toString
      }
      val toRemove = args(1).asInstanceOf[String]
      val replacement = args(2).asInstanceOf[String]
      s.replaceAllLiterally(toRemove, replacement)
    }
  }

  private val remove = TransformerFunction.pure("remove") { args =>
    if (args(0) == null) { null } else {
      val s = args(0) match {
        case s: String => s
        case s => s.toString
      }
      val toRemove = args(1).asInstanceOf[String]
      StringUtils.remove(s, toRemove)
    }
  }

  private val trim = TransformerFunction.pure("trim") { args =>
    if (args(0) == null) { null } else {
      val s = args(0) match {
        case s: String => s
        case s => s.toString
      }
      s.trim
    }
  }

  private val capitalize = TransformerFunction.pure("capitalize") { args =>
    if (args(0) == null) { null } else {
      val s = args(0) match {
        case s: String => s
        case s => s.toString
      }
      s.capitalize
    }
  }

  private val lowercase = TransformerFunction.pure("lowercase") { args =>
    if (args(0) == null) { null } else {
      val s = args(0) match {
        case s: String => s
        case s => s.toString
      }
      s.toLowerCase
    }
  }

  private val uppercase = TransformerFunction.pure("uppercase") { args =>
    if (args(0) == null) { null } else {
      val s = args(0) match {
        case s: String => s
        case s => s.toString
      }
      s.toUpperCase
    }
  }

  private val concat = TransformerFunction.pure("concat", "concatenate") { args =>
    val strings = args.map {
      case s: String => s
      case null => "null"
      case s => s.toString
    }
    strings.mkString
  }

  private val mkstring = TransformerFunction.pure("mkstring") { args =>
    val delim = args(0).asInstanceOf[String]
    val strings = args.drop(1).map {
      case s: String => s
      case null => "null"
      case s => s.toString
    }
    strings.mkString(delim)
  }

  private val emptyToNull = TransformerFunction.pure("emptyToNull") { args =>
    if (args(0) == null || args(0).toString.trim.isEmpty) { null } else { args(0) }
  }

  private val regexReplace = TransformerFunction.pure("regexReplace") { args =>
    if (args(2) == null) { null } else {
      val s = args(2) match {
        case s: String => s
        case s => s.toString
      }
      val regex = args(0).asInstanceOf[Regex]
      val replacement = args(1).asInstanceOf[String]
      regex.replaceAllIn(s, replacement)
    }
  }

  private val substr = TransformerFunction.pure("substr", "substring") { args =>
    if (args(0) == null) { null } else {
      val s = args(0) match {
        case s: String => s
        case s => s.toString
      }
      val from = args(1).asInstanceOf[Int]
      val to = args(2).asInstanceOf[Int]
      s.substring(from, to)
    }
  }

  private val strLen = TransformerFunction.pure("strlen", "stringLength", "length") { args =>
    val s = args(0) match {
      case s: String => s
      case null => ""
      case s => s.toString
    }
    s.length
  }

  private val printf = TransformerFunction.pure("printf") { args =>
    String.format(args(0).toString, args.drop(1).asInstanceOf[Array[AnyRef]]: _*)
  }
}
