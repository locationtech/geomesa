/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import org.apache.commons.text.StringEscapeUtils

import scala.util.parsing.combinator.JavaTokenParsers

trait EnhancedTokenParsers extends JavaTokenParsers {

  def nonGreedyStringLiteral: Parser[String] =
    ("\""+"""([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*?"""+"\"").r

  def nonGreedySingleQuoteLiteral: Parser[String] =
    ( "\'" +"""([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*?""" + "\'").r

  def quotedString = (nonGreedyStringLiteral | nonGreedySingleQuoteLiteral) ^^ { x =>
    StringEscapeUtils.unescapeJava(x.drop(1).dropRight(1))
  }

  def nonQuotedString = "([^'\",]+)".r

  // JavaTokenParsers.stringLiteral but single quotes
  def singleQuoteStringLiteral: Parser[String] =
    ("'"+"""([^'\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*+"""+"'").r

  def dequotedString = (singleQuoteStringLiteral | stringLiteral) ^^ { x =>
    StringEscapeUtils.unescapeJava(x.substring(1, x.length - 1))
  }
}
