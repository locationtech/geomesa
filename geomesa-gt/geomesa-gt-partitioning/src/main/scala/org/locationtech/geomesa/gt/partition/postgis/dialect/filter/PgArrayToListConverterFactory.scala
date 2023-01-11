/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect.filter

import org.geotools.util.factory.Hints
import org.geotools.util.{Converter, ConverterFactory}

/**
 * Converts from sql arrays to java lists
 */
class PgArrayToListConverterFactory extends ConverterFactory {
  override def createConverter(source: Class[_], target: Class[_], hints: Hints): Converter = {
    if (classOf[java.sql.Array].isAssignableFrom(source) && target == classOf[java.util.List[_]]) {
      PgArrayToListConverterFactory.Converter
    } else {
      null
    }
  }
}

object PgArrayToListConverterFactory {

  private val Converter = new PgArrayToListConverter()

  class PgArrayToListConverter extends Converter {
    override def convert[T](source: Any, target: Class[T]): T = {
      source.asInstanceOf[java.sql.Array].getArray match {
        case a: Array[_] => java.util.Arrays.asList(a: _*).asInstanceOf[T]
      }
    }
  }
}
