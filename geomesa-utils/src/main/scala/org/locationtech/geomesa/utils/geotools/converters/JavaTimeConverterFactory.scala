/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.converters

import java.util.Date

import org.geotools.factory.Hints
import org.geotools.util.{Converter, ConverterFactory}
import org.locationtech.geomesa.utils.text.DateParsing

class JavaTimeConverterFactory extends ConverterFactory {
  def createConverter(source: Class[_], target: Class[_], hints: Hints): Converter = {
    if (classOf[Date].isAssignableFrom(source) && classOf[String].isAssignableFrom(target)) {
      JavaTimeConverterFactory.DateToStringConverter
    } else if (classOf[Date].isAssignableFrom(target) && classOf[String].isAssignableFrom(source)) {
      JavaTimeConverterFactory.StringToDateConverter
    } else {
      null
    }
  }
}

object JavaTimeConverterFactory {

  private val DateToStringConverter = new Converter {
    override def convert[T](source: AnyRef, target: Class[T]): T =
      DateParsing.formatDate(source.asInstanceOf[Date]).asInstanceOf[T]
  }

  private val StringToDateConverter = new Converter {
    override def convert[T](source: AnyRef, target: Class[T]): T =
      DateParsing.parseDate(source.asInstanceOf[String]).asInstanceOf[T]
  }
}