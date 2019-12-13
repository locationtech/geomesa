/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import java.util.regex.Pattern

import com.beust.jcommander.{IStringConverter, IStringConverterFactory}

class GeoMesaIStringConverterFactory extends IStringConverterFactory {

  import GeoMesaIStringConverterFactory.ConverterMap

  override def getConverter[T](forType: Class[T]): Class[_ <: IStringConverter[T]] =
    ConverterMap.getOrElse(forType, null).asInstanceOf[Class[IStringConverter[T]]]
}

object GeoMesaIStringConverterFactory {
  val ConverterMap: Map[Class[_], Class[_ <: IStringConverter[_]]] =
    Map[Class[_], Class[_ <: IStringConverter[_]]](
      classOf[Pattern] -> classOf[JPatternConverter]
    )
}

class JPatternConverter extends IStringConverter[Pattern] {
  override def convert(value: String): Pattern = Pattern.compile(value)
}
