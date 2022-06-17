/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> fb4b9418a7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 1a6dc23ec5 (GEOMESA-3215 Postgis - support List-type attributes)
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
