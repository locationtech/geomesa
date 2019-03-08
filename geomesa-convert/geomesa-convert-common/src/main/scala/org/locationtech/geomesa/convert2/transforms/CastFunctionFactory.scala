/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

class CastFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] =
    Seq(stringToInt, stringToLong, stringToFloat, stringToDouble, stringToBoolean)

  // usage: stringToInt($1, 0)
  private val stringToInt = TransformerFunction.pure("stringToInt", "stringToInteger") { args =>
    tryConvert(args(0).asInstanceOf[String], _.toInt, args(1))
  }

  // usage: stringToLong($1, 0L)
  private val stringToLong = TransformerFunction.pure("stringToLong") { args =>
    tryConvert(args(0).asInstanceOf[String], _.toLong, args(1))
  }

  // usage: stringToFloat($1, 0f)
  private val stringToFloat = TransformerFunction.pure("stringToFloat") { args =>
    tryConvert(args(0).asInstanceOf[String], _.toFloat, args(1))
  }

  // usage: stringToDouble($1, 0d)
  private val stringToDouble = TransformerFunction.pure("stringToDouble") { args =>
    tryConvert(args(0).asInstanceOf[String], _.toDouble, args(1))
  }

  // usage: stringToBoolean($1, false)
  private val stringToBoolean = TransformerFunction.pure("stringToBool", "stringToBoolean") { args =>
    tryConvert(args(0).asInstanceOf[String], _.toBoolean, args(1))
  }

  private def tryConvert(s: String, conversion: String => Any, default: Any): Any = {
    if (s == null || s.isEmpty) {
      return default
    }
    try { conversion(s) } catch { case _: Exception => default }
  }
}
