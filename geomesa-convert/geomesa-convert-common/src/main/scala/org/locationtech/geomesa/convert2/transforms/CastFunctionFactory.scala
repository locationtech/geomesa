/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

class CastFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] =
    Seq(castToInt, castToLong, castToFloat, castToDouble, castToBoolean)

  // usage: stringToInt($1, 0)
  private val castToInt = TransformerFunction.pure("toInt", "toInteger", "stringToInt", "stringToInteger") { args =>
    val default = if (args.lengthCompare(1) > 0) { args(1) } else { null }
    args(0) match {
      case n: String => try { n.toInt } catch { case _: Exception => default }
      case n: Number => n.intValue()
      case n: Any    => try { n.toString.toInt } catch { case _: Exception => default }
      case null      => default
    }
  }

  // usage: stringToLong($1, 0L)
  private val castToLong = TransformerFunction.pure("toLong", "stringToLong") { args =>
    val default = if (args.lengthCompare(1) > 0) { args(1) } else { null }
    args(0) match {
      case n: String => try { n.toLong } catch { case _: Exception => default }
      case n: Number => n.longValue()
      case n: Any    => try { n.toString.toLong } catch { case _: Exception => default }
      case null      => default
    }
  }

  // usage: stringToFloat($1, 0f)
  private val castToFloat = TransformerFunction.pure("toFloat", "stringToFloat") { args =>
    val default = if (args.lengthCompare(1) > 0) { args(1) } else { null }
    args(0) match {
      case n: String => try { n.toFloat } catch { case _: Exception => default }
      case n: Number => n.floatValue()
      case n: Any    => try { n.toString.toFloat } catch { case _: Exception => default }
      case null      => default
    }
  }

  // usage: stringToDouble($1, 0d)
  private val castToDouble = TransformerFunction.pure("toDouble", "stringToDouble") { args =>
    val default = if (args.lengthCompare(1) > 0) { args(1) } else { null }
    args(0) match {
      case n: String => try { n.toDouble } catch { case _: Exception => default }
      case n: Number => n.doubleValue()
      case n: Any    => try { n.toString.toDouble } catch { case _: Exception => default }
      case null      => default
    }
  }

  // usage: stringToBoolean($1, false)
  private val castToBoolean = TransformerFunction.pure("toBool", "toBoolean", "stringToBool", "stringToBoolean") { args =>
    val default = if (args.lengthCompare(1) > 0) { args(1) } else { null }
    args(0) match {
      case b: String  => try { b.toBoolean } catch { case _: Exception => default }
      case b: Boolean => b
      case n: Number  => n.intValue() != 0
      case b: Any     => try { b.toString.toBoolean } catch { case _: Exception => default }
      case null       => default
    }
  }
}
