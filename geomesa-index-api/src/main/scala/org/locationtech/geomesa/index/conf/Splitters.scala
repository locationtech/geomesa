/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import java.nio.charset.StandardCharsets
import java.util

import scala.collection.JavaConversions._

class DigitSplitter extends TableSplitter {
  /**
   * @param options allowed options are "fmt", "min", and "max"
   * @return
   */
  override def getSplits(options: java.util.Map[String, String]): Array[Array[Byte]] = {
    val fmt = options.getOrElse("fmt", "%01d")
    val min = options.getOrElse("min", "0").toInt
    val max = options.getOrElse("max", "0").toInt
    (min to max).map(fmt.format(_).getBytes(StandardCharsets.UTF_8)).toArray
  }
}

class HexSplitter extends TableSplitter {
  // note: we don't include 0 to avoid an empty initial tablet
  val hexSplits = "123456789abcdefABCDEF".map(_.toString.getBytes(StandardCharsets.UTF_8)).toArray
  override def getSplits(options: util.Map[String, String]): Array[Array[Byte]] = hexSplits
}

class AlphaNumericSplitter extends TableSplitter {
  // note: we don't include 0 to avoid an empty initial tablet
  override def getSplits(options: util.Map[String, String]): Array[Array[Byte]] =
    (('1' to '9') ++ ('a' to 'z') ++ ('A' to 'Z')).map(c => Array(c.toByte)).toArray
}

class NoSplitter extends TableSplitter {
  override def getSplits(options: util.Map[String, String]): Array[Array[Byte]] = Array.empty
}
