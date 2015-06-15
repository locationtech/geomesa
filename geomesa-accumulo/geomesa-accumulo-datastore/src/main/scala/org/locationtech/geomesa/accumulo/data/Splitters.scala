/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.data

import java.util

import org.apache.hadoop.io.Text

import scala.collection.JavaConversions._

class DigitSplitter extends TableSplitter {
  /**
   * @param options allowed options are "fmt", "min", and "max"
   * @return
   */
  override def getSplits(options: util.Map[String, String]): Array[Text] = {
    val fmt = options.getOrElse("fmt", "%01d")
    val min = options.getOrElse("min", "0").toInt
    val max = options.getOrElse("max", "0").toInt
    (min to max).map(fmt.format(_)).map(s => new Text(s)).toArray
  }
}

class HexSplitter extends TableSplitter {
  val hexSplits = "0123456789abcdefABCDEF".map(_.toString).map(new Text(_)).toArray
  override def getSplits(options: util.Map[String, String]): Array[Text] = hexSplits
}

class AlphaNumericSplitter extends TableSplitter {
  override def getSplits(options: util.Map[String, String]): Array[Text] =
    (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).map(c => new Text("" + c)).toArray
}
