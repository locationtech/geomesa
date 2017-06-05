/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import org.apache.commons.csv.{CSVFormat, CSVParser, CSVPrinter}

object StringSerialization {

  /**
    * Encode a map of sequences as a string
    *
    * @param map map of keys to sequences of values
    * @return
    */
  def encodeSeqMap(map: Map[String, Seq[AnyRef]]): String = {
    import scala.collection.JavaConverters._
    val sb = new java.lang.StringBuilder
    val printer = new CSVPrinter(sb, CSVFormat.DEFAULT)
    map.foreach { case (k, v) =>
      printer.print(k)
      printer.printRecord(v.asJava)
    }
    sb.toString
  }

  /**
    * Decode a map of sequences from a string encoded by @see encodeSeqMap
    *
    * @param encoded encoded map
    * @return decoded map
    */
  def decodeSeqMap(encoded: String): Map[String, Seq[AnyRef]] = {
    import scala.collection.JavaConversions._
    // encoded as CSV, first element of each row is key, rest is value
    val parser = CSVParser.parse(encoded, CSVFormat.DEFAULT)
    parser.iterator.map { record =>
      val iter = record.iterator
      iter.next -> iter.toSeq
    }.toMap
  }
}
