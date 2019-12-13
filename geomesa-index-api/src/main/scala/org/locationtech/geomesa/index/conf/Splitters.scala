/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.utils.text.KVPairParser
import org.opengis.feature.simple.SimpleFeatureType

@deprecated("org.locationtech.geomesa.index.conf.splitter.DefaultSplitter")
class DigitSplitter extends TableSplitter with LazyLogging {

  /**
   * @param options allowed options are "fmt", "min", and "max"
   * @return
   */
  override def getSplits(sft: SimpleFeatureType, index: String, options: String): Array[Array[Byte]] = {
    if (index == IdIndex.name) {
      logger.warn("Using deprecated split implementation. See" +
          "http://www.geomesa.org/documentation/current/user/datastores/index_config.html for details.")
      val opts = KVPairParser.parse(options)
      val fmt = opts.getOrElse("fmt", "%01d")
      val min = opts.getOrElse("min", "0").toInt
      val max = opts.getOrElse("max", "0").toInt
      (min to max).map(fmt.format(_).getBytes(StandardCharsets.UTF_8)).toArray
    } else {
      Array(Array.empty[Byte])
    }
  }

  override def getSplits(sft: SimpleFeatureType,
                         index: String,
                         partition: String,
                         options: String): Array[Array[Byte]] = getSplits(sft, index, options)
}

@deprecated("org.locationtech.geomesa.index.conf.splitter.DefaultSplitter")
class HexSplitter extends TableSplitter with LazyLogging {

  // note: we don't include 0 to avoid an empty initial tablet
  private val hexSplits = "123456789abcdefABCDEF".map(_.toString.getBytes(StandardCharsets.UTF_8)).toArray

  override def getSplits(sft: SimpleFeatureType, index: String, options: String): Array[Array[Byte]] = {
    if (index == IdIndex.name) {
      logger.warn("Using deprecated split implementation. See" +
          "http://www.geomesa.org/documentation/current/user/datastores/index_config.html for details.")
      hexSplits
    } else {
      Array(Array.empty[Byte])
    }
  }

  override def getSplits(sft: SimpleFeatureType,
                         index: String,
                         partition: String,
                         options: String): Array[Array[Byte]] = getSplits(sft, index, options)
}

@deprecated("org.locationtech.geomesa.index.conf.splitter.DefaultSplitter")
class AlphaNumericSplitter extends TableSplitter with LazyLogging {
  // note: we don't include 0 to avoid an empty initial tablet
  override def getSplits(sft: SimpleFeatureType, index: String, options: String): Array[Array[Byte]] = {
    if (index == IdIndex.name) {
      logger.warn("Using deprecated split implementation. See" +
          "http://www.geomesa.org/documentation/current/user/datastores/index_config.html for details.")
      (('1' to '9') ++ ('a' to 'z') ++ ('A' to 'Z')).map(c => Array(c.toByte)).toArray
    } else {
      Array(Array.empty[Byte])
    }
  }

  override def getSplits(sft: SimpleFeatureType,
                         index: String,
                         partition: String,
                         options: String): Array[Array[Byte]] = getSplits(sft, index, options)
}

@deprecated("org.locationtech.geomesa.index.conf.splitter.DefaultSplitter")
class NoSplitter extends TableSplitter with LazyLogging {
  override def getSplits(sft: SimpleFeatureType, index: String, options: String): Array[Array[Byte]] = {
    if (index == IdIndex.name) {
      logger.warn("Using deprecated split implementation. See" +
          "http://www.geomesa.org/documentation/current/user/datastores/index_config.html for details.")
    }
    Array(Array.empty)
  }

  override def getSplits(sft: SimpleFeatureType,
                         index: String,
                         partition: String,
                         options: String): Array[Array[Byte]] = getSplits(sft, index, options)
}
