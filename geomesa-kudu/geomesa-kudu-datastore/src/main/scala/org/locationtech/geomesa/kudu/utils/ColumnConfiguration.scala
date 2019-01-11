/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.kudu.ColumnSchema.{ColumnSchemaBuilder, CompressionAlgorithm, Encoding}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.kudu.KuduSystemProperties

/**
  * Configures a column before building. Currently supports encoding and compression
  */
case class ColumnConfiguration(encoding: Option[Encoding], compression: Option[CompressionAlgorithm]) {
  def apply(builder: ColumnSchemaBuilder): ColumnSchemaBuilder = {
    encoding.foreach(builder.encoding)
    compression.foreach(builder.compressionAlgorithm)
    KuduSystemProperties.BlockSize.option.map(_.toInt).foreach(builder.desiredBlockSize)
    builder
  }
}

object ColumnConfiguration extends LazyLogging {

  val EncodingOption = "encoding"
  val CompressionOption = "compression"

  // kudu types with default bit-shuffle encoding: INT8, INT16, INT32, INT64, UNIXTIME_MICROS, FLOAT, DOUBLE
  private val defaultBitShuffleTypes =
    Seq(ObjectType.INT, ObjectType.LONG, ObjectType.FLOAT, ObjectType.DOUBLE, ObjectType.DATE, ObjectType.GEOMETRY)

  def apply(binding: ObjectType, config: scala.collection.Map[AnyRef, AnyRef]): ColumnConfiguration = {
    val enc = config.get(EncodingOption).map(e => encoding(e.toString))
    val comp = if (enc.contains(Encoding.BIT_SHUFFLE) || defaultBitShuffleTypes.contains(binding)) {
      config.get(CompressionOption).foreach { c =>
        // bit-shuffled columns are already compressed, and shouldn't be double compressed
        // http://kudu.apache.org/docs/schema_design.html#compression
        logger.warn(s"Ignoring compression '$c' on bit-shuffled column of type '$binding'")
      }
      None
    } else {
      Some(compression(config.get(CompressionOption).map(_.toString)))
    }
    ColumnConfiguration(enc, comp)
  }

  /**
    * Converts an encoding string into an enum
    *
    * @param name encoding
    * @return
    */
  def encoding(name: String): Encoding = {
    Encoding.values().find(_.name.equalsIgnoreCase(name)).getOrElse {
      throw new IllegalArgumentException(s"Encoding '$name' not found. Valid values are: " +
          Encoding.values().map(_.name).mkString(", "))
    }
  }

  /**
    * Default configured compression
    *
    * note: bit-shuffle encoding is already compressed, so don't compress again
    *
    * @param name compression
    * @return
    */
  def compression(name: Option[String] = None): CompressionAlgorithm = {
    val n = name.getOrElse(KuduSystemProperties.Compression.get)
    CompressionAlgorithm.values().find(_.name.equalsIgnoreCase(n)).getOrElse {
      throw new IllegalArgumentException(s"Compression '$n' not found. Valid values are: " +
          CompressionAlgorithm.values().map(_.name).mkString(", "))
    }
  }
}
