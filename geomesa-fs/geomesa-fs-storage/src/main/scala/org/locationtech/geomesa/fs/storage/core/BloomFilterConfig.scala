/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import org.apache.iceberg.TableProperties
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.schema.ColumnName
import org.locationtech.geomesa.utils.geotools.ObjectType

import scala.util.Try

/**
 * Parquet bloom filter configuration for a single attribute
 *
 * @param attribute attribute name
 * @param ndv expected number of distinct values per row group, used to size the filter
 * @param fpp false positive probability
 */
case class BloomFilterConfig(attribute: String, ndv: Option[Long] = None, fpp: Option[Double] = None)

object BloomFilterConfig {

  import TableProperties.{PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX, PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX, PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX}

  // types that are written as a single primitive parquet column, which bloom filters can be applied to
  private val SupportedTypes = Set(
    ObjectType.STRING, ObjectType.INT, ObjectType.LONG, ObjectType.FLOAT, ObjectType.DOUBLE, ObjectType.DATE,
    ObjectType.UUID, ObjectType.BYTES,
  )

  /**
   * Parse and validate bloom filter configurations. Attributes are separated with a comma (`,`), while options
   * are separated with a colon (`:`), e.g. `name:ndv=10000:fpp=0.01,age`
   *
   * @param sft simple feature type
   * @param spec bloom filter spec
   * @return
   */
  def parse(sft: SimpleFeatureType, spec: String): Seq[BloomFilterConfig] = {
    spec.split(",").toSeq.map(_.trim).filter(_.nonEmpty).map { entry =>
      val parts = entry.split(":").map(_.trim)
      val attribute = parts.head
      val descriptor = Option(sft.getDescriptor(attribute)).getOrElse {
        throw new IllegalArgumentException(s"Bloom filter attribute '$attribute' does not exist in schema ${sft.getTypeName}")
      }
      val bindings = ObjectType.selectType(descriptor)
      if (!SupportedTypes.contains(bindings.head) || bindings.last == ObjectType.JSON) {
        throw new IllegalArgumentException(
          s"Bloom filters are not supported for attribute '$attribute' of type ${descriptor.getType.getBinding.getSimpleName}")
      }
      parts.tail.foldLeft(BloomFilterConfig(attribute)) { case (config, option) =>
        option.split("=", 2).map(_.trim) match {
          case Array("ndv", v) =>
            val ndv = Try(v.toLong).toOption.filter(_ > 0).getOrElse {
              throw new IllegalArgumentException(s"Invalid bloom filter ndv for attribute '$attribute': $v")
            }
            config.copy(ndv = Some(ndv))
          case Array("fpp", v) =>
            val fpp = Try(v.toDouble).toOption.filter(d => d > 0 && d < 1).getOrElse {
              throw new IllegalArgumentException(s"Invalid bloom filter fpp for attribute '$attribute': $v")
            }
            config.copy(fpp = Some(fpp))
          case _ =>
            throw new IllegalArgumentException(s"Invalid bloom filter option for attribute '$attribute': $option")
        }
      }
    }
  }

  /**
   * Iceberg table properties for the bloom filter configurations. These are honored by any iceberg writer,
   * including GeoMesa (see `ParquetFileSystemWriter.icebergBloomFilterConf`)
   *
   * @param configs bloom filter configurations
   * @return
   */
  def tableProperties(configs: Seq[BloomFilterConfig]): Map[String, String] = {
    configs.flatMap { config =>
      val column = ColumnName(config.attribute).column
      Seq(s"$PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX$column" -> "true") ++
        config.ndv.map(n => s"$PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX$column" -> n.toString) ++
        config.fpp.map(f => s"$PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX$column" -> f.toString)
    }.toMap
  }
}
