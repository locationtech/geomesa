/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package metadata

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.PartitionSchemeFactory
import org.locationtech.geomesa.fs.storage.core.schemes.HierarchicalDateTimeScheme
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs}

/**
 * Synthetic metadata catalog that examines files on disk
 *
 * @param context file system
 * @param config converter configuration options
 */
class ConverterMetadataCatalog(context: FileSystemContext, config: Map[String, String])
    extends StorageMetadataCatalog with LazyLogging {

  import ConverterMetadata._

  private val sft = {
    val sftArg =
      config.get(SftConfigParam)
        .orElse(config.get(SftNameParam))
        .orElse(context.conf.get(SftConfigParam))
        .orElse(context.conf.get(SftNameParam))
        .getOrElse(throw new IllegalArgumentException(s"Must provide either simple feature type config or name"))
    SftArgResolver.getArg(SftArgs(sftArg, null)) match {
      case Left(e) => throw new IllegalArgumentException("Could not load SimpleFeatureType with provided parameters", e)
      case Right(schema) => schema
    }
  }

  private val schemes = {
    val partitionSchemeName =
      config.get(PartitionSchemeParam)
        .orElse(context.conf.get(PartitionSchemeParam))
        .getOrElse(throw new IllegalArgumentException("Must provide partition scheme name"))
    val partitionSchemeOpts = {
      val opts =
        (config ++ context.conf).collect { case (k, v) if k.startsWith(PartitionOptsPrefix) => s"${k.substring(PartitionOptsPrefix.length)}=$v" }
      opts.mkString(":")
    }
    partitionSchemeName.split(",").map { n =>
      val nameWithOpts = s"$n:$partitionSchemeOpts"
      // back-compatible hierarchical date check
      HierarchicalDateTimeScheme.load(sft, nameWithOpts).getOrElse(PartitionSchemeFactory.load(sft, nameWithOpts))
    }
  }

  private val converterPath =
    config.get(ConverterPathParam)
      .orElse(context.conf.get(ConverterPathParam))
      .map(context.root.resolve)
      .getOrElse(throw new IllegalArgumentException("Must provide converter path"))

  private val leafStorage =
    config.get(LeafStorageParam)
      .orElse(context.conf.get(LeafStorageParam))
      .map(_.toBoolean)
      .getOrElse {
        val deprecated = s"${PartitionOptsPrefix}leaf-storage"
        config.get(deprecated).orElse(context.conf.get(deprecated)).fold(true) { s =>
          logger.warn(s"Using deprecated leaf-storage partition-scheme option. Please define leaf-storage using '$LeafStorageParam'")
          s.toBoolean
        }
      }

  override def getTypeNames: Seq[String] =
    if (context.fs.exists(converterPath)) { Seq(sft.getTypeName) } else { Seq.empty }

  override def load(typeName: String): StorageMetadata = {
    if (typeName == sft.getTypeName) {
      new ConverterMetadata(context.copy(root = converterPath), sft, schemes, leafStorage)
    } else {
      throw new IllegalArgumentException(s"Schema '$typeName' doesn't exist - available schemas: ${sft.getTypeName}")
    }
  }

  override def create(sft: SimpleFeatureType, partitions: Seq[String], targetFileSize: Option[Long]): StorageMetadata =
    throw new UnsupportedOperationException("Converter storage is read only")
}
