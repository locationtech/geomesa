/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.partitions.HierarchicalDateTimeScheme
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
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

  import scala.collection.JavaConverters._

  private val sft = {
    val sftArg =
      config.get(SftConfigParam)
        .orElse(config.get(SftNameParam))
        .orElse(Option(context.conf.get(SftConfigParam)))
        .orElse(Option(context.conf.get(SftNameParam)))
        .getOrElse(throw new IllegalArgumentException(s"Must provide either simple feature type config or name"))
    SftArgResolver.getArg(SftArgs(sftArg, null)) match {
      case Left(e) => throw new IllegalArgumentException("Could not load SimpleFeatureType with provided parameters", e)
      case Right(schema) => schema
    }
  }

  private val schemes = {
    val partitionSchemeName =
      config.get(PartitionSchemeParam)
        .orElse(Option(context.conf.get(PartitionSchemeParam)))
        .getOrElse(throw new IllegalArgumentException("Must provide partition scheme name"))
    val partitionSchemeOpts = {
      val opts =
        config.collect { case (k, v) if k.startsWith(PartitionOptsPrefix) => k.substring(PartitionOptsPrefix.length) -> v } ++
          context.conf.getPropsWithPrefix(PartitionOptsPrefix).asScala
      opts.map { case (k, v) => s"$k=$v"}.mkString(":")
    }
    partitionSchemeName.split(",").map { n =>
      val nameWithOpts = s"$n:$partitionSchemeOpts"
      // back-compatible hierarchical date check
      HierarchicalDateTimeScheme.load(sft, nameWithOpts).getOrElse(PartitionSchemeFactory.load(sft, nameWithOpts))
    }
  }

  private val converterPath =
    config.get(ConverterPathParam)
      .orElse(Option(context.conf.get(ConverterPathParam)))
      .map(new Path(context.root, _))
      .getOrElse(throw new IllegalArgumentException("Must provide converter path"))

  private val leafStorage =
    config.get(LeafStorageParam)
      .orElse(Option(context.conf.get(LeafStorageParam)))
      .map(_.toBoolean)
      .getOrElse {
        val deprecated = s"${PartitionOptsPrefix}leaf-storage"
        config.get(deprecated).orElse(Option(context.conf.get(deprecated))).fold(true) { s =>
          logger.warn(s"Using deprecated leaf-storage partition-scheme option. Please define leaf-storage using '$LeafStorageParam'")
          s.toBoolean
        }
      }

  override def getTypeNames: Seq[String] =
    if (PathCache.exists(context.fs, converterPath)) { Seq(sft.getTypeName) } else { Seq.empty }

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
