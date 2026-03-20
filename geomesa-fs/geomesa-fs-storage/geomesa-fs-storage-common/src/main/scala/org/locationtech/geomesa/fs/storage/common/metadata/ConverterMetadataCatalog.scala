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

class ConverterMetadataCatalog(context: FileSystemContext, config: Map[String, String])
    extends StorageMetadataCatalog with LazyLogging {

  import ConverterMetadata._

  import scala.collection.JavaConverters._

  // TODO we can configure through the config passed in from the datastore in addition to the conf

  private val sft = {
    val sftArg = Option(context.conf.get(SftConfigParam))
      .orElse(Option(context.conf.get(SftNameParam)))
      .getOrElse(throw new IllegalArgumentException(s"Must provide either simple feature type config or name"))
    SftArgResolver.getArg(SftArgs(sftArg, null)) match {
      case Left(e) => throw new IllegalArgumentException("Could not load SimpleFeatureType with provided parameters", e)
      case Right(schema) => schema
    }
  }

  private val schemes = {
    val partitionSchemeOpts =
      context.conf.getPropsWithPrefix(PartitionOptsPrefix).asScala.map { case (k, v) => s"$k=$v"}.mkString(":")
    val partitionSchemeName = context.conf.get(PartitionSchemeParam)
    if (partitionSchemeName == null) {
      throw new IllegalArgumentException("Must provide partition scheme name")
    }
    partitionSchemeName.split(",").map { n =>
      val nameWithOpts = s"$n:$partitionSchemeOpts"
      // back-compatible hierarchical date check
      HierarchicalDateTimeScheme.load(sft, nameWithOpts).getOrElse(PartitionSchemeFactory.load(sft, nameWithOpts))
    }
  }

  private val leafStorage = Option(context.conf.get(LeafStorageParam)).map(_.toBoolean).getOrElse {
    val deprecated = Option(context.conf.get(s"${PartitionOptsPrefix}leaf-storage")).map { s =>
      logger.warn(s"Using deprecated leaf-storage partition-scheme option. Please define leaf-storage using '$LeafStorageParam'")
      s.toBoolean
    }
    deprecated.getOrElse(true)
  }

  private val converterPath = Option(context.conf.get(ConverterPathParam)).map(new Path(context.root, _)).getOrElse {
    throw new IllegalArgumentException("Must provide converter path")
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
