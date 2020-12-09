/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet.spark

import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionMetadata, StorageFile}
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, StorageMetadata}
import org.locationtech.geomesa.fs.storage.common.partitions.FlatScheme
import org.opengis.feature.simple.SimpleFeatureType

class RawDirectoryMetadata(val sft: SimpleFeatureType, files: Seq[String]) extends StorageMetadata {

  override val encoding: String = "parquet"
  override val scheme: PartitionScheme = FlatScheme
  override val leafStorage: Boolean = false

  // TODO: Get a count from the files...

  private lazy val partition = PartitionMetadata("", files.map(StorageFile(_, 0)), None, 0L)

  override def getPartitions(prefix: Option[String]): Seq[PartitionMetadata] =
    if (prefix.forall(_.isEmpty)) { Seq(partition) } else { Seq.empty }

  override def getPartition(name: String): Option[PartitionMetadata] =
    if (name.isEmpty) { Some(partition) } else { None }

  override def addPartition(partition: PartitionMetadata): Unit =
    throw new NotImplementedError("Metadata is read only")

  override def removePartition(partition: PartitionMetadata): Unit =
    throw new NotImplementedError("Metadata is read only")

  override def compact(partition: Option[String], threads: Int): Unit =
    throw new NotImplementedError("Metadata is read only")

  override def close(): Unit = {}
}

object RawDirectoryMetadata {
  val MetadataType = "raw"
}

