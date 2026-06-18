/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.metadata

import org.apache.iceberg.{DataFile, Table}
import org.apache.iceberg.expressions.{Expression, Expressions}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.core.iceberg.IcebergMapper
import org.locationtech.geomesa.fs.storage.core.metadata.IcebergMetadata.PropertyPrefix
import org.locationtech.geomesa.fs.storage.core.{Partition, PartitionScheme, StorageMetadata}
import org.locationtech.geomesa.utils.io.WithClose

class IcebergMetadata(table: Table, mapper: IcebergMapper) extends StorageMetadata {

  import scala.collection.JavaConverters._

  override val `type`: String = IcebergMetadata.MetadataType

  override val sft: SimpleFeatureType = mapper.sft

  override val schemes: Set[PartitionScheme] = mapper.schemes.toSet

  override def createDataFile(filePath: String, partition: Partition): DataFile = {
    mapper.createDataFile(table, filePath, partition)
  }

  override def addFiles(files: Seq[DataFile]): Unit = {
    val append = table.newAppend()
    files.foreach(f => append.appendFile(f))
    append.commit()
  }

  override def removeFile(file: DataFile): Unit = {
    table.newDelete().deleteFile(file).commit()
  }

  override def replaceFiles(existing: Seq[DataFile], replacements: Seq[DataFile]): Unit = {
    val tx = table.newTransaction()
    val delete = tx.newDelete()
    existing.foreach(f => delete.deleteFile(f))
    delete.commit()
    val append = tx.newAppend()
    replacements.foreach(f => append.appendFile(f))
    append.commit()
    tx.commitTransaction()
  }

  override def getFiles(): Seq[DataFile] = fileScan(Expressions.alwaysTrue())
  override def getFiles(partition: Partition): Seq[DataFile] = fileScan(mapper.expression(partition))
  override def getFiles(filter: Filter): Seq[DataFile] = fileScan(mapper.expression(filter))

  private def fileScan(expression: Expression): Seq[DataFile] = {
    WithClose(table.newScan().filter(expression).planFiles()) { tasks =>
      tasks.asScala.map(task => task.file()).toList
    }
  }

  override def get(key: String): Option[String] = Option(table.properties().get(s"$PropertyPrefix$key"))

  override def set(key: String, value: String): Unit = {
    if (value == null) {
      table.updateProperties().remove(s"$PropertyPrefix$key").commit()
    } else {
      table.updateProperties().set(s"$PropertyPrefix$key", value).commit()
    }
  }

  override def close(): Unit = {}
}

object IcebergMetadata {

  val MetadataType = "iceberg"

  private[metadata] val PropertyPrefix = "geomesa.props."
}
