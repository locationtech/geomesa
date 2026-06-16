/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.iceberg

import org.apache.iceberg.Table
import org.apache.iceberg.expressions.{Expression, Expressions}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.StorageFile
import org.locationtech.geomesa.fs.storage.core.{Partition, PartitionScheme, StorageMetadata}
import org.locationtech.geomesa.fs.storage.parquet.iceberg.IcebergMetadata.PropertyPrefix
import org.locationtech.geomesa.utils.io.WithClose

class IcebergMetadata(table: Table, mapper: IcebergMapper) extends StorageMetadata {

  import scala.collection.JavaConverters._

  override val `type`: String = IcebergMetadata.MetadataType

  override val sft: SimpleFeatureType = mapper.sft

  override val schemes: Set[PartitionScheme] = mapper.schemes.toSet

  override def addFile(file: StorageFile): Unit = {
    val df = mapper.toDataFile(table, file)
    table.newAppend().appendFile(df).commit()
  }

  override def removeFile(file: StorageFile): Unit = {
    val df = mapper.toDataFile(table, file)
    table.newDelete().deleteFile(df).commit()
  }

  override def replaceFiles(existing: Seq[StorageFile], replacements: Seq[StorageFile]): Unit = {
    val tx = table.newTransaction()
    val delete = tx.newDelete()
    existing.foreach(f => delete.deleteFile(mapper.toDataFile(table, f)))
    delete.commit()
    val append = tx.newAppend()
    replacements.foreach(f => append.appendFile(mapper.toDataFile(table, f)))
    append.commit()
    tx.commitTransaction()
  }

  override def getFiles(): Seq[StorageFile] = fileScan(Expressions.alwaysTrue())
  override def getFiles(partition: Partition): Seq[StorageFile] = fileScan(mapper.expression(partition))
  override def getFiles(filter: Filter): Seq[StorageFile] = fileScan(mapper.expression(filter))

  private def fileScan(expression: Expression): Seq[StorageFile] = {
    WithClose(table.newScan().filter(expression).planFiles()) { tasks =>
      tasks.asScala.map(task => mapper.fromDataFile(task.file())).toList
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

  private[iceberg] val PropertyPrefix = "geomesa.props."
}
