/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import org.apache.hadoop.fs.Path
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.StringSerialization

class FileBasedMetadataCatalog(context: FileSystemContext) extends StorageMetadataCatalog {

  private val directory = new Path(context.root, FileBasedMetadataCatalog.MetadataDirectory)

  override def getTypeNames: Seq[String] = {
    val typeNames = Seq.newBuilder[String]
    if (context.fs.exists(directory)) {
      val files = context.fs.listFiles(directory, false)
      while (files.hasNext) {
        val file = files.next()
        val name = file.getPath.getName
        if (file.isFile && !name.startsWith(".") && name.endsWith(".json")) {
          typeNames += StringSerialization.decodeAlphaNumericSafeString(name.substring(0, name.length - 5))
        }
      }
    }
    typeNames.result()
  }

  override def load(typeName: String): StorageMetadata = {
    val file = new Path(directory, StringSerialization.alphaNumericSafeString(typeName) + ".json")
    if (!context.fs.exists(file)) {
      throw new IllegalArgumentException(s"Type '$typeName' does not exit")
    }
    val meta = WithClose(context.fs.open(file))(MetadataSerialization.deserialize)
    new FileBasedMetadata(context.fs, directory, meta.copy(sft = namespaced(meta.sft, context.namespace)))
  }

  override def create(sft: SimpleFeatureType, partitions: Seq[String], targetFileSize: Option[Long]): StorageMetadata = {
    // load the partition scheme first in case it fails
    partitions.foreach(PartitionSchemeFactory.load(sft, _))
    val meta = Metadata(sft, partitions, targetFileSize)
    val file = new Path(directory, StringSerialization.alphaNumericSafeString(sft.getTypeName) + ".json")
    WithClose(context.fs.create(file, true)) { out =>
      MetadataSerialization.serialize(out, meta)
      out.hflush()
      out.hsync()
    }
    new FileBasedMetadata(context.fs, directory, meta.copy(sft = namespaced(meta.sft, context.namespace)))
  }
}

object FileBasedMetadataCatalog {
  val MetadataDirectory = "metadata"
}
