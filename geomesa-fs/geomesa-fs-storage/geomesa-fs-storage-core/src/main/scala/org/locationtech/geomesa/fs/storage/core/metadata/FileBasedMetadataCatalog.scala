/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package metadata

import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.StringSerialization

/**
 * Catalog for file-based metadata
 *
 * @param context file system
 */
class FileBasedMetadataCatalog(context: FileSystemContext) extends StorageMetadataCatalog {

  private val directory = context.root.resolve(FileBasedMetadataCatalog.MetadataDirectory + "/")

  override def getTypeNames: Seq[String] = {
    context.fs.list(directory).flatMap { file =>
      Option(file.getPath).flatMap { path =>
        Option(path.lastIndexOf('/'))
          .collect { case i if i != -1 => path.substring(i + 1) }
          .collect { case n if !n.startsWith(".") && n.endsWith(".json") =>
            StringSerialization.decodeAlphaNumericSafeString(n.substring(0, n.length - 5))
          }
      }
    }
  }

  override def load(typeName: String): FileBasedMetadata = {
    val file = directory.resolve(StringSerialization.alphaNumericSafeString(typeName) + ".json")
    WithClose(context.fs.read(file)) { opt =>
      val is = opt.orNull
      if (is == null) {
        throw new IllegalArgumentException(s"Type '$typeName' does not exit")
      }
      val meta = MetadataSerialization.deserialize(is)
      new FileBasedMetadata(context.fs, meta.copy(sft = namespaced(meta.sft, context.namespace)), directory)
    }
  }

  override def create(sft: SimpleFeatureType, partitions: Seq[String], targetFileSize: Option[Long]): FileBasedMetadata = {
    // load the partition scheme first in case it fails
    partitions.foreach(PartitionSchemeFactory.load(sft, _))
    val meta = Metadata(sft, partitions, targetFileSize)
    val file = directory.resolve(StringSerialization.alphaNumericSafeString(sft.getTypeName) + ".json")
    // TODO overwrite?
    WithClose(context.fs.overwrite(file)) { out =>
      MetadataSerialization.serialize(out, meta)
    }
    new FileBasedMetadata(context.fs, meta.copy(sft = namespaced(meta.sft, context.namespace)), directory)
  }
}

object FileBasedMetadataCatalog {
  val MetadataDirectory = "metadata"
}
