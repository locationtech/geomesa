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
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.locationtech.geomesa.utils.text.StringSerialization

import scala.util.control.NonFatal

/**
 * Catalog for file-based metadata
 *
 * @param context file system
 */
class FileBasedMetadataCatalog(context: FileSystemContext) extends StorageMetadataCatalog {

  private val directory = context.root.resolve(FileBasedMetadataCatalog.MetadataDirectory + "/")

  override def getTypeNames: Seq[String] = {
    WithClose(ObjectStore(context)) { fs =>
      val iter = fs.list(directory).flatMap { file =>
        Option(file.getPath).flatMap { path =>
          Option(path.lastIndexOf('/'))
            .collect { case i if i != -1 => path.substring(i + 1) }
            .collect { case n if !n.startsWith(".") && n.endsWith(".json") =>
              StringSerialization.decodeAlphaNumericSafeString(n.substring(0, n.length - 5))
            }
        }
      }
      WithClose(iter)(_.toList)
    }
  }

  override def load(typeName: String): FileBasedMetadata = {
    val file = directory.resolve(StringSerialization.alphaNumericSafeString(typeName) + ".json")
    val fs = ObjectStore(context)
    try {
      WithClose(fs.read(file)) { opt =>
        val is = opt.orNull
        if (is == null) {
          throw new IllegalArgumentException(s"Type '$typeName' does not exit")
        }
        val meta = MetadataSerialization.deserialize(is)
        new FileBasedMetadata(fs, meta.copy(sft = namespaced(meta.sft, context.namespace)), directory)
      }
    } catch {
      case NonFatal(e) => CloseWithLogging(fs); throw e
    }
  }

  override def create(sft: SimpleFeatureType, partitions: Seq[String], targetFileSize: Option[Long]): FileBasedMetadata = {
    // load the partition scheme first in case it fails
    partitions.foreach(PartitionSchemeFactory.load(sft, _))
    val meta = Metadata(sft, partitions, targetFileSize)
    val file = directory.resolve(StringSerialization.alphaNumericSafeString(sft.getTypeName) + ".json")
    val fs = ObjectStore(context)
    try {
      // TODO overwrite?
      WithClose(fs.overwrite(file)) { out =>
        MetadataSerialization.serialize(out, meta)
      }
      new FileBasedMetadata(fs, meta.copy(sft = namespaced(meta.sft, context.namespace)), directory)
    } catch {
      case NonFatal(e) => CloseWithLogging(fs); throw e
    }
  }
}

object FileBasedMetadataCatalog {
  val MetadataDirectory = "metadata"
}
