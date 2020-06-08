/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.hadoop.fs.{CreateFlag, FileContext, Path}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling

class FileBasedMetadataFactory extends StorageMetadataFactory {

  override def name: String = FileBasedMetadata.MetadataType

  /**
    * Loads a metadata instance from an existing root. The metadata info is persisted in a `metadata.json`
    * file under the root path.
    *
    * Will return a cached instance, if available. If a previous check was made to load a file from this root,
    * and the file did not exist, will not re-attempt to load it until after a configurable timeout
    *
    * @see `org.locationtech.geomesa.fs.storage.common.utils.PathCache#CacheDurationProperty()`
    * @param context file context
    * @return
    **/
  override def load(context: FileSystemContext): Option[FileBasedMetadata] = {
    val json = MetadataJson.readMetadata(context)
    // note: do this after loading the json to allow for old metadata transition
    val cached = FileBasedMetadataFactory.cached(context)
    json match {
      case Some(m) if m.name.equalsIgnoreCase(name) =>
        cached.orElse(throw new IllegalArgumentException(s"Could not load metadata at root '${context.root.toUri}'"))

      case None if cached.isDefined =>
        // a file-based metadata impl exists, but was created with an older version
        // create a config file pointing to it, and register that
        MetadataJson.writeMetadata(context, FileBasedMetadata.DefaultOptions)
        cached

      case _ => None
    }
  }

  override def create(context: FileSystemContext, config: Map[String, String], meta: Metadata): FileBasedMetadata = {
    val Metadata(_, encoding, _, leaf) = meta
    val sft = namespaced(meta.sft, context.namespace)
    // load the partition scheme first in case it fails
    val scheme = PartitionSchemeFactory.load(sft, meta.scheme)
    MetadataJson.writeMetadata(context, NamedOptions(name, config))
    FileBasedMetadataFactory.write(context.fc, context.root, meta)
    val directory = new Path(context.root, FileBasedMetadataFactory.MetadataDirectory)
    val metadata = new FileBasedMetadata(context.fc, directory, sft, encoding, scheme, leaf)
    FileBasedMetadataFactory.cache.put(FileBasedMetadataFactory.key(context), metadata)
    metadata
  }
}

object FileBasedMetadataFactory extends MethodProfiling with LazyLogging {

  private val MetadataDirectory = "metadata"
  private val StoragePath = s"$MetadataDirectory/storage.json"

  private val cache = new ConcurrentHashMap[String, FileBasedMetadata]()

  private def key(context: FileSystemContext): String =
    context.namespace.map(ns => s"$ns:${context.root.toUri}").getOrElse(context.root.toUri.toString)

  private def cached(context: FileSystemContext): Option[FileBasedMetadata] = {
    val loader = new java.util.function.Function[String, FileBasedMetadata]() {
      override def apply(ignored: String): FileBasedMetadata = {
        val file = new Path(context.root, StoragePath)
        if (!PathCache.exists(context.fc, file)) { null } else {
          val directory = new Path(context.root, MetadataDirectory)
          val meta = WithClose(context.fc.open(file))(MetadataSerialization.deserialize)
          val leaf = meta.leafStorage
          val sft = namespaced(meta.sft, context.namespace)
          val scheme = PartitionSchemeFactory.load(sft, meta.scheme)
          new FileBasedMetadata(context.fc, directory, sft, meta.encoding, scheme, leaf)
        }
      }
    }
    Option(cache.computeIfAbsent(key(context), loader))
  }

  /**
    * Write basic metadata to disk. This should be done once, when the storage is created
    *
    * @param fc file context
    * @param root root path
    * @param metadata simple feature type, file encoding, partition scheme, etc
    */
  private def write(fc: FileContext, root: Path, metadata: Metadata): Unit = {
    val file = new Path(root, StoragePath)
    if (PathCache.exists(fc, file, reload = true)) {
      throw new IllegalArgumentException(s"Metadata file already exists at path '$file'")
    }
    WithClose(fc.create(file, java.util.EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent)) { out =>
      MetadataSerialization.serialize(out, metadata)
      out.hflush()
      out.hsync()
    }
    PathCache.register(fc, file)
  }
}
