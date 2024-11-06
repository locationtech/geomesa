/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadata.Config
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling

import java.util.concurrent.ConcurrentHashMap

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
    val cached = FileBasedMetadataFactory.cached(context, json.getOrElse(FileBasedMetadata.LegacyOptions).options)
    json match {
      case Some(m) if m.name.equalsIgnoreCase(name) =>
        cached.orElse(throw new IllegalArgumentException(s"Could not load metadata at root '${context.root.toUri}'"))

      case None if cached.isDefined =>
        // a file-based metadata impl exists, but was created with an older version
        // create a config file pointing to it, and register that
        MetadataJson.writeMetadata(context, FileBasedMetadata.LegacyOptions)
        cached

      case _ => None
    }
  }

  override def create(context: FileSystemContext, config: Map[String, String], meta: Metadata): FileBasedMetadata = {
    val sft = namespaced(meta.sft, context.namespace)
    // load the partition scheme first in case it fails
    PartitionSchemeFactory.load(sft, meta.scheme)
    val renderer = config.get(Config.RenderKey).map(MetadataConverter.apply).getOrElse(RenderCompact)
    MetadataJson.writeMetadata(context, NamedOptions(name, config + (Config.RenderKey -> renderer.name)))
    FileBasedMetadataFactory.write(context.fs, context.root, meta)
    val directory = new Path(context.root, FileBasedMetadataFactory.MetadataDirectory)
    val metadata = new FileBasedMetadata(context.fs, directory, sft, meta, renderer)
    FileBasedMetadataFactory.cache.put(FileBasedMetadataFactory.key(context), metadata)
    metadata
  }
}

object FileBasedMetadataFactory extends MethodProfiling with LazyLogging {

  val MetadataDirectory = "metadata"
  val StoragePath = s"$MetadataDirectory/storage.json"

  private val cache = new ConcurrentHashMap[String, FileBasedMetadata]()

  private def key(context: FileSystemContext): String =
    context.namespace.map(ns => s"$ns:${context.root.toUri}").getOrElse(context.root.toUri.toString)

  private def cached(context: FileSystemContext, config: Map[String, String]): Option[FileBasedMetadata] = {
    val loader = new java.util.function.Function[String, FileBasedMetadata]() {
      override def apply(ignored: String): FileBasedMetadata = {
        val file = new Path(context.root, StoragePath)
        if (!PathCache.exists(context.fs, file)) { null } else {
          val directory = new Path(context.root, MetadataDirectory)
          val meta = WithClose(context.fs.open(file))(MetadataSerialization.deserialize)
          val sft = namespaced(meta.sft, context.namespace)
          val renderer = config.get(Config.RenderKey).map(MetadataConverter.apply).getOrElse(RenderPretty)
          new FileBasedMetadata(context.fs, directory, sft, meta, renderer)
        }
      }
    }
    Option(cache.computeIfAbsent(key(context), loader))
  }

  /**
    * Write basic metadata to disk. This should be done once, when the storage is created
    *
    * @param fs file system
    * @param root root path
    * @param metadata simple feature type, file encoding, partition scheme, etc
    */
  private [metadata] def write(fs: FileSystem, root: Path, metadata: Metadata): Unit = {
    val file = new Path(root, StoragePath)
    WithClose(fs.create(file, true)) { out =>
      MetadataSerialization.serialize(out, metadata)
      out.hflush()
      out.hsync()
    }
    PathCache.register(fs, file)
  }
}
