/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.MoreExecutors
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.hadoop.fs.{CreateFlag, FileContext, Path}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionMetadata
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadataFactory.MetadataLoader
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.opengis.feature.simple.SimpleFeatureType

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
  override def load(context: FileSystemContext): Option[StorageMetadata] = {
    val json = MetadataJson.readMetadata(context)
    // note: do this after loading the json to allow for old metadata transition
    val cached = FileBasedMetadataFactory.cached(context)
    val option = json match {
      case Some(m) if m.name.equalsIgnoreCase(name) =>
        cached.orElse(throw new IllegalArgumentException(s"Could not load metadata at root '${context.root.toUri}'"))

      case None if cached.isDefined =>
        // a file-based metadata impl exists, but was created with an older version
        // create a config file pointing to it, and register that
        MetadataJson.writeMetadata(context, FileBasedMetadata.DefaultOptions)
        cached

      case _ => None
    }
    option.map(_.reference())
  }

  override def create(context: FileSystemContext, config: Map[String, String], meta: Metadata): StorageMetadata = {
    val Metadata(_, encoding, _, leaf) = meta
    val sft = namespaced(meta.sft, context.namespace)
    // load the partition scheme first in case it fails
    val scheme = PartitionSchemeFactory.load(sft, meta.scheme)
    MetadataJson.writeMetadata(context, NamedOptions(name, config))
    FileBasedMetadataFactory.write(context.fc, context.root, meta)
    val directory = new Path(context.root, FileBasedMetadataFactory.MetadataDirectory)
    val loader = new MetadataLoader(new FileBasedMetadata(context.fc, directory, sft, encoding, scheme, leaf))
    FileBasedMetadataFactory.cache.put(FileBasedMetadataFactory.key(context), loader)
    loader.reference()
  }
}

object FileBasedMetadataFactory extends MethodProfiling with LazyLogging {

  private val MetadataDirectory = "metadata"
  private val StoragePath = s"$MetadataDirectory/storage.json"

  private val executor = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1))

  private val cache = new ConcurrentHashMap[String, MetadataLoader]()

  private def key(context: FileSystemContext): String =
    context.namespace.map(ns => s"$ns:${context.root.toUri}").getOrElse(context.root.toUri.toString)

  private def cached(context: FileSystemContext): Option[MetadataLoader] = {
    val loader = new java.util.function.Function[String, MetadataLoader]() {
      override def apply(ignored: String): MetadataLoader = {
        val file = new Path(context.root, StoragePath)
        if (!PathCache.exists(context.fc, file)) { null } else {
          val directory = new Path(context.root, MetadataDirectory)
          val meta = WithClose(context.fc.open(file))(MetadataSerialization.deserialize)
          val leaf = meta.leafStorage
          val sft = namespaced(meta.sft, context.namespace)
          val scheme = PartitionSchemeFactory.load(sft, meta.scheme)
          new MetadataLoader(new FileBasedMetadata(context.fc, directory, sft, meta.encoding, scheme, leaf))
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

  /**
    * Manages periodic re-loading of a shared metadata instance. Ensures that instances which do not
    * have any current references will not keep reloading.
    *
    * @param metadata shared metadata instance
    */
  private class MetadataLoader(val metadata: FileBasedMetadata) {

    // track references to this shared instance
    private var references: Int = 0
    private var reloader: ScheduledFuture[_] = _

    /**
      * Register a reference to this metadata - used to track closing over shared instances
      */
    def reference(): StorageMetadata = synchronized {
      references += 1
      if (references == 1) {
        val runnable = new Runnable() { override def run(): Unit = metadata.reload() }
        val delay = PathCache.CacheDurationProperty.toDuration.get.toMillis
        reloader = executor.scheduleWithFixedDelay(runnable, delay, delay, TimeUnit.MILLISECONDS)
      }
      new ReferencedMetadata(this)
    }

    /**
      * Deregister a reference to this metadata - used to track closing over shared instances
      */
    def dereference(): Unit = synchronized {
      require(references != 0, "Called `deregister` without first invoking `register`")
      references -= 1
      if (references == 0) {
        reloader.cancel(true)
        reloader = null
      }
    }
  }

  /**
    * Wrapper class for the shared metadata instance. Ensures that multiple calls to `close` will
    * only deregister once
    */
  private class ReferencedMetadata(loader: MetadataLoader) extends StorageMetadata {
    private val closed = new AtomicBoolean(false)
    override def sft: SimpleFeatureType = loader.metadata.sft
    override def encoding: String = loader.metadata.encoding
    override def scheme: PartitionScheme = loader.metadata.scheme
    override def leafStorage: Boolean = loader.metadata.leafStorage
    override def getPartition(name: String): Option[PartitionMetadata] = loader.metadata.getPartition(name)
    override def getPartitions(prefix: Option[String]): Seq[PartitionMetadata] = loader.metadata.getPartitions(prefix)
    override def addPartition(partition: PartitionMetadata): Unit = loader.metadata.addPartition(partition)
    override def removePartition(partition: PartitionMetadata): Unit = loader.metadata.removePartition(partition)
    override def reload(): Unit = loader.metadata.reload()
    override def compact(partition: Option[String], threads: Int): Unit = loader.metadata.compact(partition, threads)
    override def close(): Unit = if (closed.compareAndSet(false, true)) { loader.dereference() }
  }
}
