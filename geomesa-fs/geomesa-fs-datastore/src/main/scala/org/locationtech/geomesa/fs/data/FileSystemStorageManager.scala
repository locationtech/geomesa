/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import java.util.concurrent.ConcurrentHashMap

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.locationtech.geomesa.utils.stats.MethodProfiling

import scala.util.control.NonFatal

/**
  * Manages the storages and associated simple feature types underneath a given path
  *
  * @param fc file context
  * @param conf configuration
  * @param root root path for the data store
  */
class FileSystemStorageManager private (fc: FileContext, conf: Configuration, root: Path, namespace: Option[String])
    extends MethodProfiling with LazyLogging {

  import scala.collection.JavaConverters._

  private val cache = new ConcurrentHashMap[String, (Path, FileSystemStorage)]().asScala

  /**
    * Gets the storage associated with the given simple feature type, if any
    *
    * @param typeName simple feature type name
    * @return
    */
  def storage(typeName: String): Option[FileSystemStorage] = {
    cache.get(typeName).map(_._2) // check cached values
        .orElse(Some(defaultPath(typeName)).filter(PathCache.exists(fc, _)).flatMap(loadPath)) // check expected (default) path
        .orElse(loadAll().find(_.metadata.sft.getTypeName == typeName)) // check other paths until we find it
  }

  /**
    * Gets all storages under the root path
    *
    * @return
    */
  def storages(): Seq[FileSystemStorage] = {
    loadAll().foreach(_ => Unit) // force loading of everything
    cache.map { case (_, (_, storage)) => storage }.toSeq
  }

  /**
    * Caches a storage instance for future use. Avoids loading it a second time if referenced later.
    *
    * @param path path for the storage
    * @param storage storage instance
    */
  def register(path: Path, storage: FileSystemStorage): Unit =
    cache.put(storage.metadata.sft.getTypeName, (path, storage))

  /**
    * Default path for a given simple feature type name. Generally the simple feature type will go under
    * a folder with the type name, but this is not required
    *
    * @param typeName simple feature type name
    * @return
    */
  def defaultPath(typeName: String): Path = new Path(root, typeName)

  /**
    * Loads all storages under this root (if they aren't already loaded)
    *
    * @return
    */
  private def loadAll(): Iterator[FileSystemStorage] = {
    if (!PathCache.exists(fc, root)) { Iterator.empty } else {
      val dirs = PathCache.list(fc, root).filter(_.isDirectory).map(_.getPath)
      dirs.filterNot(path => cache.exists { case (_, (p, _)) => p == path }).flatMap(loadPath)
    }
  }

  /**
    * Attempt to load a storage under the given root path. Requires an appropriate storage implementation
    * to be available on the classpath.
    *
    * @param path storage root path
    * @return
    */
  private def loadPath(path: Path): Option[FileSystemStorage] = {

    def complete(storage: Option[FileSystemStorage], time: Long): Unit =
      logger.debug(s"${ if (storage.isDefined) "Loaded" else "No" } storage at path '$path' in ${time}ms")

    profile(complete _) {
      val context = FileSystemContext(fc, conf, path, namespace)
      StorageMetadataFactory.load(context).map { meta =>
        try {
          val storage = FileSystemStorageFactory(context, meta)
          meta.reload() // ensure metadata is loaded
          register(path, storage)
          storage
        } catch {
          case NonFatal(e) => CloseQuietly(meta).foreach(e.addSuppressed); throw e
        }
      }
    }
  }
}

object FileSystemStorageManager {

  private val cache = Caffeine.newBuilder().build(
    new CacheLoader[(FileContext, Configuration, Path, Option[String]), FileSystemStorageManager]() {
      override def load(key: (FileContext, Configuration, Path, Option[String])): FileSystemStorageManager =
        new FileSystemStorageManager(key._1, key._2, key._3, key._4)
    }
  )

  /**
    * Load a cached storage manager instance
    *
    * @param fc file context
    * @param conf configuration
    * @param root data store root path
    * @return
    */
  def apply(fc: FileContext, conf: Configuration, root: Path, namespace: Option[String]): FileSystemStorageManager =
    cache.get((fc, conf, root, namespace))
}
