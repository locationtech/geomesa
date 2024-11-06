/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.utils

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, RemoteIterator}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import java.util.concurrent.TimeUnit

/**
  * Caches file statuses to avoid repeated file system operations. Status expires after a
  * configurable period, by default 10 minutes.
  */
object PathCache {

  val CacheDurationProperty: SystemProperty = SystemProperty("geomesa.fs.file.cache.duration", "15 minutes")

  private val duration = CacheDurationProperty.toDuration.get.toMillis

  // cache for checking existence of files
  private val pathCache =
    Caffeine.newBuilder().expireAfterWrite(duration, TimeUnit.MILLISECONDS).build(
      new CacheLoader[(FileSystem, Path), java.lang.Boolean]() {
        override def load(key: (FileSystem, Path)): java.lang.Boolean = key._1.exists(key._2)
      }
    )

  // cache for individual file status
  private val statusCache =
    Caffeine.newBuilder().expireAfterWrite(duration, TimeUnit.MILLISECONDS).build(
      new CacheLoader[(FileSystem, Path), FileStatus]() {
        override def load(key: (FileSystem, Path)): FileStatus = key._1.getFileStatus(key._2)
      }
    )

  // cache for checking directory contents
  private val listCache =
    Caffeine.newBuilder().expireAfterWrite(duration, TimeUnit.MILLISECONDS).build(
      new CacheLoader[(FileSystem, Path), Stream[FileStatus]]() {
        override def load(key: (FileSystem, Path)): Stream[FileStatus] =
          RemoteIterator(key._1.listStatusIterator(key._2)).toStream
      }
    )

  /**
    * * Register a path as existing
    *
    * @param fs file system
    * @param path path
    */
  def register(fs: FileSystem, path: Path): Unit = {
    pathCache.put((fs, path), java.lang.Boolean.TRUE)
    val status = statusCache.refresh((fs, path))
    val parent = path.getParent
    if (parent != null) {
      listCache.getIfPresent((fs, parent)) match {
        case null => // no-op
        case list => listCache.put((fs, parent), list :+ status.get())
      }
    }
  }

  /**
    * Check to see if a path exists
    *
    * @param fs file system
    * @param path path
    * @param reload reload the file status from the underlying file system before checking
    * @return
    */
  def exists(fs: FileSystem, path: Path, reload: Boolean = false): Boolean = {
    if (reload) {
      invalidate(fs, path)
    }
    pathCache.get((fs, path)).booleanValue()
  }

  /**
    * Gets the file status for a path
    *
    * @param fs file system
    * @param path path
    * @return
    */
  def status(fs: FileSystem, path: Path, reload: Boolean = false): FileStatus = {
    if (reload) {
      invalidate(fs, path)
    }
    statusCache.get((fs, path))
  }

  /**
    * List the children of a path
    *
    * @param fs file system
    * @param dir directory path
    * @return
    */
  def list(fs: FileSystem, dir: Path, reload: Boolean = false): Iterator[FileStatus] = {
    if (reload) {
      invalidate(fs, dir)
    }
    listCache.get((fs, dir)).iterator
  }

  /**
    * Invalidate any cached values for the path - they will be re-loaded on next access
    *
    * @param fs file system
    * @param path path
    */
  def invalidate(fs: FileSystem, path: Path): Unit = Seq(pathCache, statusCache, listCache).foreach(_.invalidate((fs, path)))

  object RemoteIterator {
    def apply[T](iter: RemoteIterator[T]): Iterator[T] = new Iterator[T] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): T = iter.next
    }
  }
}
