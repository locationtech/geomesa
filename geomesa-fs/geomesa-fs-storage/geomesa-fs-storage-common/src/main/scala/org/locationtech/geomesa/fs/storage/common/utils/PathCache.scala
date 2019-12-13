/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.utils

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.hadoop.fs.{FileContext, FileStatus, Path, RemoteIterator}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

/**
  * Caches file statuses to avoid repeated file system operations. Status expires after a
  * configurable period, by default 10 minutes.
  */
object PathCache {

  val CacheDurationProperty = SystemProperty("geomesa.fs.file.cache.duration", "10 minutes")

  private val duration = CacheDurationProperty.toDuration.get.toMillis

  // cache for checking existence of files
  private val pathCache =
    Caffeine.newBuilder().expireAfterWrite(duration, TimeUnit.MILLISECONDS).build(
          new CacheLoader[(FileContext, Path), java.lang.Boolean]() {
            override def load(key: (FileContext, Path)): java.lang.Boolean = key._1.util.exists(key._2)
          }
        )

  // cache for individual file status
  private val statusCache =
    Caffeine.newBuilder().expireAfterWrite(duration, TimeUnit.MILLISECONDS).build(
      new CacheLoader[(FileContext, Path), FileStatus]() {
        override def load(key: (FileContext, Path)): FileStatus = key._1.getFileStatus(key._2)
      }
    )

  // cache for checking directory contents
  private val listCache =
    Caffeine.newBuilder().expireAfterWrite(duration, TimeUnit.MILLISECONDS).build(
          new CacheLoader[(FileContext, Path), Stream[FileStatus]]() {
            override def load(key: (FileContext, Path)): Stream[FileStatus] =
              RemoteIterator(key._1.listStatus(key._2)).toStream
          }
        )

  /**
    * * Register a path as existing
    *
    * @param fc file context
    * @param path path
    * @param status file status, if available
    * @param list directory contents, if available
    */
  def register(
      fc: FileContext,
      path: Path,
      status: Option[FileStatus] = None,
      list: Option[Stream[FileStatus]] = None): Unit = {
    pathCache.put((fc, path), java.lang.Boolean.TRUE)
    status.foreach(statusCache.put((fc, path), _))
    list.foreach(listCache.put((fc, path), _))
  }

  /**
    * Check to see if a path exists
    *
    * @param fc file context
    * @param path path
    * @param reload reload the file status from the underlying file system before checking
    * @return
    */
  def exists(fc: FileContext, path: Path, reload: Boolean = false): Boolean = {
    if (reload) {
      invalidate(fc, path)
    }
    pathCache.get((fc, path)).booleanValue()
  }

  /**
    * Gets the file status for a path
    *
    * @param fc file context
    * @param path path
    * @return
    */
  def status(fc: FileContext, path: Path): FileStatus = statusCache.get((fc, path))

  /**
    * List the children of a path
    *
    * @param fc file context
    * @param dir directory path
    * @return
    */
  def list(fc: FileContext, dir: Path): Iterator[FileStatus] = listCache.get((fc, dir)).iterator

  /**
    * Invalidate any cached values for the path - they will be re-loaded on next access
    *
    * @param fc file context
    * @param path path
    */
  def invalidate(fc: FileContext, path: Path): Unit =
    Seq(pathCache, statusCache, listCache).foreach(_.invalidate((fc, path)))

  object RemoteIterator {
    def apply[T](iter: RemoteIterator[T]): Iterator[T] = new Iterator[T] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): T = iter.next
    }
  }
}
