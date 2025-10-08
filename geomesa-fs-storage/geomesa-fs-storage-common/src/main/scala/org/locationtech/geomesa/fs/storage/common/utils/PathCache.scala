/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.utils

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, RemoteIterator}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import java.io.FileNotFoundException
import java.lang
import java.lang.reflect.Method
import java.util.concurrent.{CompletableFuture, Executor, TimeUnit}
import java.util.function.BiConsumer

/**
  * Caches file statuses to avoid repeated file system operations. Status expires after a
  * configurable period, by default 10 minutes.
  */
object PathCache extends LazyLogging {

  val CacheDurationProperty: SystemProperty = SystemProperty("geomesa.fs.file.cache.duration", "15 minutes")

  private val duration = CacheDurationProperty.toDuration.get.toMillis

  // cache for checking existence of files
  private val pathCache: LoadingCache[(FileSystem, Path), lang.Boolean] =
    Caffeine.newBuilder().expireAfterWrite(duration, TimeUnit.MILLISECONDS).build(
      new CacheLoader[(FileSystem, Path), java.lang.Boolean]() {
        override def load(key: (FileSystem, Path)): java.lang.Boolean = key._1.exists(key._2)
      }
    )

  // cache for checking directory contents
  private val listCache: LoadingCache[(FileSystem, Path), Stream[FileStatus]] =
    Caffeine.newBuilder().expireAfterWrite(duration, TimeUnit.MILLISECONDS).build(
      new CacheLoader[(FileSystem, Path), Stream[FileStatus]]() {
        override def load(key: (FileSystem, Path)): Stream[FileStatus] =
          RemoteIterator(key._1.listStatusIterator(key._2)).toStream
      }
    )

  // cache for individual file status
  private val statusCache: LoadingCache[(FileSystem, Path), FileStatus] =
    Caffeine.newBuilder().expireAfterWrite(duration, TimeUnit.MILLISECONDS).build(
      new CacheLoader[(FileSystem, Path), FileStatus]() {
        override def load(key: (FileSystem, Path)): FileStatus = {
          try { key._1.getFileStatus(key._2) } catch {
              case _: FileNotFoundException => null
          }
        }

        override def asyncLoad(key: (FileSystem, Path), executor: Executor): CompletableFuture[FileStatus] = {
          super.asyncLoad(key, executor)
            .whenCompleteAsync(new ListCacheRefresh(key), executor)
            .asInstanceOf[CompletableFuture[FileStatus]]
        }

        override def asyncReload(
            key: (FileSystem, Path),
            oldValue: FileStatus,
            executor: Executor): CompletableFuture[FileStatus] = {
          super.asyncReload(key, oldValue, executor)
            .whenCompleteAsync(new ListCacheRefresh(key), executor)
            .asInstanceOf[CompletableFuture[FileStatus]]
        }
      }
    )

  // we use reflection to get around Caffeine 2.x/3.x API differences in some environments
  private val refresh: Method = classOf[LoadingCache[_, _]].getMethods.find(_.getName == "refresh").getOrElse {
    logger.warn("Could not get refresh cache method - cache operations will be less efficient")
    null
  }

  /**
    * Register a path as existing
    *
    * @param fs file system
    * @param path path
    */
  def register(fs: FileSystem, path: Path): Unit = {
    pathCache.put((fs, path), java.lang.Boolean.TRUE)
    if (refresh != null) {
      refresh.invoke(statusCache, (fs, path)) // also triggers listCache update
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
    * Gets the file status for a path. Path must exist.
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
  def invalidate(fs: FileSystem, path: Path): Unit = {
    Seq(pathCache, statusCache, listCache).foreach(_.invalidate((fs, path)))
    if (path.getParent != null) {
      listCache.invalidate((fs, path.getParent))
    }
  }

  object RemoteIterator {
    def apply[T](iter: RemoteIterator[T]): Iterator[T] = new Iterator[T] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): T = iter.next
    }
  }

  private class ListCacheRefresh(key: (FileSystem, Path)) extends BiConsumer[FileStatus, Throwable] {
    override def accept(status: FileStatus, u: Throwable): Unit = {
      if (status != null) { // could be null if load fails
        val (fs, path) = key
        val parent = path.getParent
        if (parent != null) {
          listCache.asMap().computeIfPresent((fs, parent), load(status)_)
        }
      }
    }

    // noinspection ScalaUnusedSymbol
    private def load(status: FileStatus)(ignored: (FileSystem, Path), list: Stream[FileStatus]): Stream[FileStatus] =
      list.filterNot(f => f.getPath == status.getPath) :+ status
  }
}
