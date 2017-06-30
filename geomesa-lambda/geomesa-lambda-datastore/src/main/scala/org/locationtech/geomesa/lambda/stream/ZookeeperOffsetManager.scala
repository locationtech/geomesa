/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream

import java.nio.charset.StandardCharsets
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.locationtech.geomesa.index.utils.Releasable
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener
import org.locationtech.geomesa.lambda.stream.ZookeeperOffsetManager.CuratorOffsetListener
import org.locationtech.geomesa.utils.io.CloseWithLogging

import scala.util.control.NonFatal

class ZookeeperOffsetManager(zookeepers: String, namespace: String = "geomesa") extends OffsetManager {

  import ZookeeperOffsetManager.offsetsPath

  private val client = CuratorFrameworkFactory.builder()
      .namespace(namespace)
      .connectString(zookeepers)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()
  client.start()

  private val listeners = new ConcurrentHashMap[(String, OffsetListener), CuratorOffsetListener]
  private val caches = new ConcurrentHashMap[String, PathChildrenCache]

  override def getOffset(topic: String, partition: Int): Long = {
    val path = ZookeeperOffsetManager.offsetsPath(topic, partition)
    if (client.checkExists().forPath(path) == null) { 0L } else {
      ZookeeperOffsetManager.deserializeOffsets(client.getData.forPath(path))
    }
  }

  override def setOffset(topic: String, partition: Int, offset: Long): Unit = {
    val path = ZookeeperOffsetManager.offsetsPath(topic, partition)
    if (client.checkExists().forPath(path) == null) {
      client.create().creatingParentsIfNeeded().forPath(path)
    }
    client.setData().forPath(path, ZookeeperOffsetManager.serializeOffset(offset))
  }

  override def deleteOffsets(topic: String): Unit = {
    val path = ZookeeperOffsetManager.offsetsPath(topic)
    if (client.checkExists().forPath(path) != null) {
      client.delete().deletingChildrenIfNeeded().forPath(path)
    }
  }

  override def addOffsetListener(topic: String, listener: OffsetListener): Unit = {
    val path = offsetsPath(topic)
    val curatorListener = new CuratorOffsetListener(client, listener, path)
    listeners.put((topic, listener), curatorListener)
    val cache = synchronized {
      var cache = caches.get(topic)
      if (cache == null) {
        cache = new PathChildrenCache(client, path, true)
        cache.start()
        caches.put(topic, cache)
      }
      cache
    }
    cache.getListenable.addListener(curatorListener)
  }

  override def removeOffsetListener(topic: String, listener: OffsetListener): Unit = {
    for {
     listenable <- Option(listeners.remove((topic, listener)))
     cache      <- Option(caches.get(topic))
    } {
      cache.getListenable.removeListener(listenable)
    }
  }

  override def close(): Unit = {
    import scala.collection.JavaConversions._
    caches.values.foreach(CloseWithLogging.apply)
    CloseWithLogging(client)
  }

  override protected def acquireDistributedLock(path: String): Releasable =
    acquireLock(path, (lock) => { lock.acquire(); true })

  override protected def acquireDistributedLock(path: String, timeOut: Long): Option[Releasable] =
    Option(acquireLock(path, (lock) => lock.acquire(timeOut, TimeUnit.MILLISECONDS)))

  private def acquireLock(path: String, acquire: (InterProcessSemaphoreMutex) => Boolean): Releasable = {
    val lock = new InterProcessSemaphoreMutex(client, s"/$path/locks")
    if (acquire(lock)) {
      new Releasable { override def release(): Unit = lock.release() }
    } else {
      null
    }
  }
}

object ZookeeperOffsetManager {

  def serializeOffset(offset: Long): Array[Byte] = offset.toString.getBytes(StandardCharsets.UTF_8)
  def deserializeOffsets(bytes: Array[Byte]): Long = new String(bytes, StandardCharsets.UTF_8).toLong

  private def offsetsPath(topic: String): String = s"/$topic/offsets"
  private def offsetsPath(topic: String, partition: Int): String = s"${offsetsPath(topic)}/$partition"
  private def partitionFromPath(path: String): Int = path.substring(path.lastIndexOf("/") + 1).toInt

  private class CuratorOffsetListener(client: CuratorFramework, listener: OffsetListener, path: String)
      extends PathChildrenCacheListener with LazyLogging {

    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      import PathChildrenCacheEvent.Type.{CHILD_ADDED, CHILD_UPDATED}
      try {
        val eventPath = Option(event.getData).map(_.getPath).getOrElse("")
        if ((event.getType == CHILD_ADDED || event.getType == CHILD_UPDATED) && eventPath.startsWith(path)) {
          logger.trace(s"ZK event triggered for: $eventPath")
          val partition = partitionFromPath(eventPath)
          val offset = ZookeeperOffsetManager.deserializeOffsets(event.getData.getData)
          listener.offsetChanged(partition, offset)
        }
      } catch {
        case NonFatal(e) => logger.warn("Error handling ZK event", e)
      }
    }
  }
}