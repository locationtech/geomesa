/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.zk

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.{Lock, ReentrantLock}

import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.locationtech.geomesa.index.utils.{DistributedLocking, Releasable}

trait ZookeeperLocking extends DistributedLocking {

  protected def mock: Boolean
  protected def zookeepers: String

  /**
    * Gets and acquires a distributed lock based on the key.
    * Make sure that you 'release' the lock in a finally block.
    *
    * @param key key to lock on - equivalent to a path in zookeeper
    * @return the lock
    */
  override protected def acquireDistributedLock(key: String): Releasable = {
    if (mock) {
      val lock = ZookeeperLocking.mockLocks.synchronized {
        ZookeeperLocking.mockLocks.getOrElseUpdate(key, new ReentrantLock())
      }
      lock.lock()
      Releasable(lock)
    } else {
      val (client, lock) = distributedLock(key)
      try {
        lock.acquire()
        ZookeeperLocking.releasable(lock, client)
      } catch {
        case e: Exception => client.close(); throw e
      }
    }
  }

  /**
    * Gets and acquires a distributed lock based on the key.
    * Make sure that you 'release' the lock in a finally block.
    *
    * @param key key to lock on - equivalent to a path in zookeeper
    * @param timeOut how long to wait to acquire the lock, in millis
    * @return the lock, if obtained
    */
  override protected def acquireDistributedLock(key: String, timeOut: Long): Option[Releasable] = {
    if (mock) {
      val lock = ZookeeperLocking.mockLocks.synchronized {
        ZookeeperLocking.mockLocks.getOrElseUpdate(key, new ReentrantLock())
      }
      if (lock.tryLock(timeOut, TimeUnit.MILLISECONDS)) {
        Some(Releasable(lock))
      } else {
        None
      }
    } else {
      val (client, lock) = distributedLock(key)
      try {
        if (lock.acquire(timeOut, TimeUnit.MILLISECONDS)) {
          Some(ZookeeperLocking.releasable(lock, client))
        } else {
          None
        }
      } catch {
        case e: Exception => client.close(); throw e
      }
    }
  }

  private def distributedLock(key: String): (CuratorFramework, InterProcessSemaphoreMutex) = {
    val lockPath = if (key.startsWith("/")) key else s"/$key"
    val backOff = new ExponentialBackoffRetry(1000, 3)
    val client = CuratorFrameworkFactory.newClient(zookeepers, backOff)
    client.start()
    val lock = new InterProcessSemaphoreMutex(client, lockPath)
    (client, lock)
  }
}

object ZookeeperLocking {

  private lazy val mockLocks = scala.collection.mutable.Map.empty[String, Lock]

  // delegate lock that will close the curator client upon release
  def releasable(lock: InterProcessSemaphoreMutex, client: CuratorFramework): Releasable =
    new Releasable { override def release(): Unit = try { lock.release() } finally { client.close() } }
}
