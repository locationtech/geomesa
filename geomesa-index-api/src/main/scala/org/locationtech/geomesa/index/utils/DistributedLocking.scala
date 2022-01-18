/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.{Lock, ReentrantLock}

trait DistributedLocking {

  /**
    * Gets and acquires a distributed lock based on the key.
    * Make sure that you 'release' the lock in a finally block.
    *
    * @param key key to lock on - equivalent to a path in zookeeper
    * @return the lock
    */
  protected def acquireDistributedLock(key: String): Releasable

  /**
    * Gets and acquires a distributed lock based on the key.
    * Make sure that you 'release' the lock in a finally block.
    *
    * @param key key to lock on - equivalent to a path in zookeeper
    * @param timeOut how long to wait to acquire the lock, in millis
    * @return the lock, if obtained
    */
  protected def acquireDistributedLock(key: String, timeOut: Long): Option[Releasable]

  /**
    * Execute a function wrapped in a lock
    *
    * @param key key to lock on
    * @param fn function to run with the lock
    * @tparam T result type
    * @return
    */
  protected def withLock[T](key: String, fn: => T): T = {
    val lock = acquireDistributedLock(key)
    try { fn } finally { lock.release() }
  }

  /**
    * Execute a function wrapped in a lock
    *
    * @param key key to lock on
    * @param timeOut how long to wait to acquire the lock, in millis
    * @param fn function to run with the lock
    * @param fallback function to run if the lock could not be acquired
    * @tparam T result type
    * @return
    */
  protected def withLock[T](key: String, timeOut: Long, fn: => T, fallback: => T): T = {
    acquireDistributedLock(key, timeOut) match {
      case None => fallback
      case Some(lock) => try { fn } finally { lock.release() }
    }
  }
}

trait LocalLocking extends DistributedLocking {

  import LocalLocking.locks

  override protected def acquireDistributedLock(key: String): Releasable = {
    val lock = locks.synchronized(locks.getOrElseUpdate(key, new ReentrantLock()))
    lock.lock()
    Releasable(lock)
  }

  override protected def acquireDistributedLock(key: String, timeOut: Long): Option[Releasable] = {
    val lock = locks.synchronized(locks.getOrElseUpdate(key, new ReentrantLock()))
    if (lock.tryLock(timeOut, TimeUnit.MILLISECONDS)) { Some(Releasable(lock)) } else { None }
  }
}

object LocalLocking {
  private val locks = scala.collection.mutable.Map.empty[String, Lock]
}

trait Releasable {
  def release(): Unit
}

object Releasable {
  def apply(lock: Lock): Releasable = new Releasable { override def release(): Unit = lock.unlock() }
}
