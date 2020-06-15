/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.concurrent

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.typesafe.scalalogging.LazyLogging

/**
 * Executor service that will grow up to the number of threads specified.
 *
 * Assertion: core java executors offer 3 features that we want, but you can only use two of them at once:
 *   1. Add threads as needed but re-use idle threads if available
 *   2. Limit the total number of threads available
 *   3. Accept and queue tasks once the thread limit is reached
 *
 * This executor allows us to do all three. It is backed by an unlimited, cached thread pool that will handle
 * re-use and expiration of threads, while it tracks the number of threads used. It is fairly lightweight, and
 * can be instantiated for a given task and then shutdown after use.
 *
 * @param maxThreads max threads to use at once
 */
class CachedThreadPool(maxThreads: Int) extends AbstractExecutorService with LazyLogging {

  @volatile
  private var available = maxThreads
  private val queue = new java.util.LinkedList[TrackableFutureTask[_]]()
  private val tasks = new java.util.HashSet[Future[_]]()
  private val stopped = new AtomicBoolean(false)
  private val lock = new ReentrantLock()
  private val done = lock.newCondition()

  override def shutdown(): Unit = stopped.set(true)

  override def shutdownNow(): java.util.List[Runnable] = {
    stopped.set(true)
    lock.lock()
    try {
      val waiting = new java.util.ArrayList[Runnable](queue)
      queue.clear()
      // copy the running tasks to prevent concurrent modification errors in the synchronous cancel call
      val running = new java.util.ArrayList[Future[_]](tasks).iterator()
      while (running.hasNext) {
        running.next.cancel(true)
      }
      waiting
    } finally {
      lock.unlock()
    }
  }

  override def isShutdown: Boolean = stopped.get

  override def isTerminated: Boolean = stopped.get && available == maxThreads // should be safe to read a volatile primitive

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
    lock.lock()
    try {
      if (isTerminated) { true } else {
        done.await(timeout, unit)
        isTerminated
      }
    } finally {
      lock.unlock()
    }
  }

  override def execute(command: Runnable): Unit = {
    if (stopped.get) {
      throw new RejectedExecutionException("Trying to execute a task but executor service is shut down")
    }
    val task = command match {
      case t: TrackableFutureTask[_] => t
      case c => newTaskFor(c, null)
    }
    runOrQueueTask(task)
  }

  private def runOrQueueTask(task: TrackableFutureTask[_]): Unit = {
    lock.lock()
    try {
      if (available > 0) {
        // note that we could fairly easily create a global thread limit by backing this with a different pool
        try { CachedThreadPool.pool.execute(task) } catch {
          case e: RejectedExecutionException =>
            // we still need to execute the task to fulfill the executor API
            logger.warn(
              "CachedThreadPool rejected queued task (likely due to shutdown)," +
                s"creating new single thread executor: $task: $e")
            val pool = Executors.newSingleThreadExecutor()
            pool.execute(task)
            pool.shutdown()
        }
        available -= 1
        tasks.add(task)
      } else {
        queue.offer(task) // unbounded queue so should always succeed
      }
    } finally {
      lock.unlock()
    }
  }

  override protected def newTaskFor[T](runnable: Runnable, value: T): TrackableFutureTask[T] =
    new TrackableFutureTask[T](runnable, value)

  class TrackableFutureTask[T](runnable: Runnable, result: T) extends FutureTask[T](runnable, result) {
    override def done(): Unit = {
      lock.lock()
      try {
        available += 1
        val next = queue.poll()
        if (next != null) {
          runOrQueueTask(next) // note: this may briefly use more than maxThreads as this thread finishes up
        } else if (isTerminated) {
          CachedThreadPool.this.done.signalAll()
        }
      } finally {
        lock.unlock()
      }
    }

    override def toString: String = s"TrackableFutureTask[$runnable]"
  }
}

object CachedThreadPool {

  // unlimited size but re-uses cached threads
  private val pool = ExitingExecutor(Executors.newCachedThreadPool().asInstanceOf[ThreadPoolExecutor])

  /**
   * Execute a single command in a potentially cached thread
   *
   * @param command command
   */
  def execute(command: Runnable): Unit = pool.execute(command)

  /**
   * Submit a single command to run in a potentially cached thread
   *
   * @param command command
   * @return
   */
  def submit(command: Runnable): Future[_] = pool.submit(command)
}
