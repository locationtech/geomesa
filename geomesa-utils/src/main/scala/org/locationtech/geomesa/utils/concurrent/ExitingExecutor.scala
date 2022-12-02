/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.concurrent

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ThreadFactory, ThreadPoolExecutor, TimeUnit}

object ExitingExecutor {

  /**
   * Get an executor service that will not stop the JVM from exiting
   *
   * @param executor executor
   * @param force force executor to `shutdownNow`, or wait for clean `shutdown`
   * @tparam T type bounds
   * @return
   */
  def apply[T <: ThreadPoolExecutor](executor: T, force: Boolean = false): T = {
    executor.setThreadFactory(new DaemonThreadFactory(executor.getThreadFactory))
    sys.addShutdownHook {
      if (force) {
        executor.shutdownNow()
      } else {
        executor.shutdown()
      }
      try { executor.awaitTermination(120, TimeUnit.SECONDS) } catch {
        case _: InterruptedException => // ignore
      }
    }
    executor
  }

  class DaemonThreadFactory(underlying: ThreadFactory) extends ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val thread = underlying.newThread(r)
      thread.setDaemon(true)
      thread
    }
  }

  /**
   * Thread factory that creates threads with custom names putting a thread number
   * replacing `%d` pattern. For example `thread-%d`, will produce `thread-1`, `thread-2`, ... threads
   * @param namePattern Thread name
   */
  class NamedThreadFactory(namePattern: String) extends ThreadFactory {
    require(namePattern.contains("%d"), "name pattern should contain %d to set the thread number")
    private val group = Option(System.getSecurityManager)
      .map(_.getThreadGroup)
      .getOrElse(Thread.currentThread.getThreadGroup)
    private val counter = new AtomicInteger(0)
    override def newThread(r: Runnable): Thread = {
      val thread = new Thread(group, r, namePattern.format(counter.getAndIncrement()), 0)
      if (thread.isDaemon) thread.setDaemon(false)
      if (thread.getPriority != Thread.NORM_PRIORITY) thread.setPriority(Thread.NORM_PRIORITY)
      thread
    }
  }
}
