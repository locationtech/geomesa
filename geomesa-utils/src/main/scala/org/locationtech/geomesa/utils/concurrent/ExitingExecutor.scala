/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.concurrent

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
}
