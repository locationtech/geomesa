/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import java.util.concurrent.{Delayed, ScheduledFuture, TimeUnit}

object ExpirationMocking {

  class WrappedRunnable(val delay: Long) {
    var runnable: Runnable = _
    var cancelled: Boolean = false
    var done: Boolean = false
  }

  class ScheduledExpiry[T](runnable: WrappedRunnable) extends ScheduledFuture[T] {
    override def getDelay(unit: TimeUnit): Long = unit.convert(runnable.delay, TimeUnit.MILLISECONDS)
    override def compareTo(o: Delayed): Int = java.lang.Long.compare(runnable.delay, o.getDelay(TimeUnit.MILLISECONDS))
    override def cancel(mayInterruptIfRunning: Boolean): Boolean = { runnable.cancelled = true; true }
    override def isCancelled: Boolean = runnable.cancelled
    override def isDone: Boolean = runnable.done
    override def get(): T = runnable.runnable.run().asInstanceOf[T]
    override def get(timeout: Long, unit: TimeUnit): T = runnable.runnable.run().asInstanceOf[T]
  }
}
