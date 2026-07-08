/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.utils

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.core.utils.MultiPartitionAction.ActionHolder
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorage, Partition}
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging}

import java.io.{Closeable, Flushable}
import java.util.Map.Entry
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BiFunction
import scala.util.control.NonFatal

/**
 * Multi partition actions
 *
 * @param storage file system storage instance
 * @param maxOpenPartitions max open partition writers
 */
abstract class MultiPartitionAction[T <: Closeable with Flushable](storage: FileSystemStorage, maxOpenPartitions: Int)
    extends (SimpleFeature => Unit) with Closeable with Flushable with LazyLogging {

  @volatile
  private var closed = false

  private val perPartition = new ConcurrentHashMap[Partition, ActionHolder[T]]()
  private val oldest = MultiPartitionAction.WriterHolder.oldest[T]

  private lazy val logId = getClass.getSimpleName

  protected def createAction(partition: Partition): T
  protected def apply(action: T, feature: SimpleFeature): Unit

  override def apply(feature: SimpleFeature): Unit = {
    val partition = Partition(storage.schemes.map(_.getPartition(feature)))
    apply(perPartition.compute(partition, ComputeForAction).action, feature)
    if (perPartition.size() > maxOpenPartitions) {
      val oldest = perPartition.reduceEntries(maxOpenPartitions / 2, this.oldest)
      if (perPartition.remove(oldest.getKey, oldest.getValue)) {
        logger.debug(
          s"Closing $logId for partition ${oldest.getKey} (last accessed ${oldest.getValue.age}ms ago) due to hitting max " +
            s"open threshold $maxOpenPartitions")
        CloseWithLogging(oldest.getValue)
      }
    }
  }

  override def flush(): Unit = {
    var ex: Throwable = null
    perPartition.entrySet().forEach { entry =>
      try { perPartition.computeIfPresent(entry.getKey, ComputeForFlush) } catch {
        case NonFatal(e) =>  if (ex == null) { ex = e } else { ex.addSuppressed(e) }
      }
    }
    if (ex != null) {
      throw ex
    }
  }

  override def close(): Unit = {
    closed = true
    var ex: Throwable = null
    perPartition.forEach { (p, writer) =>
      logger.debug(s"Closing $logId for partition $p")
      CloseQuietly(writer).foreach(e => if (ex == null) { ex = e } else { ex.addSuppressed(e) })
    }
    if (ex != null) {
      throw ex
    }
  }

  private object ComputeForAction extends BiFunction[Partition, ActionHolder[T], ActionHolder[T]] {
    override def apply(p: Partition, current: ActionHolder[T]): ActionHolder[T] = {
      if (current == null) {
        logger.debug(s"Opening $logId for partition $p")
        new ActionHolder(createAction(p))
      } else {
        current.updateAccessTime() // update access time before returning so it's not expired
        current
      }
    }
  }

  private object ComputeForFlush extends BiFunction[Partition, ActionHolder[T], ActionHolder[T]] {
    override def apply(p: Partition, current: ActionHolder[T]): ActionHolder[T] = {
      if (current != null) {
        logger.debug(s"Flushing $logId for partition $p")
        current.flush()
      }
      current
    }
  }
}

object MultiPartitionAction extends LazyLogging {

  private class ActionHolder[T <: Closeable with Flushable](val action: T) extends Closeable with Flushable {

    @volatile
    private var lastAccess: Long = System.currentTimeMillis()
    private val closed: AtomicBoolean = new AtomicBoolean(false)

    def age: Long = System.currentTimeMillis() - lastAccess
    def updateAccessTime(): Unit = lastAccess = System.currentTimeMillis()

    override def flush(): Unit = action.flush()

    override def close(): Unit = {
      if (closed.compareAndSet(false, true)) {
        action.close()
      }
    }
  }

  private object WriterHolder {
    def oldest[T <: Closeable with Flushable]: BiFunction[Entry[Partition, ActionHolder[T]], Entry[Partition, ActionHolder[T]], Entry[Partition, ActionHolder[T]]] =
      (left, right) => if (right.getValue.age < left.getValue.age) { right } else { left }
  }
}
