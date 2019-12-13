/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream

import java.io.Closeable

import org.locationtech.geomesa.index.utils.{DistributedLocking, Releasable}
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener

/**
  * Manages storing and watching distributed offsets
  */
trait OffsetManager extends DistributedLocking with Closeable {
  def getOffset(topic: String, partition: Int): Long
  def setOffset(topic: String, partition: Int, offset: Long): Unit
  def deleteOffsets(topic: String): Unit
  def acquireLock(topic: String, partition: Int, timeOut: Long): Option[Releasable] =
    acquireDistributedLock(s"$topic/$partition", timeOut)
  def addOffsetListener(topic: String, listener: OffsetListener): Unit
  def removeOffsetListener(topic: String, listener: OffsetListener): Unit
}

object OffsetManager {
  trait OffsetListener {
    def offsetChanged(partition: Int, offset: Long): Unit
  }
}
