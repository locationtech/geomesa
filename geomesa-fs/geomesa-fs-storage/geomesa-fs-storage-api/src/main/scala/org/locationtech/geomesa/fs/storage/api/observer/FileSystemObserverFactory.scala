/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api.observer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.Closeable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
 * Factory for observing file writes
 */
trait FileSystemObserverFactory extends Closeable {

  /**
   * Called once after instantiating the factory
   *
   * @param conf hadoop configuration
   * @param root root path
   * @param sft simple feature type
   */
  def init(conf: Configuration, root: Path, sft: SimpleFeatureType): Unit

  /**
   * Create an observer for the given path
   *
   * @param path file path being written
   * @return
   */
  def apply(path: Path): FileSystemObserver
}

object FileSystemObserverFactory {

  object NoOpObserver extends FileSystemObserver {
    override def apply(feature: SimpleFeature): Unit = {}
    override def flush(): Unit = {}
    override def close(): Unit = {}
  }

  object CompositeObserver {
    def apply(observers: Seq[FileSystemObserver]): FileSystemObserver = {
      val seq = observers.flatMap {
        case NoOpObserver => Seq.empty
        case o: CompositeObserver => o.observers
        case o => Seq(o)
      }
      if (seq.isEmpty) {
        NoOpObserver
      } else if (seq.lengthCompare(1) == 0) {
        seq.head
      } else {
        new CompositeObserver(seq)
      }
    }
  }

  /**
   * Composite observer
   *
   * @param observers observers
   */
  class CompositeObserver(val observers: Seq[FileSystemObserver]) extends FileSystemObserver {

    override def apply(feature: SimpleFeature): Unit = observers.foreach(_.apply(feature))

    override def flush(): Unit = {
      val errors = ArrayBuffer.empty[Throwable]
      observers.foreach { o =>
        Try(o.flush()).failed.foreach(errors.+=)
      }
      if (errors.nonEmpty) {
        val e = errors.head
        errors.tail.foreach(e.addSuppressed)
        throw e
      }
    }

    override def close(): Unit = {
      val errors = ArrayBuffer.empty[Throwable]
      observers.foreach { o =>
        Try(o.close()).failed.foreach(errors.+=)
      }
      if (errors.nonEmpty) {
        val e = errors.head
        errors.tail.foreach(e.addSuppressed)
        throw e
      }
    }
  }
}
