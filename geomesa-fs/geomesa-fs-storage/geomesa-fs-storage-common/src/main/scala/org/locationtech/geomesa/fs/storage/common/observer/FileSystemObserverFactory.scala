/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common
package observer

import java.io.Closeable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.utils.io.{CloseQuietly, FlushQuietly}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
    override def write(feature: SimpleFeature): Unit = {}
    override def flush(): Unit = {}
    override def close(): Unit = {}
  }

  /**
   * Composite observer
   *
   * @param observers observers
   */
  class CompositeObserver(observers: Seq[FileSystemObserver]) extends FileSystemObserver {
    override def write(feature: SimpleFeature): Unit = observers.foreach(_.write(feature))
    override def flush(): Unit = FlushQuietly(observers).foreach(e => throw e)
    override def close(): Unit = CloseQuietly(observers).foreach(e => throw e)
  }
}
