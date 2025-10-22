/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common
package observer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.fs.storage.common.observer.FileSystemObserverFactory.FactoryBridge
import org.locationtech.geomesa.utils.io.{CloseQuietly, FlushQuietly}

import java.io.Closeable

/**
 * Factory for observing file writes
 */
@deprecated("moved to org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserverFactory")
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

  def bridge(): org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserverFactory = new FactoryBridge(this)
}

@deprecated("moved to org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserverFactory")
object FileSystemObserverFactory {

  @deprecated("moved to org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserverFactory")
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
  @deprecated("moved to org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserverFactory")
  class CompositeObserver(observers: Seq[FileSystemObserver]) extends FileSystemObserver {
    override def write(feature: SimpleFeature): Unit = observers.foreach(_.write(feature))
    override def flush(): Unit = FlushQuietly(observers).foreach(e => throw e)
    override def close(): Unit = CloseQuietly(observers).foreach(e => throw e)
  }

  private class FactoryBridge(delegate: FileSystemObserverFactory)
      extends org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserverFactory {
    override def init(conf: Configuration, root: Path, sft: SimpleFeatureType): Unit = delegate.init(conf, root, sft)
    override def apply(path: Path): org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserver =
      new ObserverBridge(delegate.apply(path))
    override def close(): Unit = delegate.close()
  }

  private class ObserverBridge(delegate: FileSystemObserver)
      extends org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserver {
    override def apply(feature: SimpleFeature): Unit = delegate.write(feature)
    override def flush(): Unit = delegate.flush()
    override def close(): Unit = delegate.close()
  }
}
