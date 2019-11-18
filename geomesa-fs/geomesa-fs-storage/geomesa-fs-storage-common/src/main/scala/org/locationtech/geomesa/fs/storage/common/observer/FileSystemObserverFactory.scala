/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common
package observer

import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.utils.io.{CloseQuietly, FlushQuietly}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Factory for observing file writes
 */
trait FileSystemObserverFactory {
  def apply(sft: SimpleFeatureType, partition: String, path: Path): FileSystemObserver
}

object FileSystemObserverFactory {

  def apply(sft: SimpleFeatureType): Seq[FileSystemObserverFactory] =
    sft.getObservers.map(o => Class.forName(o).newInstance().asInstanceOf[FileSystemObserverFactory])

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
