/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.fs.storage.api.observer.{FileSystemObserver, FileSystemObserverFactory}
import org.locationtech.geomesa.parquet.NewTestObserverFactory.NewTestObserver

import java.util.Collections
import scala.collection.mutable.ArrayBuffer

class NewTestObserverFactory extends FileSystemObserverFactory {
  override def init(conf: Configuration, root: Path, sft: SimpleFeatureType): Unit = {}
  override def apply(path: Path): FileSystemObserver = {
    val observer = new NewTestObserver(path)
    NewTestObserverFactory.observers += observer
    observer
  }
  override def close(): Unit = {}
}

object NewTestObserverFactory {

  import scala.collection.JavaConverters._

  val observers: scala.collection.mutable.Set[NewTestObserver] =
    Collections.synchronizedSet(new java.util.HashSet[NewTestObserver]()).asScala

  class NewTestObserver(val path: Path) extends FileSystemObserver {

    val features = ArrayBuffer.empty[SimpleFeature]
    var closed = false

    override def apply(feature: SimpleFeature): Unit = features += feature
    override def flush(): Unit = {}
    override def close(): Unit = closed = true
  }
}
