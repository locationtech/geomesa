/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage
import org.locationtech.geomesa.fs.storage.core.observer.{FileSystemObserver, FileSystemObserverFactory}
import org.locationtech.geomesa.parquet.TestObserverFactory.TestObserver

import java.net.URI
import java.util.Collections
import scala.collection.mutable.ArrayBuffer

class TestObserverFactory extends FileSystemObserverFactory {
  override def init(storage: FileSystemStorage): Unit = {}
  override def apply(path: URI): FileSystemObserver = {
    val observer = new TestObserver(path)
    TestObserverFactory.observers += observer
    observer
  }
  override def close(): Unit = {}
}

object TestObserverFactory {

  import scala.collection.JavaConverters._

  val observers: scala.collection.mutable.Set[TestObserver] =
    Collections.synchronizedSet(new java.util.HashSet[TestObserver]()).asScala

  class TestObserver(val path: URI) extends FileSystemObserver {

    val features = ArrayBuffer.empty[SimpleFeature]
    var closed = false

    override def apply(feature: SimpleFeature): Unit = features += feature
    override def flush(): Unit = {}
    override def close(): Unit = closed = true
  }
}
