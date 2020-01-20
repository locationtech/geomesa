/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc

import java.util.Collections

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.storage.common.observer.{FileSystemObserver, FileSystemObserverFactory}
import org.locationtech.geomesa.fs.storage.orc.TestObserverFactory.TestObserver
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.ArrayBuffer

class TestObserverFactory extends FileSystemObserverFactory {
  override def init(conf: Configuration, root: Path, sft: SimpleFeatureType): Unit = {}
  override def apply(path: Path): FileSystemObserver = {
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

  class TestObserver(val path: Path) extends FileSystemObserver {

    val features = ArrayBuffer.empty[SimpleFeature]
    var closed = false

    override def write(feature: SimpleFeature): Unit = features += feature
    override def flush(): Unit = {}
    override def close(): Unit = closed = true
  }
}
