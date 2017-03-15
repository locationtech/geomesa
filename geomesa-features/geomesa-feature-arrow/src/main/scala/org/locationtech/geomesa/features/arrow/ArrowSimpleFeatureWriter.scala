/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.features.arrow

import java.io.{Closeable, Flushable, OutputStream}

import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class ArrowSimpleFeatureWriter(sft: SimpleFeatureType, os: OutputStream) extends Closeable with Flushable {

  def start(dictionaries: Map[String, Map[Any, Int]] = Map.empty): Unit = {

  }

  def add(feature: SimpleFeature): Unit = {

  }

  override def flush(): Unit = {

  }

  override def close(): Unit = {

  }
}
