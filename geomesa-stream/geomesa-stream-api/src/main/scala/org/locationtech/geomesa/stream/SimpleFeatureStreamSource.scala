/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.stream

import java.util.ServiceLoader

import com.typesafe.config.Config
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait SimpleFeatureStreamSource {
  def next: SimpleFeature
  def sft: SimpleFeatureType
  def init(): Unit = {}
}

trait SimpleFeatureStreamSourceFactory {
  def canProcess(conf: Config): Boolean
  def create(conf: Config): SimpleFeatureStreamSource = create(conf, "")
  def create(conf: Config, namespace: String): SimpleFeatureStreamSource
}

object SimpleFeatureStreamSource {
  def buildSource(conf: Config): SimpleFeatureStreamSource = buildSource(conf, "")

  def buildSource(conf: Config, namespace: String): SimpleFeatureStreamSource = {
    import scala.collection.JavaConversions._
    val factory = ServiceLoader.load(classOf[SimpleFeatureStreamSourceFactory]).iterator().find(_.canProcess(conf))
    factory.map { f => f.create(conf, namespace) }.getOrElse(throw new RuntimeException("Cannot load source"))
  }
}

