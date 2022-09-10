/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.stream

import com.typesafe.config.Config
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.util.ServiceLoader

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
    import scala.collection.JavaConverters._
    val factory = ServiceLoader.load(classOf[SimpleFeatureStreamSourceFactory]).iterator().asScala.find(_.canProcess(conf))
    factory.map { f => f.create(conf, namespace) }.getOrElse(throw new RuntimeException("Cannot load source"))
  }
}

