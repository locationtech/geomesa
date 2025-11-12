/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.util.ServiceLoader

object GeoMesaSpark {

  import scala.collection.JavaConverters._

  private lazy val providers = ServiceLoader.load(classOf[SpatialRDDProvider]).asScala.toList

  def apply(params: java.util.Map[String, _]): SpatialRDDProvider =
    providers.find(_.canProcess(params)).getOrElse(throw new RuntimeException("Could not find a SpatialRDDProvider"))

  def apply(params: Map[String, _]): SpatialRDDProvider = apply(params.asJava)
}

