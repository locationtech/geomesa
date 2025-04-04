/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.util.ServiceLoader

object GeoMesaSpark {

  import scala.collection.JavaConverters._

  lazy val providers: ServiceLoader[SpatialRDDProvider] = ServiceLoader.load(classOf[SpatialRDDProvider])

  def apply(params: java.util.Map[String, _ <: java.io.Serializable]): SpatialRDDProvider =
    providers.asScala.find(_.canProcess(params)).getOrElse(throw new RuntimeException("Could not find a SpatialRDDProvider"))
}

