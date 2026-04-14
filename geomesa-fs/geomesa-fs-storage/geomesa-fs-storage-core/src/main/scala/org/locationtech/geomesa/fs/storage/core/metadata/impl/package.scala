/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.metadata

import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

package object impl {

  /**
    * Creates a new simple feature type with the namespace in the simple feature type name
    *
    * @param sft simple feature type
    * @param namespace optional namespace
    * @return
    */
  def namespaced(sft: SimpleFeatureType, namespace: Option[String]): SimpleFeatureType =
    namespace.map(ns => SimpleFeatureTypes.renameSft(sft, s"$ns:${sft.getTypeName}")).getOrElse(sft)
}
