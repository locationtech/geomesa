/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.metrics

import io.micrometer.core.instrument.Tags
import org.geotools.api.feature.simple.SimpleFeatureType

object MetricsTags {

  /**
   * Gets a tag for identifying a feature type by name
   *
   * @param sft simple feature type
   * @return
   */
  def typeNameTag(sft: SimpleFeatureType): Tags = typeNameTag(sft.getTypeName)

  /**
   * Gets a tag for identifying a feature type by name
   *
   * @param typeName type name
   * @return
   */
  def typeNameTag(typeName: String): Tags = Tags.of("type.name", typeName)
}
