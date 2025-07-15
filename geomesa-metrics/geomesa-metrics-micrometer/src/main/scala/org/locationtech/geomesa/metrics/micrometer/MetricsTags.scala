/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.metrics.micrometer

import io.micrometer.core.instrument.Tags

object MetricsTags {

  /**
   * Gets a tag for identifying a feature type by name
   *
   * @param typeName type name
   * @return
   */
  def typeNameTag(typeName: String): Tags = Tags.of("type.name", typeName)
}
