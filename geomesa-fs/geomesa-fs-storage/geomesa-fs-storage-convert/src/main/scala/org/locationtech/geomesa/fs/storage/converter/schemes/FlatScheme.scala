/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.schemes

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter

object FlatScheme extends PartitionScheme {

  override val depth: Int = 0

  override def pattern: String = ""

  override def getIntersectingPartitions(filter: Filter): Option[Seq[String]] = Some(Seq(""))

  object FlatPartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(sft: SimpleFeatureType, config: NamedOptions): Option[PartitionScheme] =
      if (config.name.equalsIgnoreCase("flat")) { Some(FlatScheme) } else { None }
  }
}
