/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import java.util.{Collections, Optional}

import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

object FlatScheme extends PartitionScheme {

  val Name = "flat"

  override def getName: String = Name

  override def getPartition(feature: SimpleFeature): String = ""

  override def getPartitions(filter: Filter): java.util.List[String] = Collections.singletonList("")

  override def getMaxDepth: Int = 0

  override def isLeafStorage: Boolean = true

  override def getOptions: java.util.Map[String, String] = Collections.emptyMap()

  class FlatPartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(name: String,
                      sft: SimpleFeatureType,
                      options: java.util.Map[String, String]): Optional[PartitionScheme] = {
      if (name == Name) {
        Optional.of(FlatScheme)
      } else {
        Optional.empty()
      }
    }
  }
}
