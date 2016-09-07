/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.attribute

import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.{AccumuloFeatureIndex, AccumuloFilterStrategy}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

// current version - added feature ID and dates to row key
object AttributeIndex extends AccumuloFeatureIndex with AttributeWritableIndex with AttributeQueryableIndex {

  override val name: String = "attr"

  override val version: Int = 2

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.exists(_.isIndexed)
  }
}

// noinspection ScalaDeprecation
// initial implementation
@deprecated
object AttributeIndexV1 extends AccumuloFeatureIndex with AttributeWritableIndexV5 with AttributeQueryableIndexV5 {

  override val name: String = "attr"

  override val version: Int = 1

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.exists(_.isIndexed)
  }

  override def getFilterStrategy(sft: SimpleFeatureType, filter: Filter): Seq[AccumuloFilterStrategy] =
    // note: strategy is the same between versioned indices
    AttributeIndex.getFilterStrategy(sft, filter)

  override def getCost(sft: SimpleFeatureType,
                       ops: Option[AccumuloDataStore],
                       filter: AccumuloFilterStrategy,
                       transform: Option[SimpleFeatureType]): Long =
    // note: cost is the same between versioned indices
    AttributeIndex.getCost(sft, ops, filter, transform)
}