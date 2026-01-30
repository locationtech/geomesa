/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.index.api.ShardStrategy.AttributeShardStrategy
import org.locationtech.geomesa.index.api.{RowKeyValue, WritableFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.ConfiguredIndex
import org.locationtech.geomesa.index.index.attribute.{AttributeIndex, AttributeIndexKey, AttributeIndexKeySpace}
import org.locationtech.geomesa.utils.index.IndexCoverage
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode

class JoinIndex(ds: GeoMesaDataStore[_],
                sft: SimpleFeatureType,
                attribute: String,
                secondaries: Seq[String],
                mode: IndexMode)
    extends AttributeIndex(ds, sft, attribute, secondaries, mode) with AttributeJoinIndex {

  override val keySpace: AttributeIndexKeySpace =
    new AttributeIndexKeySpace(sft, AttributeShardStrategy(sft), attribute) {
      override def toIndexKey(writable: WritableFeature,
                              tier: Array[Byte],
                              id: Array[Byte],
                              lenient: Boolean): RowKeyValue[AttributeIndexKey] = {
        super.toIndexKey(writable, tier, id, lenient).copy(writable.reducedValues)
      }
    }
}

object JoinIndex extends ConfiguredIndex {

  // hook to allow for not returning join plans
  val AllowJoinPlans: ThreadLocal[Boolean] = new ThreadLocal[Boolean] {
    override def initialValue: Boolean = true
  }

  override val name: String = AttributeIndex.JoinIndexName
  override val version: Int = 8

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    AttributeIndex.supports(sft, attributes)

  override def defaults(sft: SimpleFeatureType): Seq[Seq[String]] =
    AttributeIndex.defaults(sft, _.equalsIgnoreCase(IndexCoverage.JOIN.toString))

  override def defaults(sft: SimpleFeatureType, primary: AttributeDescriptor): Option[Seq[String]] =
    AttributeIndex.defaults(sft, primary)
}
