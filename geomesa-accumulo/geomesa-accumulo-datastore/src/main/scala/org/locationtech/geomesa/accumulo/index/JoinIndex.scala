/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.locationtech.geomesa.accumulo.data.AccumuloWritableFeature
import org.locationtech.geomesa.index.api.ShardStrategy.AttributeShardStrategy
import org.locationtech.geomesa.index.api.{RowKeyValue, WritableFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.ConfiguredIndex
import org.locationtech.geomesa.index.index.attribute.{AttributeIndex, AttributeIndexKey, AttributeIndexKeySpace}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.simple.SimpleFeatureType

class JoinIndex(ds: GeoMesaDataStore[_],
                sft: SimpleFeatureType,
                attribute: String,
                secondaries: Seq[String],
                mode: IndexMode)
    extends AttributeIndex(ds, sft, attribute, secondaries, mode) with AccumuloJoinIndex {

  override val keySpace: AttributeIndexKeySpace =
    new AttributeIndexKeySpace(sft, AttributeShardStrategy(sft), attribute) {
      override def toIndexKey(writable: WritableFeature,
                              tier: Array[Byte],
                              id: Array[Byte],
                              lenient: Boolean): RowKeyValue[AttributeIndexKey] = {
        val kv = super.toIndexKey(writable, tier, id, lenient)
        kv.copy(values = writable.asInstanceOf[AccumuloWritableFeature].indexValues)
      }
    }
}

object JoinIndex extends ConfiguredIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  // hook to allow for not returning join plans
  val AllowJoinPlans: ThreadLocal[Boolean] = new ThreadLocal[Boolean] {
    override def initialValue: Boolean = true
  }

  override val name: String = AttributeIndex.JoinIndexName
  override val version: Int = 8

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    AttributeIndex.supports(sft, attributes)

  override def defaults(sft: SimpleFeatureType): Seq[Seq[String]] = {
    sft.getAttributeDescriptors.asScala.flatMap { d =>
      val index = d.getUserData.get(AttributeOptions.OPT_INDEX).asInstanceOf[String]
      if (index != null && index.equalsIgnoreCase(IndexCoverage.JOIN.toString) && AttributeIndexKey.encodable(d)) {
        Seq(Seq(d.getLocalName) ++ Option(sft.getGeomField) ++ sft.getDtgField.filter(_ != d.getLocalName))
      } else {
        Seq.empty
      }
    }
  }
}
