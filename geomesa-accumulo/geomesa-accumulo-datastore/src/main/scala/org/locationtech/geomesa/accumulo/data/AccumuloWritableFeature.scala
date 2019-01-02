/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.locationtech.geomesa.accumulo.index.IndexValueEncoder
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.index.api.WritableFeature.{AttributeLevelWritableFeature, FeatureWrapper}
import org.locationtech.geomesa.index.api.{KeyValue, WritableFeature}
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Writable feature with support for 'index-values' used in attribute join indices
  *
  * @param delegate base writable feature
  */
abstract class AccumuloWritableFeature(delegate: WritableFeature) extends WritableFeature {

  override val feature: SimpleFeature = delegate.feature
  override val values: Seq[KeyValue] = delegate.values
  override val id: Array[Byte] = delegate.id

  def indexValues: Seq[KeyValue]
}

object AccumuloWritableFeature {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def wrapper(sft: SimpleFeatureType, delegate: FeatureWrapper): FeatureWrapper = {
    val serializer = IndexValueEncoder(sft)
    if (sft.getVisibilityLevel == VisibilityLevel.Attribute) {
      new AccumuloAttributeLevelFeatureWrapper(delegate, serializer)
    } else {
      new AccumuloFeatureLevelFeatureWrapper(delegate, serializer)
    }
  }

  class AccumuloFeatureLevelFeatureWrapper(delegate: FeatureWrapper, serializer: SimpleFeatureSerializer)
      extends FeatureWrapper {
    override def wrap(feature: SimpleFeature): WritableFeature =
      new AccumuloFeatureLevelFeature(delegate.wrap(feature), serializer)
  }

  class AccumuloAttributeLevelFeatureWrapper(delegate: FeatureWrapper, serializer: SimpleFeatureSerializer)
      extends FeatureWrapper {
    override def wrap(feature: SimpleFeature): WritableFeature =
      new AccumuloAttributeLevelFeature(delegate.wrap(feature), serializer)
  }

  class AccumuloFeatureLevelFeature(delegate: WritableFeature, serializer: SimpleFeatureSerializer)
      extends AccumuloWritableFeature(delegate) {
    override lazy val indexValues: Seq[KeyValue] = {
      lazy val serialized = serializer.serialize(feature)
      delegate.values.map(_.copy(toValue = serialized))
    }
  }

  class AccumuloAttributeLevelFeature(delegate: WritableFeature, serializer: SimpleFeatureSerializer)
      extends AccumuloWritableFeature(delegate) {
    override lazy val indexValues: Seq[KeyValue] = delegate match {
      case f: AttributeLevelWritableFeature =>
        val sf = new ScalaSimpleFeature(feature.getFeatureType, "")
        f.indexGroups.map { case (vis, indices) =>
          indices.foreach(i => sf.setAttributeNoConvert(i, feature.getAttribute(i)))
          val kv = KeyValue(f.colFamily, indices, vis, serializer.serialize(sf))
          indices.foreach(i => sf.setAttributeNoConvert(i, null))
          kv
        }
    }
  }
}
