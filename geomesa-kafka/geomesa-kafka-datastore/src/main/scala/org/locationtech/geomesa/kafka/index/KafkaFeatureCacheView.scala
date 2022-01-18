/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.filter.index.SpatialIndexSupport
import org.locationtech.geomesa.kafka.data.KafkaDataStore.LayerView
import org.locationtech.geomesa.kafka.index.KafkaFeatureCache.EmptyFeatureCache
import org.locationtech.geomesa.utils.geotools.Transform
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import java.util.concurrent.ConcurrentHashMap

trait KafkaFeatureCacheView extends KafkaFeatureCache {
  def sft: SimpleFeatureType
  override val views: Seq[KafkaFeatureCacheView] = Seq.empty
}

object KafkaFeatureCacheView {

  /**
   * Create a new layer view
   *
   * @param view view config
   * @param index spatial index to use
   * @return
   */
  def apply(view: LayerView, index: SpatialIndexSupport): KafkaFeatureCacheView = {
    (view.filter, view.transform) match {
      case (Some(f), None)    => new KafkaFeatureCacheFilterView(view.viewSft, index, f)
      case (None, Some(t))    => new KafkaFeatureCacheTransformView(view.viewSft, index, t.toArray)
      case (Some(f), Some(t)) => new KafkaFeatureCacheFilterTransformView(view.viewSft, index, f, t.toArray)
      case _ => throw new IllegalArgumentException("LayerView must define at least one of filter or transform")
    }
  }

  /**
   * Create an empty view
   *
   * @param sft view feature type
   * @return
   */
  def empty(sft: SimpleFeatureType): KafkaFeatureCacheView = new KafkaFeatureCacheEmptyView(sft)

  /**
   * Empty view class
   *
   * @param sft view feature type
   */
  class KafkaFeatureCacheEmptyView(val sft: SimpleFeatureType)
    extends EmptyFeatureCache(Seq.empty) with KafkaFeatureCacheView

  /**
   * View that filters input records based on a CQL predicate
   *
   * @param sft view feature type
   * @param support spatial index
   * @param filter filter
   */
  class KafkaFeatureCacheFilterView(
      sft: SimpleFeatureType,
      support: SpatialIndexSupport,
      filter: Filter
    ) extends BaseFeatureCacheView(sft, support) {
    override def put(feature: SimpleFeature): Unit = {
      if (filter.evaluate(feature)) {
        super.put(ScalaSimpleFeature.wrap(sft, feature))
      }
    }
  }

  /**
   * View that transforms input records to a new feature type
   *
   * @param sft view feature type
   * @param support spatial index
   * @param transforms transform definitions
   */
  class KafkaFeatureCacheTransformView(
      sft: SimpleFeatureType,
      support: SpatialIndexSupport,
      transforms: Array[Transform]
    ) extends BaseFeatureCacheView(sft, support) {
    override def put(feature: SimpleFeature): Unit = super.put(new TransformSimpleFeature(sft, transforms, feature))
  }

  /**
   * View that both filters and transforms input records
   *
   * @param sft view feature type
   * @param support spatial index
   * @param filter filter
   * @param transforms transform definitions
   */
  class KafkaFeatureCacheFilterTransformView(
      sft: SimpleFeatureType,
      support: SpatialIndexSupport,
      filter: Filter,
      transforms: Array[Transform]
    ) extends BaseFeatureCacheView(sft, support) {
    override def put(feature: SimpleFeature): Unit = {
      if (filter.evaluate(feature)) {
        super.put(new TransformSimpleFeature(sft, transforms, feature))
      }
    }
  }

  /**
   * Base view implementation
   *
   * @param sft view feature type
   * @param support spatial index
   */
  abstract class BaseFeatureCacheView(val sft: SimpleFeatureType, support: SpatialIndexSupport)
    extends KafkaFeatureCacheView {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    private val geomIndex = sft.getGeomIndex
    private val idMap = new ConcurrentHashMap[String, SimpleFeature]()

    override def put(feature: SimpleFeature): Unit = {
      idMap.put(feature.getID, feature)
      support.index.insert(feature.getAttribute(geomIndex).asInstanceOf[Geometry], feature.getID, feature)
    }

    override def remove(id: String): Unit = {
      val feature = idMap.remove(id)
      if (feature != null) {
        support.index.remove(feature.getAttribute(geomIndex).asInstanceOf[Geometry], id)
      }
    }

    override def clear(): Unit = {
      idMap.clear()
      support.index.clear()
    }

    override def size(): Int = idMap.size()

    override def size(filter: Filter): Int =
      if (filter == Filter.INCLUDE) { idMap.size() } else { query(filter).length }

    override def query(id: String): Option[SimpleFeature] = Option(idMap.get(id))

    override def query(filter: Filter): Iterator[SimpleFeature] = support.query(filter)

    override def close(): Unit = {}
  }
}
