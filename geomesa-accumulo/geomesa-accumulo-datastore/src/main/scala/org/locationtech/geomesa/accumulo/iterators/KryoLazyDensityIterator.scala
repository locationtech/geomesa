/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.utils.KryoLazyDensityUtils
import org.locationtech.geomesa.index.utils.KryoLazyDensityUtils._
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
 * Density iterator - only works on kryo-encoded features
 */
class KryoLazyDensityIterator extends KryoLazyAggregatingIterator[DensityResult] with LazyLogging with KryoLazyDensityUtils {

  override def init(options: Map[String, String]): DensityResult = {
    initialize(options, sft)
  }

  override def aggregateResult(sf: SimpleFeature, result: DensityResult): Unit = writeGeom(sf, result)

  override def encodeResult(result: DensityResult): Array[Byte] = KryoLazyDensityUtils.encodeResult(result)

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

object KryoLazyDensityIterator extends LazyLogging with KryoLazyDensityUtils {

  import KryoLazyDensityUtils._
  val DEFAULT_PRIORITY = 25
  /**
   * Creates an iterator config for the kryo density iterator
   */
  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndexType,
                filter: Option[Filter],
                hints: Hints,
                deduplicate: Boolean,
                priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    val envelope = hints.getDensityEnvelope.get
    val (width, height) = hints.getDensityBounds.get
    val weight = hints.getDensityWeight
    val is = new IteratorSetting(priority, "density-iter", classOf[KryoLazyDensityIterator])
    configure(is, sft, index, filter, deduplicate, envelope, width, height, weight)
  }

  protected[iterators] def configure(is: IteratorSetting,
                                     sft: SimpleFeatureType,
                                     index: AccumuloFeatureIndexType,
                                     filter: Option[Filter],
                                     deduplicate: Boolean,
                                     envelope: Envelope,
                                     gridWidth: Int,
                                     gridHeight: Int,
                                     weightAttribute: Option[String]): IteratorSetting = {
    // we never need to dedupe densities - either we don't have dupes or we weight based on the duplicates
    KryoLazyAggregatingIterator.configure(is, sft, index, filter, deduplicate, None)
    is.addOption(ENVELOPE_OPT, s"${envelope.getMinX},${envelope.getMaxX},${envelope.getMinY},${envelope.getMaxY}")
    is.addOption(GRID_OPT, s"$gridWidth,$gridHeight")
    weightAttribute.foreach(is.addOption(WEIGHT_OPT, _))
    is
  }

  /**
   * Adapts the iterator to create simple features.
   * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
   */
  def kvsToFeatures(): (Entry[Key, Value]) => SimpleFeature = {
    val sf = new ScalaSimpleFeature("", DENSITY_SFT)
    sf.setAttribute(0, GeometryUtils.zeroPoint)
    (e: Entry[Key, Value]) => {
      // Return value in user data so it's preserved when passed through a RetypingFeatureCollection
      sf.getUserData.put(DENSITY_VALUE, e.getValue.get())
      sf
    }
  }

}
