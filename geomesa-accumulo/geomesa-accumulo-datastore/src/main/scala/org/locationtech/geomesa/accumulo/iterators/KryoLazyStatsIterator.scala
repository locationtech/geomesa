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
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.commons.codec.binary.Base64
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.iterators.KryoLazyFilterTransformIterator._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.index.utils.KryoLazyStatsUtils
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
 * Reads simple features and observe them with a Stat server-side
 *
 * Only works with z3IdxStrategy for now (queries that date filters)
 */
class KryoLazyStatsIterator extends KryoLazyAggregatingIterator[Stat] {

  import org.locationtech.geomesa.accumulo.iterators.KryoLazyStatsIterator._

  var serializer: StatSerializer = null

  override def init(options: Map[String, String]): Stat = {
    sft = IteratorCache.sft(options(KryoLazyAggregatingIterator.SFT_OPT))
    val transformSchema = options.get(TRANSFORM_SCHEMA_OPT).map(IteratorCache.sft).getOrElse(sft)
    serializer = StatSerializer(transformSchema)

    Stat(transformSchema, options(STATS_STRING_KEY))
  }

  override def aggregateResult(sf: SimpleFeature, result: Stat): Unit = result.observe(sf)

  override def encodeResult(result: Stat): Array[Byte] = serializer.serialize(result)
}

object KryoLazyStatsIterator extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.{ENCODE_STATS, STATS_STRING}

  val DEFAULT_PRIORITY = 30
  val STATS_STRING_KEY = "geomesa.stats.string"
  val STATS_FEATURE_TYPE_KEY = "geomesa.stats.featuretype"
  // Need a filler namespace, else geoserver throws NPE for xml output

  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndexType,
                filter: Option[Filter],
                hints: Hints,
                deduplicate: Boolean,
                priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    val is = new IteratorSetting(priority, "stats-iter", classOf[KryoLazyStatsIterator])
    KryoLazyAggregatingIterator.configure(is, sft, index, filter, deduplicate, None)
    is.addOption(STATS_STRING_KEY, hints.get(STATS_STRING).asInstanceOf[String])



    val transform = hints.getTransform
    transform.foreach { case (tdef, tsft) =>
      is.addOption(TRANSFORM_DEFINITIONS_OPT, tdef)
      is.addOption(TRANSFORM_SCHEMA_OPT, SimpleFeatureTypes.encodeType(tsft))
    }

    is
  }

  def kvsToFeatures(sft: SimpleFeatureType): (Entry[Key, Value]) => SimpleFeature = {
    val sf = new ScalaSimpleFeature("", KryoLazyStatsUtils.StatsSft)
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    (e: Entry[Key, Value]) => {
      // value is the already serialized stat
      sf.setAttribute(0, Base64.encodeBase64URLSafeString(e.getValue.get()))
      sf
    }
  }
}
