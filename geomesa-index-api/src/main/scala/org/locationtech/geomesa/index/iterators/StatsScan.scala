/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import org.geotools.factory.Hints
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.utils.stats.{Stat, StatSerializer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait StatsScan extends AggregatingScan[Stat] {
  import org.locationtech.geomesa.index.iterators.StatsScan.Configuration._

  var serializer: StatSerializer = _

  override protected def initResult(sft: SimpleFeatureType,
                                    transform: Option[SimpleFeatureType],
                                    options: Map[String, String]): Stat = {
    val finalSft = transform.getOrElse(sft)
    serializer = StatSerializer(finalSft)

    Stat(finalSft, options(STATS_STRING_KEY))
  }

  override protected def aggregateResult(sf: SimpleFeature, result: Stat): Unit = result.observe(sf)

  // encode the result as a byte array
  override protected def encodeResult(result: Stat): Array[Byte] = serializer.serialize(result)
}

object StatsScan {

  object Configuration {
    val STATS_STRING_KEY       = "geomesa.stats.string"
    val STATS_FEATURE_TYPE_KEY = "geomesa.stats.featuretype"
  }

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _, _],
                filter: Option[Filter],
                hints: Hints): Map[String, String] = {
    import org.locationtech.geomesa.index.conf.QueryHints.{RichHints, STATS_STRING}
    import Configuration.STATS_STRING_KEY
    AggregatingScan.configure(sft, index, filter, hints.getTransform, hints.getSampling) ++
      Map(STATS_STRING_KEY -> hints.get(STATS_STRING).asInstanceOf[String])
  }
}
