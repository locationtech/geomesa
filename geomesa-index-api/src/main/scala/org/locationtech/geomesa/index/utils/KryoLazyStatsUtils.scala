/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import org.apache.commons.codec.binary.Base64
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints.{ENCODE_STATS, RichHints, STATS_STRING}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.{Stat, StatSerializer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object KryoLazyStatsUtils {
  val StatsSft = SimpleFeatureTypes.createType("stats:stats", "stats:String,geom:Geometry")

  /**
    * Encodes a stat as a base64 string.
    *
    * Creates a new serializer each time, so don't call repeatedly.
    *
    * @param stat stat to encode
    * @param sft simple feature type of underlying schema
    * @return base64 string
    */
  def encodeStat(stat: Stat, sft: SimpleFeatureType): String =
    Base64.encodeBase64URLSafeString(StatSerializer(sft).serialize(stat))

  /**
    * Decodes a stat string from a result simple feature.
    *
    * Creates a new serializer each time, so not used internally.
    *
    * @param encoded encoded string
    * @param sft simple feature type of the underlying schema
    * @return stat
    */
  def decodeStat(encoded: String, sft: SimpleFeatureType): Stat =
    StatSerializer(sft).deserialize(Base64.decodeBase64(encoded))

  /**
    * Reduces computed simple features which contain stat information into one on the client
    *
    * @param features iterator of features received per tablet server from query
    * @param hints query hints that the stats are being run against
    * @return aggregated iterator of features
    */
  def reduceFeatures(sft: SimpleFeatureType, hints: Hints)
                    (features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
    val transform = hints.getTransform
    val serializer = transform match {
      case Some((tdef, tsft)) => StatSerializer(tsft)
      case None               => StatSerializer(sft)
    }

    val decodedStats = features.map { f =>
      serializer.deserialize(Base64.decodeBase64(f.getAttribute(0).toString))
    }

    val sum = if (decodedStats.isEmpty) {
      // create empty stat based on the original input so that we always return something
      Stat(sft, hints.get(STATS_STRING).asInstanceOf[String])
    } else {
      val sum = decodedStats.next()
      decodedStats.foreach(sum += _)
      sum
    }
    decodedStats.close()

    val stats = if (hints.containsKey(ENCODE_STATS) && hints.get(ENCODE_STATS).asInstanceOf[Boolean]) encodeStat(sum, sft) else sum.toJson
    Iterator(new ScalaSimpleFeature("stat", StatsSft, Array(stats, GeometryUtils.zeroPoint)))
  }
}
