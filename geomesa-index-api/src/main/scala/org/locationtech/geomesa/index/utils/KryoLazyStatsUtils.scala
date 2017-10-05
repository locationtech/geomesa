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
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.stats.{Stat, StatSerializer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object KryoLazyStatsUtils {

  val StatsSft: SimpleFeatureType = SimpleFeatureTypes.createType("stats:stats", "stats:String,geom:Geometry")

  /**
    * Encodes a stat as a base64 string.
    *
    * @param sft simple feature type of underlying schema
    * @return function to encode a stat as a base64 string
    */
  def encodeStat(sft: SimpleFeatureType): (Stat) => String = {
    val serializer = StatSerializer(sft)
    (stat) => Base64.encodeBase64URLSafeString(serializer.serialize(stat))
  }

  /**
    * Decodes a stat string from a result simple feature.
    *
    * @param sft simple feature type of the underlying schema
    * @return function to convert an encoded encoded string to a stat
    */
  def decodeStat(sft: SimpleFeatureType): (String) => Stat = {
    val serializer = StatSerializer(sft)
    (encoded) => serializer.deserialize(Base64.decodeBase64(encoded))
  }


  /**
    * Reduces computed simple features which contain stat information into one on the client
    *
    * @param features iterator of features received per tablet server from query
    * @param hints query hints that the stats are being run against
    * @return aggregated iterator of features
    */
  def reduceFeatures(sft: SimpleFeatureType,
                     hints: Hints)
                    (features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
    val statSft = hints.getTransformSchema.getOrElse(sft)
    val sum = try {
      if (features.isEmpty) {
        // create empty stat based on the original input so that we always return something
        Stat(statSft, hints.getStatsQuery)
      } else {
        val decode = decodeStat(statSft)
        val sum = decode(features.next.getAttribute(0).asInstanceOf[String])
        while (features.hasNext) {
          sum += decode(features.next.getAttribute(0).asInstanceOf[String])
        }
        sum
      }
    } finally {
      CloseWithLogging(features)
    }

    val stats = if (hints.isStatsEncode) { encodeStat(statSft)(sum) } else { sum.toJson }
    CloseableIterator(Iterator(new ScalaSimpleFeature(StatsSft, "stat", Array(stats, GeometryUtils.zeroPoint))))
  }
}
