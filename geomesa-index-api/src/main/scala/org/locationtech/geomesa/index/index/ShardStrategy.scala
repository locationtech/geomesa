/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import java.util.concurrent.ConcurrentHashMap

import org.locationtech.geomesa.index.api.WrappedFeature
import org.locationtech.geomesa.index.utils.SplitArrays
import org.opengis.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

trait ShardStrategy {

  def apply(feature: WrappedFeature): Array[Byte]

  def shards: Seq[Array[Byte]]
}

object ShardStrategy {

  private val instances = new ConcurrentHashMap[Int, ShardStrategyImpl]()

  def apply(count: Int): ShardStrategy = {
    if (count < 2) { NoShardStrategy } else {
      var strategy = instances.get(count)
      if (strategy == null) {
        strategy = new ShardStrategyImpl(SplitArrays(count))
        instances.put(count, strategy)
      }
      strategy
    }
  }

  object NoShardStrategy extends ShardStrategy {
    override def apply(feature: WrappedFeature): Array[Byte] = Array.empty
    override val shards: Seq[Array[Byte]] = Seq.empty
  }

  object ZShardStrategy {
    def apply(sft: SimpleFeatureType): ShardStrategy = ShardStrategy(sft.getZShards)
  }

  object AttributeShardStrategy {
    def apply(sft: SimpleFeatureType): ShardStrategy = ShardStrategy(Option(sft.getAttributeShards).getOrElse(1))
  }

  class ShardStrategyImpl(override val shards: IndexedSeq[Array[Byte]]) extends ShardStrategy {
    override def apply(feature: WrappedFeature): Array[Byte] = shards(feature.idHash % shards.length)
  }
}
