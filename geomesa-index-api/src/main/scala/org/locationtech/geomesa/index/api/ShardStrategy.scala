/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import org.locationtech.geomesa.index.utils.SplitArrays
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType

import java.util.concurrent.ConcurrentHashMap

trait ShardStrategy {

  /**
    * Get a shard for the given feature. This should be consistent for a given feature ID
    *
    * @param feature feature
    * @return
    */
  def apply(feature: WritableFeature): Array[Byte]

  /**
    * All possible shards
    *
    * @return
    */
  def shards: Seq[Array[Byte]]

  /**
    * Length of each shard. 0 indicates no sharding
    *
    * @return
    */
  def length: Int
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
    override def apply(feature: WritableFeature): Array[Byte] = Array.empty
    override val shards: Seq[Array[Byte]] = Seq.empty
    override val length: Int = 0
  }

  object Z2ShardStrategy {
    def apply(sft: SimpleFeatureType): ShardStrategy = ShardStrategy(sft.getZ2Shards)
  }

  object Z3ShardStrategy {
    def apply(sft: SimpleFeatureType): ShardStrategy = ShardStrategy(sft.getZ3Shards)
  }

  object AttributeShardStrategy {
    def apply(sft: SimpleFeatureType): ShardStrategy = ShardStrategy(sft.getAttributeShards)
  }

  class ShardStrategyImpl(override val shards: IndexedSeq[Array[Byte]]) extends ShardStrategy {
    override def apply(feature: WritableFeature): Array[Byte] = {
      try { shards(feature.idHash % shards.length) } catch {
        // handle case where hash is Int.MinValue, which isn't handled by math.abs
        case e: IndexOutOfBoundsException => shards.head
      }
    }
    override val length: Int = shards.head.length
  }
}
