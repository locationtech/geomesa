/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.function.{BucketHashFunction, MurmurHashFunction}
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.{PartitionRange, RangeBuilder}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionKey
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}

class HashScheme[T](
    attribute: String,
    index: Int,
    buckets: Int,
    hasher: MurmurHashFunction.Hashing[T],
  ) extends PartitionScheme {

  import FilterHelper.ff

  override val name: String = s"${HashScheme.Name}:attribute=$attribute:buckets=$buckets"

  private val format = s"%0${(buckets - 1).toString.length}d"
  private val default = PartitionKey(name, format.format(0))

  override def getPartition(feature: SimpleFeature): PartitionKey = {
    val value = feature.getAttribute(index)
    if (value == null) {
      default
    } else {
      PartitionKey(name, toPartition(value.asInstanceOf[T]))
    }
  }

  override def getRangesForFilter(filter: Filter): Option[Seq[PartitionRange]] = {
    getBounds(filter).map { bounds =>
      val builder = new RangeBuilder()
      bounds.foreach { bound =>
        // note: these are all equality values, as determined in getBounds
        val lower = toPartition(bound.lower.value.get)
        val upper = lower + ZeroChar
        builder += PartitionRange(name, lower, upper)
      }
      builder.result()
    }
  }

  override def getPartitionsForFilter(filter: Filter): Option[Seq[PartitionKey]] = {
    getBounds(filter).map { bounds =>
      bounds.map { bound =>
        // note: these are all equality values, as determined in getBounds
        PartitionKey(name, toPartition(bound.lower.value.get))
      }
    }
  }

  private def getBounds(filter: Filter): Option[Seq[Bounds[T]]] = {
    val bounds = FilterHelper.extractAttributeBounds(filter, attribute, hasher.binding)
    if (bounds.isEmpty || bounds.exists(_.isRange)) {
      None
    } else if (bounds.disjoint) {
      Some(Seq.empty)
    } else {
      Some(bounds.values)
    }
  }

  private def toPartition(value: T): String = format.format((hasher(value) & Int.MaxValue) % buckets)

  override def getCoveringFilter(partition: PartitionKey): Filter = {
    val fn = BucketHashFunction.Name.getFunctionName
    ff.equals(ff.function(fn, ff.property(attribute), ff.literal(buckets)), ff.literal(partition.value.toInt))
  }
}

object HashScheme {

  val Name = "hash"

  class HashPartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] = {
      val opts = SchemeOpts(scheme)
      if (opts.name != Name) { None } else {
        val attribute = opts.getSingle("attribute").orNull
        require(attribute != null, s"Hash scheme requires an attribute to be specified with 'attribute=<attribute>'")
        val index = attributeIndex(sft, attribute)
        val binding = sft.getDescriptor(index).getType.getBinding
        val hasher = MurmurHashFunction.Hashers.find(_.binding.isAssignableFrom(binding)).orNull
        require(hasher != null, s"Invalid type binding '${binding.getName}' of attribute '$attribute'")
        val buckets = opts.getSingle("buckets").orNull
        require(buckets != null, s"Hash scheme requires a number of buckets to be specified with 'buckets=<n>'")
        Some(new HashScheme(attribute, index, buckets.toInt, hasher))
      }
    }
  }
}
