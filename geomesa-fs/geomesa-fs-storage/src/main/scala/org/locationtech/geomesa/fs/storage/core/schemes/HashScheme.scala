/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.apache.iceberg.expressions.{Expression, Expressions}
import org.apache.iceberg.{PartitionSpec, StructLike}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.function.{BucketHashFunction, MurmurHashFunction}
import org.locationtech.geomesa.fs.storage.core.schema.ColumnName
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionScheme.EnumeratedScheme

/**
 * Hash (bucket) scheme
 *
 * @param attribute attribute name
 * @param index index of the attribute in the feature type
 * @param buckets number of buckets values are hashed into
 * @param hasher type-specific hasher
 */
case class HashScheme[T](attribute: String, index: Int, buckets: Int, hasher: MurmurHashFunction.Hashing[T])
    extends PartitionScheme with EnumeratedScheme {

  import FilterHelper.ff

  override val name: String = s"${HashScheme.Name}:attribute=$attribute:buckets=$buckets"

  override val column: String = ColumnName.encode(attribute)

  private val format = s"%0${(buckets - 1).toString.length}d"
  private val default = PartitionKey(name, format.format(0))

  override def getPartition(feature: SimpleFeature): PartitionKey = {
    val value = feature.getAttribute(index)
    if (value == null) {
      default
    } else {
      PartitionKey(name, format.format((hasher(value.asInstanceOf[T]) & Int.MaxValue) % buckets))
    }
  }

  override def getCoveringFilter(partition: PartitionKey): Filter = {
    val fn = BucketHashFunction.Name.getFunctionName
    ff.equals(ff.function(fn, ff.property(attribute), ff.literal(buckets)), ff.literal(partition.value.toInt))
  }

  override def getCoveringExpression(partition: PartitionKey): Expression =
    Expressions.equal(Expressions.bucket[Integer](column, buckets), Integer.valueOf(partition.value))

  override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder = b.bucket(column, buckets)

  override def getPartition(partition: StructLike, i: Int): PartitionKey =
    PartitionKey(name, format.format(partition.get(i, classOf[Integer])))

  override def partitions: Seq[PartitionKey] = Seq.tabulate(buckets)(i => PartitionKey(name, format.format(i)))
}

object HashScheme extends PartitionSchemeFactory {

  val Name = "hash"

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
