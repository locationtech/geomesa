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
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.{PartitionFilter, PartitionRange, SinglePartition}
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey

import java.util.Locale
import scala.collection.mutable.ArrayBuffer

/**
  * Lexicoded attribute partitioning
  *
  * @param attribute attribute name
  * @param index attribute index in the sft
  * @param binding type binding
  * @param allowedValues list of allowedValues to partition
  */
case class AttributeScheme(
    attribute: String,
    index: Int,
    binding: Class[_],
    maxWidth: Int,
    defaultValue: Option[String],
    allowedValues: Seq[String]
  ) extends PartitionScheme {

  import FilterHelper.ff

  private val alias = AttributeIndexKey.alias(binding)

  override val name: String = {
    val opts = ArrayBuffer(AttributeScheme.Name, s"attribute=$attribute")
    if (maxWidth < Int.MaxValue) {
      opts += s"width=$maxWidth"
    }
    defaultValue.foreach(v => opts +=  s"default=$v")
    if (allowedValues.nonEmpty) {
      opts ++= allowedValues.map(v => s"allow=$v")
    }
    opts.mkString(":")
  }

  override def getPartition(feature: SimpleFeature): String = {
    val value = feature.getAttribute(index)
    if (value == null) {
      return defaultValue.orNull
    }
    val encodedValue = encode(value)
    if (allowedValues.isEmpty || allowedValues.contains(encodedValue)) {
      encodedValue
    } else {
      defaultValue.orNull
    }
  }

  override def getIntersectingPartitions(filter: Filter): Option[Seq[PartitionFilter]] = {
    val bounds = FilterHelper.extractAttributeBounds(filter, attribute, binding)
    if (bounds.isEmpty) {
      None
    } else if (bounds.disjoint) {
      Some(Seq.empty)
    } else {
      val rangeFilter = Some(filter)
      // TODO we can remove some filters based on whether a partition is fully covered or not
      //          if (bounds.precise) {
      //            // remove the attribute filter that we've already accounted for in our covered partitions
      //            val coveredFilter = FilterExtractingVisitor(filter, attribute, AttributeScheme.propertyIsEquals _)._2
      //            val simplified = SimplifiedFilter(coveredFilter.getOrElse(Filter.INCLUDE), covered, partial = false)
      //          } else {
      //
      //          }
      val ranges = bounds.values.map { bound =>
        if (bound.isEquals) {
          SinglePartition(name, encode(bound.lower.value.get))
        } else {
          // TODO handle inclusive upper endpoints
          val lower = bound.lower.value.map(encode).getOrElse("").substring(0, maxWidth)
          val upper = bound.upper.value.map(encode).getOrElse("zzz") // TODO
          PartitionRange(name, lower, upper)
        }
      }
      Some(Seq(PartitionFilter(ranges, rangeFilter)))
    }
  }

  override def getCoveringFilter(partition: String): Filter =
    ff.equals(ff.property(attribute), ff.literal(AttributeIndexKey.decode(alias, partition)))

  private def encode(value: Any): String = {
    val encoded = AttributeIndexKey.typeEncode(value)
    if (classOf[String].isAssignableFrom(binding)) {
      encoded.toLowerCase(Locale.US)
    } else {
      encoded
    }
  }
}

object AttributeScheme {

  val Name = "attribute"

  class AttributePartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] = {
      val opts = SchemeOpts(scheme)
      if (opts.name != Name) { None } else {
        val attribute = opts.getSingle("attribute").orNull
        require(attribute != null, s"Attribute scheme requires an attribute to be specified with 'attribute=<attribute>'")
        val index = sft.indexOf(attribute)
        require(index != -1, s"Attribute '$attribute' does not exist in schema '${sft.getTypeName}'")
        val binding = sft.getDescriptor(index).getType.getBinding
        require(AttributeIndexKey.encodable(binding), s"Invalid type binding '${binding.getName}' of attribute '$attribute'")
        val width = opts.getSingle("width").fold(Int.MaxValue)(_.toInt)

        def encode(value: String): String = {
          val encoded = AttributeIndexKey.encodeForQuery(value.trim(), binding).substring(0, width)
          if (classOf[String].isAssignableFrom(binding)) {
            encoded.toLowerCase(Locale.US)
          } else {
            encoded
          }
        }

        val allowedValues = opts.getMulti("allow").map(encode)
        val defaultValue = opts.getSingle("default").map(encode).orElse(allowedValues.headOption)
        require(allowedValues.isEmpty || defaultValue.forall(allowedValues.contains), "Default partition must be one of the allowed values")
        Some(AttributeScheme(attribute, index, binding, width, defaultValue, allowedValues))
      }
    }
  }
}
