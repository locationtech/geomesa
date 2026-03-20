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

import java.nio.charset.StandardCharsets
import java.util.Locale
import scala.collection.mutable.ArrayBuffer

trait AttributeScheme extends PartitionScheme {
  def attribute: String
  def index: Int
}

object AttributeScheme {

  val Name = "attribute"

  private val ZeroChar = new String(Array[Byte](0), StandardCharsets.UTF_8)

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

        val width = opts.getSingle("width").map(_.toInt)
        val divisor = opts.getSingle("divisor").map(_.toInt)
        val scale = opts.getSingle("scale").map(_.toInt)

        val isString = classOf[String].isAssignableFrom(binding)
        val isWholeNumber = binding == classOf[Integer] || binding == classOf[java.lang.Long]
        val isDecimalNumber = binding == classOf[Float] || binding == classOf[Double]

        if (width.isDefined && !isString) {
          throw new IllegalArgumentException(
            s"'width' option is only supported for String-type attributes, not ${binding.getSimpleName}")
        } else if (divisor.isDefined && !isWholeNumber) {
          throw new IllegalArgumentException(
            s"'divisor' option is only supported for Integer and Long-type attributes, not ${binding.getSimpleName}")
        } else if (scale.isDefined && !isDecimalNumber) {
          throw new IllegalArgumentException(
            s"'scale' option is only supported for Float and Double-type attributes, not ${binding.getSimpleName}")
        }

        val allowedValues = opts.getMulti("allow")
        val defaultValue = opts.getSingle("default").getOrElse("")
        require(
          allowedValues.isEmpty || defaultValue.isEmpty || allowedValues.contains(defaultValue),
          "Default partition must be one of the allowed values")

        if (isString) {
          Some(StringScheme(attribute, index, width, defaultValue, allowedValues))
        } else if (binding == classOf[Integer] || binding == classOf[java.lang.Long]) {
          // TODO implement these
          ???
        } else if (binding == classOf[Float] || binding == classOf[Double]) {
          // TODO implement these
          ???
        } else {
          throw new IllegalArgumentException(
            s"Attribute scheme is not supported for type ${binding.getSimpleName} - " +
              s"supported types are String Integer, Long, Float, and Double")
        }
      }
    }
  }

  /**
   * Lexicoded attribute partitioning
   *
   * @param attribute attribute name
   * @param index attribute index in the sft
   * @param maxWidth max width for partition values
   * @param defaultValue default value
   * @param allowedValues list of allowedValues to partition
   */
  case class StringScheme(
      attribute: String,
      index: Int,
      maxWidth: Option[Int],
      defaultValue: String,
      allowedValues: Seq[String]
    ) extends AttributeScheme {

    import FilterHelper.ff

    private val default = if (defaultValue.isEmpty) { defaultValue } else { toPartition(defaultValue) }
    private val allowed = allowedValues.map(toPartition).distinct

    override val name: String = {
      val opts = ArrayBuffer(Name, s"attribute=$attribute")
      maxWidth.foreach(w => opts += s"width=$w")
      if (defaultValue.nonEmpty) {
        opts += s"default=$defaultValue"
      }
      if (allowedValues.nonEmpty) {
        opts ++= allowedValues.map(v => s"allow=$v")
      }
      opts.mkString(":")
    }

    override def getPartition(feature: SimpleFeature): String = {
      val value = feature.getAttribute(index)
      if (value == null) {
        return defaultValue
      }
      val partition = toPartition(value.asInstanceOf[String])
      if (allowed.isEmpty || allowed.contains(partition)) {
        partition
      } else {
        default
      }
    }

    override def getIntersectingPartitions(filter: Filter): Option[Seq[PartitionFilter]] = {
      val bounds = FilterHelper.extractAttributeBounds(filter, attribute, classOf[String])
      if (bounds.isEmpty) {
        None
      } else if (bounds.disjoint) {
        Some(Seq.empty)
      } else {
        val rangeFilter = Some(filter)
        // TODO we can remove some filters based on whether a partition is fully covered or not
        // if (bounds.precise) {
        //   // remove the attribute filter that we've already accounted for in our covered partitions
        //   val coveredFilter = FilterExtractingVisitor(filter, attribute, AttributeScheme.propertyIsEquals _)._2
        //   val simplified = SimplifiedFilter(coveredFilter.getOrElse(Filter.INCLUDE), covered, partial = false)
        //  }
        val ranges = bounds.values.flatMap { bound =>
          if (bound.isEquals) {
            val partition = toPartition(bound.lower.value.get)
            if (allowed.isEmpty || allowed.contains(partition)) {
              Seq(SinglePartition(name, partition))
            } else {
              Seq.empty
            }
          } else {
            val lower = bound.lower.value.fold("")(toPartition)
            val upper = bound.upper.value.fold("zzz"/*TODO*/)(encodeUpperBound(_, bound.upper.exclusive))
            val range = PartitionRange(name, lower, upper)
            if (allowed.isEmpty) {
              Seq(range)
            } else {
              allowed.collect { case v if range.contains(v) => SinglePartition(name, v) }
            }
          }
        }
        Some(Seq(PartitionFilter(ranges :+ SinglePartition(name, default), rangeFilter)))
      }
    }

    override def getCoveringFilter(partition: String): Filter = {
      val escaped =
        partition.replaceAllLiterally("""\""", """\\""").replaceAllLiterally("""%""", """\%""").replaceAllLiterally("""_""", """\_""")
      val regex = if (maxWidth.isDefined) { escaped + "%" } else { escaped }
      ff.like(ff.property(attribute), regex, "%", "_", "\\", false)
    }

    private def toPartition(value: String): String = {
      val encoded = value.toLowerCase(Locale.US)
      maxWidth match {
        case None => encoded
        case Some(w) => encoded.slice(0, w)
      }
    }

    private def encodeUpperBound(value: String, exclusive: Boolean): String = {
      val encoded = toPartition(value)
      if (maxWidth.isDefined || !exclusive) {
        encoded + ZeroChar
      } else {
        encoded
      }
    }
  }
}

