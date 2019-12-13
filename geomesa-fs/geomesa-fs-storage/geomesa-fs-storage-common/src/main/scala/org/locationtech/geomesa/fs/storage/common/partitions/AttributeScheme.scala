/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.SimplifiedFilter
import org.locationtech.geomesa.fs.storage.api.{NamedOptions, PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{And, Filter, Or, PropertyIsEqualTo}

/**
  * Lexicoded attribute partitioning
  *
  * @param attribute attribute name
  * @param index attribute index in the sft
  * @param binding type binding
  */
case class AttributeScheme(attribute: String, index: Int, binding: Class[_]) extends PartitionScheme {

  override val depth: Int = 1

  override def getPartitionName(feature: SimpleFeature): String = {
    val value = feature.getAttribute(index)
    if (value == null) { "" } else { AttributeIndexKey.typeEncode(value) }
  }

  override def getSimplifiedFilters(filter: Filter, partition: Option[String]): Option[Seq[SimplifiedFilter]] = {
    val bounds = FilterHelper.extractAttributeBounds(filter, attribute, binding)
    if (bounds.disjoint) {
      Some(Seq.empty)
    } else if (bounds.isEmpty || !bounds.precise || !bounds.forall(_.isEquals)) {
      None
    } else {
      // note: we verified they are all single values above
      val covered = bounds.values.map(bound => AttributeIndexKey.encodeForQuery(bound.lower.value.get, binding))

      // remove the attribute filter that we've already accounted for in our covered partitions
      val coveredFilter = FilterExtractingVisitor(filter, attribute, AttributeScheme.propertyIsEquals _)._2
      val simplified = SimplifiedFilter(coveredFilter.getOrElse(Filter.INCLUDE), covered, partial = false)

      partition match {
        case None => Some(Seq(simplified))
        case Some(p) if simplified.partitions.contains(p) => Some(Seq(simplified.copy(partitions = Seq(p))))
        case _ => Some(Seq.empty)
      }
    }
  }
}

object AttributeScheme {

  val Name = "attribute"

  /**
    * Check to extract only the equality filters that we can process with this partition scheme
    *
    * @param filter filter
    * @return
    */
  def propertyIsEquals(filter: Filter): Boolean = {
    filter match {
      case _: And | _: Or => true // note: implies further processing of children
      case _: PropertyIsEqualTo => true
      case _ => false
    }
  }

  object Config {
    val AttributeOpt: String = "partitioned-attribute"
  }

  class AttributePartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(sft: SimpleFeatureType, config: NamedOptions): Option[PartitionScheme] = {
      if (config.name != Name) { None } else {
        val attribute = config.options.getOrElse(Config.AttributeOpt, null)
        require(attribute != null, s"Attribute scheme requires valid attribute name '${Config.AttributeOpt}'")
        val index = sft.indexOf(attribute)
        require(index != -1, s"Attribute '$attribute' does not exist in schema '${sft.getTypeName}'")
        val binding = sft.getDescriptor(index).getType.getBinding
        require(AttributeIndexKey.encodable(binding),
          s"Invalid type binding '${binding.getName}' of attribute '$attribute'")
        Some(AttributeScheme(attribute, index, binding))
      }
    }
  }
}
