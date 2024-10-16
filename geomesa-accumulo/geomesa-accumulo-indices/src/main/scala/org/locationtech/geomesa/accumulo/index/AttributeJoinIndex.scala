/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.features.kryo.serialization.IndexValueSerializer
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.index.attribute.{AttributeIndex, AttributeIndexKey, AttributeIndexValues}

/**
  * Mixin trait to add join support to the normal attribute index class
  */
trait AttributeJoinIndex extends GeoMesaFeatureIndex[AttributeIndexValues[Any], AttributeIndexKey] {

  this: AttributeIndex =>

  import scala.collection.JavaConverters._

  private val attribute = attributes.head
  private val attributeIndex = sft.indexOf(attribute)
  private val descriptor = sft.getDescriptor(attributeIndex)
  private val binding = descriptor.getType.getBinding
  val indexSft: SimpleFeatureType = IndexValueSerializer.getIndexSft(sft)

  override val name: String = JoinIndex.name
  override val identifier: String = GeoMesaFeatureIndex.identifier(name, version, attributes)

  abstract override def getFilterStrategy(
      filter: Filter,
      transform: Option[SimpleFeatureType]): Option[FilterStrategy] = {
    super.getFilterStrategy(filter, transform).flatMap { strategy =>
      // verify that it's ok to return join plans, and filter them out if not
      if (!requiresJoin(strategy.secondary, transform)) {
        Some(strategy)
      } else if (!JoinIndex.AllowJoinPlans.get) {
        None
      } else {
        val primary = strategy.primary.getOrElse(Filter.INCLUDE)
        val bounds = FilterHelper.extractAttributeBounds(primary, attribute, binding)
        val joinMultiplier = 9f + bounds.values.length // 10 plus 1 per additional range being scanned
        val multiplier = strategy.costMultiplier * joinMultiplier
        Some(FilterStrategy(strategy.index, strategy.primary, strategy.secondary, strategy.temporal, multiplier))
      }
    }
  }

  /**
    * Does the query require a join against the record table, or can it be satisfied
    * in a single scan
    *
    * @param filter non-attribute filter being evaluated, if any
    * @param transform transform being applied, if any
    * @return
    */
  private def requiresJoin(filter: Option[Filter], transform: Option[SimpleFeatureType]): Boolean =
    !canUseIndexSchema(filter, transform) && !canUseIndexSchemaPlusKey(filter, transform)

  /**
    * Determines if the given filter and transform can operate on index encoded values.
    */
  def canUseIndexSchema(filter: Option[Filter], transform: Option[SimpleFeatureType]): Boolean = {
    // verify that transform *does* exist and only contains fields in the index sft,
    // and that filter *does not* exist or can be fulfilled by the index sft
    supportsTransform(transform) && supportsFilter(filter)
  }

  /**
    * Determines if the given filter and transform can operate on index encoded values
    * in addition to the values actually encoded in the attribute index keys
    */
  def canUseIndexSchemaPlusKey(filter: Option[Filter], transform: Option[SimpleFeatureType]): Boolean = {
    transform.exists { t =>
      val attributes = t.getAttributeDescriptors.asScala.map(_.getLocalName)
      attributes.forall(a => a == attribute || indexSft.indexOf(a) != -1) && supportsFilter(filter)
    }
  }

  def supportsTransform(transform: Option[SimpleFeatureType]): Boolean =
    transform.exists(_.getAttributeDescriptors.asScala.map(_.getLocalName).forall(indexSft.indexOf(_) != -1))

  def supportsFilter(filter: Option[Filter]): Boolean =
    filter.forall(FilterHelper.propertyNames(_, sft).forall(indexSft.indexOf(_) != -1))
}
