/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.strategies

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter._
import org.geotools.api.filter.expression.{Expression, PropertyName}
import org.geotools.api.filter.temporal.{After, Before, During, TEquals}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex}
import org.locationtech.geomesa.utils.stats.Cardinality

import java.util.Date

trait AttributeFilterStrategy[T, U] extends GeoMesaFeatureIndex[T, U] {

  import AttributeFilterStrategy.isTemporal
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  private val Seq(attribute, tiered @ _*) = attributes
  private val descriptor = sft.getDescriptor(attribute)
  private val isList = descriptor.isList
  private val binding = if (isList) { descriptor.getListType() } else { descriptor.getType.getBinding }

  override def getFilterStrategy(filter: Filter, transform: Option[SimpleFeatureType]): Option[FilterStrategy] = {

    val (primary, secondary) =
      FilterExtractingVisitor(filter, attribute, sft, AttributeFilterStrategy.attributeCheck(sft))

    primary.flatMap { extracted =>
      val bounds = FilterHelper.extractAttributeBounds(extracted, attribute, binding)
      if (bounds.isEmpty) { None } else {
        val isEquals = bounds.precise && bounds.forall(_.isEquals)
        lazy val secondaryFilterAttributes = tiered.union(secondary.toSeq.flatMap(FilterHelper.propertyNames))
        val temporal = isTemporal(sft, attribute) || (isEquals && secondaryFilterAttributes.exists(isTemporal(sft, _)))
        val basePriority =
          if (isEquals || (bounds.disjoint && !isList)) {
            1f // TODO account for secondary index
          } else if (!bounds.forall(_.isBounded)) {
            1000f // not null
          } else {
            2.5f // range query
          }
        // prioritize attributes based on cardinality hint
        val priority = descriptor.getCardinality() match {
          case Cardinality.HIGH    => basePriority / 10f
          case Cardinality.UNKNOWN => basePriority
          case Cardinality.LOW     => basePriority * 10f
        }
        Some(FilterStrategy(this, primary, secondary, temporal, priority))
      }
    }
  }
}

object AttributeFilterStrategy {

  /**
    * Checks for attribute filters that we can satisfy using the attribute index strategy
    *
    * @param filter filter to evaluate
    * @return true if we can process it as an attribute query
    */
  def attributeCheck(sft: SimpleFeatureType)(filter: Filter): Boolean = {
    filter match {
      case _: And | _: Or => true // note: implies further processing of children
      case _: PropertyIsEqualTo => true
      case _: PropertyIsBetween => true
      case _: PropertyIsGreaterThan | _: PropertyIsLessThan => true
      case _: PropertyIsGreaterThanOrEqualTo | _: PropertyIsLessThanOrEqualTo => true
      case _: During |  _: Before | _: After | _: TEquals => true
      case _: PropertyIsNull => true // we need this to be able to handle 'not null'
      case f: PropertyIsLike => isStringProperty(sft, f.getExpression) && likeEligible(f)
      case f: Not => f.getFilter.isInstanceOf[PropertyIsNull]
      case _ => false
    }
  }

  def isStringProperty(sft: SimpleFeatureType, e: Expression): Boolean = e match {
    case p: PropertyName => Option(sft.getDescriptor(p.getPropertyName)).exists(_.getType.getBinding == classOf[String])
    case _ => false
  }

  private def isTemporal(sft: SimpleFeatureType, attribute: String): Boolean =
    classOf[Date].isAssignableFrom(sft.getDescriptor(attribute).getType.getBinding)
}
