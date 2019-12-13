/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.strategies

import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex}
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.utils.stats.Cardinality
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.{Expression, PropertyName}
import org.opengis.filter.temporal.{After, Before, During, TEquals}

trait AttributeFilterStrategy[T, U] extends GeoMesaFeatureIndex[T, U] {

  def attribute: String

  /**
    * Static cost - equals 100, range 250, not null 5000
    *
    * high cardinality: / 10
    * low cardinality: * 10
    *
    * Compare with id at 1, z3 at 200, z2 at 400
    */
  override def getFilterStrategy(filter: Filter,
                                 transform: Option[SimpleFeatureType],
                                 stats: Option[GeoMesaStats]): Option[FilterStrategy] = {
    import org.locationtech.geomesa.index.strategies.AttributeFilterStrategy.attributeCheck
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    val (primary, secondary) = FilterExtractingVisitor(filter, attribute, sft, attributeCheck(sft))
    primary.map { extracted =>
      lazy val cost = {
        val descriptor = sft.getDescriptor(attribute)
        val base = stats.flatMap(_.getCount(sft, extracted, exact = false)).getOrElse {
          val binding = if (descriptor.isList) { descriptor.getListType() } else { descriptor.getType.getBinding }
          val bounds = FilterHelper.extractAttributeBounds(extracted, attribute, binding)
          if (bounds.isEmpty || !bounds.forall(_.isBounded)) {
            AttributeFilterStrategy.StaticNotNullCost
          } else if (bounds.precise && !bounds.exists(_.isRange)) {
            AttributeFilterStrategy.StaticEqualsCost // TODO account for secondary index
          } else {
            AttributeFilterStrategy.StaticRangeCost
          }
        }
        // prioritize attributes based on cardinality hint
        descriptor.getCardinality() match {
          case Cardinality.HIGH    => base / 10
          case Cardinality.UNKNOWN => base
          case Cardinality.LOW     => base * 10
        }
      }
      FilterStrategy(this, primary, secondary, cost)
    }
  }
}

object AttributeFilterStrategy {

  val StaticEqualsCost  = 100L
  val StaticRangeCost   = 250L
  val StaticNotNullCost = 5000L

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
}
