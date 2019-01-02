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
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.utils.stats.Cardinality
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.{Expression, PropertyName}
import org.opengis.filter.temporal.{After, Before, During, TEquals}

trait AttributeFilterStrategy[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
    extends GeoMesaFeatureIndex[DS, F, W] {

  override def getFilterStrategy(sft: SimpleFeatureType,
                                 filter: Filter,
                                 transform: Option[SimpleFeatureType]): Seq[FilterStrategy[DS, F, W]] = {
    import org.locationtech.geomesa.index.strategies.AttributeFilterStrategy.attributeCheck
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    val attributes = FilterHelper.propertyNames(filter, sft)
    val indexedAttributes = attributes.filter(a => Option(sft.getDescriptor(a)).exists(_.isIndexed))
    indexedAttributes.flatMap { attribute =>
      val (primary, secondary) = FilterExtractingVisitor(filter, attribute, sft, attributeCheck(sft))
      if (primary.isDefined) {
        Seq(FilterStrategy(this, primary, secondary))
      } else {
        Seq.empty
      }
    }
  }

  /**
    * Static cost - equals 100, range 250, not null 5000
    *
    * high cardinality: / 10
    * low cardinality: * 10
    *
    * Compare with id at 1, z3 at 200, z2 at 400
    */
  override def getCost(sft: SimpleFeatureType,
                       stats: Option[GeoMesaStats],
                       filter: FilterStrategy[DS, F, W],
                       transform: Option[SimpleFeatureType]): Long = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    filter.primary match {
      case None    => Long.MaxValue
      case Some(f) =>
        // if there is a filter, we know it has a valid property name
        val attribute = FilterHelper.propertyNames(f, sft).head
        val cost = stats.flatMap(_.getCount(sft, f, exact = false)).getOrElse {
          val binding = sft.getDescriptor(attribute).getType.getBinding
          val bounds = FilterHelper.extractAttributeBounds(f, attribute, binding)
          if (bounds.isEmpty || !bounds.forall(_.isBounded)) {
            AttributeFilterStrategy.StaticNotNullCost
          } else if (bounds.precise && !bounds.exists(_.isRange)) {
            AttributeFilterStrategy.StaticEqualsCost
          } else {
            AttributeFilterStrategy.StaticRangeCost
          }
        }
        // prioritize attributes based on cardinality hint
        sft.getDescriptor(attribute).getCardinality() match {
          case Cardinality.HIGH    => cost / 10
          case Cardinality.UNKNOWN => cost
          case Cardinality.LOW     => cost * 10
        }
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
      case f: Not =>  f.getFilter.isInstanceOf[PropertyIsNull]
      case _ => false
    }
  }

  def isStringProperty(sft: SimpleFeatureType, e: Expression): Boolean = e match {
    case p: PropertyName => Option(sft.getDescriptor(p.getPropertyName)).exists(_.getType.getBinding == classOf[String])
    case _ => false
  }
}
