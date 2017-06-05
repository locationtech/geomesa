/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
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
    * Static cost - 100
    *
    * high cardinality: / 10
    * low cardinality: Long.MaxValue
    *
    * Compare with id lookups at 1, z2/z3 at 200-401
    */
  override def getCost(sft: SimpleFeatureType,
                       ds: Option[DS],
                       filter: FilterStrategy[DS, F, W],
                       transform: Option[SimpleFeatureType]): Long = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    filter.primary match {
      case None    => Long.MaxValue
      case Some(f) =>
        lazy val cost = ds.flatMap(_.stats.getCount(sft, f, exact = false)).getOrElse(AttributeFilterStrategy.StaticCost)
        // if there is a filter, we know it has a valid property name
        val attribute = FilterHelper.propertyNames(f, sft).head
        sft.getDescriptor(attribute).getCardinality() match {
          case Cardinality.HIGH    => cost / 10 // prioritize attributes marked high-cardinality
          case Cardinality.UNKNOWN => cost
          case Cardinality.LOW     => Long.MaxValue
        }
    }
  }
}

object AttributeFilterStrategy {

  val StaticCost = 100L

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
