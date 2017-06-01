/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import java.util.Date
import java.util.regex.Pattern

import com.googlecode.cqengine.attribute.Attribute
import com.googlecode.cqengine.query.Query
import com.googlecode.cqengine.{query => cqquery}
import com.vividsolutions.jts.geom.Geometry
import org.geotools.filter.LikeToRegexConverter
import org.geotools.filter.visitor.AbstractFilterVisitor
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.memory.cqengine.query.{GeoToolsFilterQuery, Intersects => CQIntersects}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._
import org.opengis.temporal.Period

import scala.collection.JavaConversions._
import scala.language._

class CQEngineQueryVisitor(sft: SimpleFeatureType) extends AbstractFilterVisitor {
  implicit val lookup = SFTAttributes(sft)

  /* Logical operators */

  /**
    * And
    */
  override def visit(filter: And, data: scala.Any): AnyRef = {
    val children = filter.getChildren

    val query = children.map { f =>
      f.accept(this, null) match {
        case q: Query[SimpleFeature] => q
        case _ => throw new RuntimeException(s"Can't parse filter: $f.")
      }
    }.toList
    new cqquery.logical.And[SimpleFeature](query)
  }

  /**
    * Or
    */
  override def visit(filter: Or, data: scala.Any): AnyRef = {
    val children = filter.getChildren

    val query = children.map { f =>
      f.accept(this, null) match {
        case q: Query[SimpleFeature] => q
        case _ => throw new RuntimeException(s"Can't parse filter: $f.")
      }
    }.toList
    new cqquery.logical.Or[SimpleFeature](query)
  }

  /**
    * Not
    */
  override def visit(filter: Not, data: scala.Any): AnyRef = {
    val subfilter = filter.getFilter

    val subquery = subfilter.accept(this, null) match {
      case q: Query[SimpleFeature] => q
      case _ => throw new RuntimeException(s"Can't parse filter: $subfilter.")
    }
    new cqquery.logical.Not[SimpleFeature](subquery)
  }

  /* Id, null, nil, exclude, include */

  /**
    * Id
    */
  override def visit(filter: Id, extractData: scala.AnyRef): AnyRef = {
    val attr = SFTAttributes.fidAttribute
    val values = filter.getIDs.map(_.toString)
    new cqquery.simple.In[SimpleFeature, String](attr, true, values)
  }

  /**
    * PropertyIsNil:
    * follows the example of IsNilImpl by using the same implementation as PropertyIsNull
    */
  override def visit(filter: PropertyIsNil, extraData: scala.Any): AnyRef = {
    val prop = getProp(filter)
    val attr = lookup.lookup[Any](prop.name)
    new cqquery.logical.Not[SimpleFeature](new cqquery.simple.Has(attr))
  }

  /**
    * PropertyIsNull
    */
  override def visit(filter: PropertyIsNull, data: scala.Any): AnyRef = {
    val prop = getProp(filter)
    val attr = lookup.lookup[Any](prop.name)
    // TODO: could this be done better?
    new cqquery.logical.Not[SimpleFeature](new cqquery.simple.Has(attr))
  }

  /**
    * ExcludeFilter
    */
  override def visit(filter: ExcludeFilter, data: scala.Any): AnyRef = new cqquery.simple.None(classOf[SimpleFeature])

  /**
    * IncludeFilter
    * (handled a level above if IncludeFilter is the root node)
    */
  override def visit(filter: IncludeFilter, data: scala.Any): AnyRef = new cqquery.simple.All(classOf[SimpleFeature])

  /* MultiValuedFilters */

  /**
    * PropertyIsEqualTo
    */
  override def visit(filter: PropertyIsEqualTo, data: scala.Any): AnyRef = {
    val prop = getProp(filter)
    val attribute: Attribute[SimpleFeature, Any] = lookup.lookup[Any](prop.name)
    val value = prop.literal.evaluate(null, attribute.getAttributeType)
    new cqquery.simple.Equal(attribute, value)
  }

  /**
    * PropertyIsGreaterThan
    */
  override def visit(filter: PropertyIsGreaterThan, data: scala.Any): cqquery.simple.GreaterThan[SimpleFeature, _] = {
    val prop = getProp(filter)
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntGTQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongGTQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatGTQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleGTQuery(prop)
      case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateGTQuery(prop)
      case c => throw new RuntimeException(s"PropertyIsGreaterThan: $c not supported")
    }
  }

  /**
    * PropertyIsGreaterThanOrEqualTo
    */
  override def visit(filter: PropertyIsGreaterThanOrEqualTo, data: scala.Any): cqquery.simple.GreaterThan[SimpleFeature, _] = {
    val prop = getProp(filter)
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntGTEQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongGTEQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatGTEQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleGTEQuery(prop)
      case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateGTEQuery(prop)
      case c => throw new RuntimeException(s"PropertyIsGreaterThanOrEqualTo: $c not supported")
    }
  }

  /**
    * PropertyIsLessThan
    */
  override def visit(filter: PropertyIsLessThan, data: scala.Any): cqquery.simple.LessThan[SimpleFeature, _] = {
    val prop = getProp(filter)
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntLTQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongLTQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatLTQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleLTQuery(prop)
      case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateLTQuery(prop)
      case c => throw new RuntimeException(s"PropertyIsLessThan: $c not supported")
    }
  }

  /**
    * PropertyIsLessThanOrEqualTo
    */
  override def visit(filter: PropertyIsLessThanOrEqualTo, data: scala.Any): cqquery.simple.LessThan[SimpleFeature, _] = {
    val prop = getProp(filter)
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntLTEQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongLTEQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatLTEQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleLTEQuery(prop)
      case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateLTEQuery(prop)
      case c => throw new RuntimeException(s"PropertyIsLessThanOrEqualTo: $c not supported")
    }
  }

  /**
    * PropertyIsNotEqualTo
    */
  override def visit(filter: PropertyIsNotEqualTo, data: scala.Any): AnyRef = {
    val prop = getProp(filter)
    val attribute: Attribute[SimpleFeature, Any] = lookup.lookup[Any](prop.name)
    val value: Any = prop.literal.evaluate(null, attribute.getAttributeType)
    // TODO: could this be done better?
    // may not be as big an issue as PropertyIsNull, as I'm not
    // even sure how to build this filter in (E)CQL
    new cqquery.logical.Not(new cqquery.simple.Equal(attribute, value))
  }

  /**
    * PropertyIsBetween
    * (in the OpenGIS spec, lower and upper are inclusive)
   */
  override def visit(filter: PropertyIsBetween, data: scala.Any): AnyRef = {
    val prop = getProp(filter)
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntBetweenQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongBetweenQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatBetweenQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleBetweenQuery(prop)
      case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateBetweenQuery(prop)
      case c => throw new RuntimeException(s"PropertyIsBetween: $c not supported")
    }
  }

  /**
    * PropertyIsLike
    */
  override def visit(filter: PropertyIsLike, data: scala.Any): AnyRef = {
    val prop = getProp(filter)
    val attr = lookup.lookup[String](prop.name)

    val converter = new LikeToRegexConverter(filter)
    val pattern = if (filter.isMatchingCase)
                    Pattern.compile(converter.getPattern)
                  else
                    Pattern.compile(converter.getPattern, Pattern.CASE_INSENSITIVE)

    new cqquery.simple.StringMatchesRegex[SimpleFeature, String](attr, pattern)
  }

  /* Spatial filters */

  /**
    * BBOX
    */
  override def visit(filter: BBOX, data: scala.Any): AnyRef = {
    val prop = getProp(filter)
    val attributeName = prop.name
    val geom = prop.literal.evaluate(null, classOf[Geometry])
    val geomAttribute = lookup.lookup[Geometry](attributeName)

    new CQIntersects(geomAttribute, geom)
  }

  /**
    * Intersects
    */
  override def visit(filter: Intersects, data: scala.Any): AnyRef = {
    val prop = getProp(filter)
    val attributeName = prop.name
    val geom = prop.literal.evaluate(null, classOf[Geometry])
    val geomAttribute = lookup.lookup[Geometry](attributeName)

    new CQIntersects(geomAttribute, geom)
  }

  /**
    * BinarySpatialOperator: fallback non-indexable implementation of other spatial operators.
    * Handles:
    *    Contains, Crosses, Disjoint, Beyond, DWithin, Equals, Overlaps, Touches, Within
    */
  override def visit(filter: BinarySpatialOperator, data: scala.Any): AnyRef = handleGeneralCQLFilter(filter)

  /* Temporal filters */

  /**
    * After: only for time attributes, and is exclusive
    */
  override def visit(after: After, extraData: scala.Any): AnyRef = {
    val prop = getProp(after)
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[Date].isAssignableFrom(c) => {
        val attr = lookup.lookup[Date](prop.name)
        val value = prop.literal.evaluate(null, classOf[Date])
        new cqquery.simple.GreaterThan[SimpleFeature, Date](attr, value, false)
      }
      case c => throw new RuntimeException(s"After: $c not supported")
    }
  }

  /**
    * Before: only for time attributes, and is exclusive
    */
  override def visit(before: Before, extraData: scala.Any): AnyRef = {
    val prop = getProp(before)
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[Date].isAssignableFrom(c) => {
        val attr = lookup.lookup[Date](prop.name)
        val value = prop.literal.evaluate(null, classOf[Date])
        new cqquery.simple.LessThan[SimpleFeature, Date](attr, value, false)
      }
      case c => throw new RuntimeException(s"Before: $c not supported")
    }
  }

  /**
    * During: only for time attributes, and is exclusive at both ends
    */
  override def visit(during: During, extraData: scala.Any): AnyRef = {
    val prop = getProp(during)
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[Date].isAssignableFrom(c) => {
        val attr = lookup.lookup[Date](prop.name)
        val p = prop.literal.evaluate(null, classOf[Period])
        val lower = p.getBeginning.getPosition.getDate
        val upper = p.getEnding.getPosition.getDate
        new cqquery.simple.Between[SimpleFeature, java.util.Date](attr, lower, false, upper, false)
      }
      case c => throw new RuntimeException(s"During: $c not supported")
    }
  }

  /**
    * BinaryTemporalOperator: Fallback non-indexable implementation of other temporal operators.
    * Handles:
    *     AnyInteracts, Begins, BegunBy, EndedBy, Ends, Meets,
    *     MetBy, OverlappedBy, TContains, TEquals, TOverlaps
    */
  override def visit(filter: BinaryTemporalOperator, data: scala.Any): AnyRef = handleGeneralCQLFilter(filter)

  def handleGeneralCQLFilter(filter: Filter): Query[SimpleFeature] = {
    new GeoToolsFilterQuery(filter)
  }

  /**
    * Build a PropertyLiteral for every expression with a property name in it
    * (essentially a wrapper around getAttributeProperty)
    */
  def getProp(filter: Filter): PropertyLiteral = {
    val prop = filter match {
      case f: BinarySpatialOperator => checkOrder(f.getExpression1, f.getExpression2)
      case f: BinaryTemporalOperator => checkOrder(f.getExpression1, f.getExpression2)
      case f: PropertyIsNotEqualTo => checkOrder(f.getExpression1, f.getExpression2)
      // we support a wider range of PropertyIsLike filters than getAttributeProperty does
      case f: PropertyIsLike => {
        val propName = f.getExpression.asInstanceOf[PropertyName].getPropertyName
        Some(PropertyLiteral(propName, ff.literal(f.getLiteral), None))
      }
      case f: PropertyIsNull => {
        val propName = f.getExpression.asInstanceOf[PropertyName].getPropertyName
        Some(PropertyLiteral(propName, ff.literal(null), None))
      }
      case f: PropertyIsNil => {
        val propName = f.getExpression.asInstanceOf[PropertyName].getPropertyName
        Some(PropertyLiteral(propName, ff.literal(null), None))
      }
      case f => getAttributeProperty(f)
    }
    prop match {
      case Some(p) => p
      case None => throw new RuntimeException(s"Can't parse filter $filter")
    }
  }
}
