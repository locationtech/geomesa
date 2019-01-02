/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import com.googlecode.cqengine.query.simple.All
import com.googlecode.cqengine.{query => cqquery}
import org.locationtech.jts.geom.Geometry
import org.geotools.filter.LikeToRegexConverter
import org.geotools.filter.visitor.AbstractFilterVisitor
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.memory.cqengine.query.{GeoToolsFilterQuery, Intersects => CQIntersects}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.opengis.filter.expression.Literal
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._

import scala.collection.JavaConversions._
import scala.language._

class CQEngineQueryVisitor(sft: SimpleFeatureType) extends AbstractFilterVisitor {

  implicit val lookup: SFTAttributes = SFTAttributes(sft)

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
    if (query.exists(_.isInstanceOf[All[_]])) {
      new cqquery.simple.All(classOf[SimpleFeature])
    } else {
      new cqquery.logical.And[SimpleFeature](query)
    }
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
    if (query.exists(_.isInstanceOf[All[_]])) {
      new cqquery.simple.All(classOf[SimpleFeature])
    } else {
      new cqquery.logical.Or[SimpleFeature](query)
    }
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
    // In the event that the visitor cannot 'plan' a query, it returns an 'All' Query to indicate
    //  that all the Simple Features should be considered.
    // As such, we do not negate the query going back.
    if (subquery.isInstanceOf[All[_]]) {
      subquery
    } else {
      new cqquery.logical.Not[SimpleFeature](subquery)
    }
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
    val name = getAttribute(filter)
    val attr = lookup.lookup[Any](name)
    new cqquery.logical.Not[SimpleFeature](new cqquery.simple.Has(attr))
  }

  /**
    * PropertyIsNull
    */
  override def visit(filter: PropertyIsNull, data: scala.Any): AnyRef = {
    val name = getAttribute(filter)
    val attr = lookup.lookup[Any](name)
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
    val name = getAttribute(filter)
    val attribute: Attribute[SimpleFeature, Any] = lookup.lookup[Any](name)
    val bounds = FilterHelper.extractAttributeBounds(filter, name, attribute.getAttributeType).values.headOption.getOrElse {
      Bounds.everything[Any]
    }
    if(!bounds.isBounded) new cqquery.simple.All(classOf[SimpleFeature])
    else new cqquery.simple.Equal(attribute, bounds.lower.value.get)
  }

  /**
    * PropertyIsGreaterThan
    */
  override def visit(filter: PropertyIsGreaterThan, data: scala.Any): AnyRef = {
    val name = getAttribute(filter)
    val binding = sft.getDescriptor(name).getType.getBinding
    FilterHelper.extractAttributeBounds(filter, name, binding).values.headOption.getOrElse {
      Bounds.everything[Any]
    }.bounds match {
      case (Some(lo), None) =>
        binding match {
          case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntGTQuery(name, lo.asInstanceOf[java.lang.Integer])
          case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongGTQuery(name, lo.asInstanceOf[java.lang.Long])
          case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatGTQuery(name, lo.asInstanceOf[java.lang.Float])
          case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleGTQuery(name, lo.asInstanceOf[java.lang.Double])
          case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateGTQuery(name, lo.asInstanceOf[java.util.Date])
          case c if classOf[java.lang.String ].isAssignableFrom(c) => BuildStringGTQuery(name, lo.asInstanceOf[java.lang.String])
          case c => throw new RuntimeException(s"PropertyIsGreaterThan: $c not supported")
        }
      case (None, Some(hi)) =>
        binding match {
          case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntLTQuery(name, hi.asInstanceOf[java.lang.Integer])
          case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongLTQuery(name, hi.asInstanceOf[java.lang.Long])
          case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatLTQuery(name, hi.asInstanceOf[java.lang.Float])
          case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleLTQuery(name, hi.asInstanceOf[java.lang.Double])
          case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateLTQuery(name, hi.asInstanceOf[java.util.Date])
          case c if classOf[java.lang.String   ].isAssignableFrom(c) => BuildStringLTQuery(name, hi.asInstanceOf[java.lang.String])
          case c => throw new RuntimeException(s"PropertyIsGreaterThan: $c not supported")
        }
      case _ => new cqquery.simple.All(classOf[SimpleFeature])
    }
  }

  /**
    * PropertyIsGreaterThanOrEqualTo
    */
  override def visit(filter: PropertyIsGreaterThanOrEqualTo, data: scala.Any): AnyRef = {
    val name = getAttribute(filter)
    val binding = sft.getDescriptor(name).getType.getBinding
    FilterHelper.extractAttributeBounds(filter, name, binding).values.headOption.getOrElse {
      Bounds.everything[Any]
    }.bounds match {
      case (Some(lo), None) =>
        binding match {
          case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntGTEQuery(name, lo.asInstanceOf[java.lang.Integer])
          case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongGTEQuery(name, lo.asInstanceOf[java.lang.Long])
          case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatGTEQuery(name, lo.asInstanceOf[java.lang.Float])
          case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleGTEQuery(name, lo.asInstanceOf[java.lang.Double])
          case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateGTEQuery(name, lo.asInstanceOf[java.util.Date])
          case c if classOf[java.lang.String   ].isAssignableFrom(c) => BuildStringGTEQuery(name, lo.asInstanceOf[java.lang.String])
          case c => throw new RuntimeException(s"PropertyIsGreaterThanOrEqualTo: $c not supported")
        }
      case (None, Some(hi)) =>
        binding match {
          case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntLTEQuery(name, hi.asInstanceOf[java.lang.Integer])
          case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongLTEQuery(name, hi.asInstanceOf[java.lang.Long])
          case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatLTEQuery(name, hi.asInstanceOf[java.lang.Float])
          case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleLTEQuery(name, hi.asInstanceOf[java.lang.Double])
          case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateLTEQuery(name, hi.asInstanceOf[java.util.Date])
          case c if classOf[java.lang.String   ].isAssignableFrom(c) => BuildStringLTEQuery(name, hi.asInstanceOf[java.lang.String])
          case c => throw new RuntimeException(s"PropertyIsGreaterThanOrEqualTo: $c not supported")
        }
      case _ => new cqquery.simple.All(classOf[SimpleFeature])
    }
  }

  /**
    * PropertyIsLessThan
    */
  override def visit(filter: PropertyIsLessThan, data: scala.Any): AnyRef = {
    val name = getAttribute(filter)
    val binding = sft.getDescriptor(name).getType.getBinding
    FilterHelper.extractAttributeBounds(filter, name, binding).values.headOption.getOrElse {
      Bounds.everything[Any]
    }.bounds match {
      case (Some(lo), None) =>
        binding match {
          case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntGTQuery(name, lo.asInstanceOf[java.lang.Integer])
          case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongGTQuery(name, lo.asInstanceOf[java.lang.Long])
          case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatGTQuery(name, lo.asInstanceOf[java.lang.Float])
          case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleGTQuery(name, lo.asInstanceOf[java.lang.Double])
          case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateGTQuery(name, lo.asInstanceOf[java.util.Date])
          case c if classOf[java.lang.String   ].isAssignableFrom(c) => BuildStringGTQuery(name, lo.asInstanceOf[java.lang.String])
          case c => throw new RuntimeException(s"PropertyIsLessThan: $c not supported")
        }
      case (None, Some(hi)) =>
        binding match {
          case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntLTQuery(name, hi.asInstanceOf[java.lang.Integer])
          case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongLTQuery(name, hi.asInstanceOf[java.lang.Long])
          case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatLTQuery(name, hi.asInstanceOf[java.lang.Float])
          case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleLTQuery(name, hi.asInstanceOf[java.lang.Double])
          case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateLTQuery(name, hi.asInstanceOf[java.util.Date])
          case c if classOf[java.lang.String   ].isAssignableFrom(c) => BuildStringLTQuery(name, hi.asInstanceOf[java.lang.String])
          case c => throw new RuntimeException(s"PropertyIsLessThan: $c not supported")
        }
      case _ => new cqquery.simple.All(classOf[SimpleFeature])
    }
  }

  /**
    * PropertyIsLessThanOrEqualTo
    */
  override def visit(filter: PropertyIsLessThanOrEqualTo, data: scala.Any): AnyRef = {
    val name = getAttribute(filter)
    val binding = sft.getDescriptor(name).getType.getBinding
    FilterHelper.extractAttributeBounds(filter, name, binding).values.headOption.getOrElse {
      Bounds.everything[Any]
    }.bounds match {
      case (Some(lo), None) =>
        binding match {
          case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntGTEQuery(name, lo.asInstanceOf[java.lang.Integer])
          case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongGTEQuery(name, lo.asInstanceOf[java.lang.Long])
          case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatGTEQuery(name, lo.asInstanceOf[java.lang.Float])
          case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleGTEQuery(name, lo.asInstanceOf[java.lang.Double])
          case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateGTEQuery(name, lo.asInstanceOf[java.util.Date])
          case c if classOf[java.lang.String   ].isAssignableFrom(c) => BuildStringGTEQuery(name, lo.asInstanceOf[java.lang.String])
          case c => throw new RuntimeException(s"PropertyIsLessThanOrEqualTo: $c not supported")
        }
      case (None, Some(hi)) =>
        binding match {
          case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntLTEQuery(name, hi.asInstanceOf[java.lang.Integer])
          case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongLTEQuery(name, hi.asInstanceOf[java.lang.Long])
          case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatLTEQuery(name, hi.asInstanceOf[java.lang.Float])
          case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleLTEQuery(name, hi.asInstanceOf[java.lang.Double])
          case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateLTEQuery(name, hi.asInstanceOf[java.util.Date])
          case c if classOf[java.lang.String   ].isAssignableFrom(c) => BuildStringLTEQuery(name, hi.asInstanceOf[java.lang.String])
          case c => throw new RuntimeException(s"PropertyIsLessThanOrEqualTo: $c not supported")
        }
      case _ => new cqquery.simple.All(classOf[SimpleFeature])
    }
  }

  /**
    * PropertyIsNotEqualTo
    */
  override def visit(filter: PropertyIsNotEqualTo, data: scala.Any): AnyRef = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

    val name = getAttribute(filter)
    val attribute: Attribute[SimpleFeature, Any] = lookup.lookup[Any](name)
    val value: Any = Iterator(filter.getExpression1, filter.getExpression2).collect {
      case lit: Literal => lit.evaluate(null, attribute.getAttributeType)
    }.headOption.getOrElse {
      throw new RuntimeException(s"Can't parse not equal to values ${filterToString(filter)}")
    }
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
    val name = getAttribute(filter)
    val binding = sft.getDescriptor(name).getType.getBinding
    val values = FilterHelper.extractAttributeBounds(filter, name, binding).values.headOption.getOrElse {
      throw new RuntimeException(s"Can't parse less than or equal to values ${filterToString(filter)}")
    }
    val between = (values.lower.value.get, values.upper.value.get)

    binding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntBetweenQuery(name, between.asInstanceOf[(java.lang.Integer, java.lang.Integer)])
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongBetweenQuery(name, between.asInstanceOf[(java.lang.Long, java.lang.Long)])
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatBetweenQuery(name, between.asInstanceOf[(java.lang.Float, java.lang.Float)])
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleBetweenQuery(name, between.asInstanceOf[(java.lang.Double, java.lang.Double)])
      case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateBetweenQuery(name, between.asInstanceOf[(java.util.Date, java.util.Date)])
      case c if classOf[java.lang.String   ].isAssignableFrom(c) => BuildStringBetweenQuery(name, between.asInstanceOf[(java.lang.String, java.lang.String)])
      case c => throw new RuntimeException(s"PropertyIsBetween: $c not supported")
    }
  }

  /**
    * PropertyIsLike
    */
  override def visit(filter: PropertyIsLike, data: scala.Any): AnyRef = {
    val name = getAttribute(filter)
    val attr = lookup.lookup[String](name)

    val converter = new LikeToRegexConverter(filter)
    val pattern = if (filter.isMatchingCase) {
      Pattern.compile(converter.getPattern)
    } else {
      Pattern.compile(converter.getPattern, Pattern.CASE_INSENSITIVE)
    }
    new cqquery.simple.StringMatchesRegex[SimpleFeature, String](attr, pattern)
  }

  /* Spatial filters */

  /**
    * BBOX
    */
  override def visit(filter: BBOX, data: scala.Any): AnyRef = {
    val name = getAttribute(filter)
    val geom = FilterHelper.extractGeometries(filter, name).values.headOption.getOrElse {
      throw new RuntimeException(s"Can't parse bbox values ${filterToString(filter)}")
    }
    val geomAttribute = lookup.lookup[Geometry](name)

    new CQIntersects(geomAttribute, geom)
  }

  /**
    * Intersects
    */
  override def visit(filter: Intersects, data: scala.Any): AnyRef = {
    val name = getAttribute(filter)
    val geom = FilterHelper.extractGeometries(filter, name).values.headOption.getOrElse {
      throw new RuntimeException(s"Can't parse intersects values ${filterToString(filter)}")
    }
    val geomAttribute = lookup.lookup[Geometry](name)

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
    val name = getAttribute(after)
    sft.getDescriptor(name).getType.getBinding match {
      case c if classOf[Date].isAssignableFrom(c) =>
        val attr = lookup.lookup[Date](name)
        FilterHelper.extractIntervals(after, name).values.headOption.getOrElse {
          throw new RuntimeException(s"Can't parse after values ${filterToString(after)}")
        }.bounds match {
          case (Some(lo), None) => new cqquery.simple.GreaterThan[SimpleFeature, Date](attr, Date.from(lo.toInstant), false)
          case (None, Some(hi)) => new cqquery.simple.LessThan[SimpleFeature, Date](attr, Date.from(hi.toInstant), false)
        }

      case c => throw new RuntimeException(s"After: $c not supported")
    }
  }

  /**
    * Before: only for time attributes, and is exclusive
    */
  override def visit(before: Before, extraData: scala.Any): AnyRef = {
    val name = getAttribute(before)
    sft.getDescriptor(name).getType.getBinding match {
      case c if classOf[Date].isAssignableFrom(c) =>
        val attr = lookup.lookup[Date](name)
        FilterHelper.extractIntervals(before, name).values.headOption.getOrElse {
          throw new RuntimeException(s"Can't parse before values ${filterToString(before)}")
        }.bounds match {
          case (Some(lo), None) => new cqquery.simple.GreaterThan[SimpleFeature, Date](attr, Date.from(lo.toInstant), false)
          case (None, Some(hi)) => new cqquery.simple.LessThan[SimpleFeature, Date](attr, Date.from(hi.toInstant), false)
        }

      case c => throw new RuntimeException(s"Before: $c not supported")
    }
  }

  /**
    * During: only for time attributes, and is exclusive at both ends
    */
  override def visit(during: During, extraData: scala.Any): AnyRef = {
    val name = getAttribute(during)
    sft.getDescriptor(name).getType.getBinding match {
      case c if classOf[Date].isAssignableFrom(c) =>
        val attr = lookup.lookup[Date](name)
        val bounds = FilterHelper.extractIntervals(during, name).values.headOption.getOrElse {
          throw new RuntimeException(s"Can't parse during values ${filterToString(during)}")
        }
        new cqquery.simple.Between[SimpleFeature, java.util.Date](attr, Date.from(bounds.lower.value.get.toInstant),
          bounds.lower.inclusive, Date.from(bounds.upper.value.get.toInstant), bounds.upper.inclusive)

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

  def getAttribute(filter: Filter): String = FilterHelper.propertyNames(filter, null).headOption.getOrElse {
    throw new RuntimeException(s"Can't parse filter ${filterToString(filter)}")
  }
}
