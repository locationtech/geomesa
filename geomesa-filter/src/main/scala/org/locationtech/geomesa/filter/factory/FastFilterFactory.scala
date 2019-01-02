/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.factory

import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.DuplicatingFilterVisitor
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.expression.AttributeExpression.{FunctionLiteral, PropertyLiteral}
import org.locationtech.geomesa.filter.expression.FastDWithin.DWithinLiteral
import org.locationtech.geomesa.filter.expression.FastPropertyIsEqualTo.{FastIsEqualTo, FastIsEqualToIgnoreCase, FastListIsEqualToAny}
import org.locationtech.geomesa.filter.expression.FastPropertyName.{FastPropertyNameAccessor, FastPropertyNameAttribute}
import org.locationtech.geomesa.filter.expression.OrHashEquality.OrHashListEquality
import org.locationtech.geomesa.filter.expression.OrSequentialEquality.OrSequentialListEquality
import org.locationtech.geomesa.filter.expression.{OrHashEquality, OrSequentialEquality}
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.utils.geotools.SimpleFeaturePropertyAccessor
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.MultiValuedFilter.MatchAction
import org.opengis.filter.expression.{Expression, PropertyName}
import org.opengis.filter.spatial.DWithin
import org.opengis.filter.{Filter, FilterFactory2, Or, PropertyIsEqualTo}
import org.opengis.geometry.Geometry
import org.xml.sax.helpers.NamespaceSupport

/**
  * Filter factory that creates optimized filters
  *
  * Note: usage expects the sft to be set in FastFilterFactory.sfts
  * FastFilterFactory.toFilter will handle this for you
 */
class FastFilterFactory private extends org.geotools.filter.FilterFactoryImpl with FilterFactory2 {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConverters._

  override def property(name: String): PropertyName = {
    val sft = FastFilterFactory.sfts.get
    val colon = name.indexOf(":")
    val local = if (colon == -1) { name } else { name.substring(colon + 1) }
    val index = sft.indexOf(local)
    if (index != -1) {
      new FastPropertyNameAttribute(name, index)
    } else {
      val sf = new SimpleFeatureBuilder(sft).buildFeature("")
      SimpleFeaturePropertyAccessor.getAccessor(sf, name) match {
        case Some(a) => new FastPropertyNameAccessor(name, a)
        case None    => super.property(name)
      }
    }
  }

  override def property(name: Name): PropertyName = property(name.getLocalPart)

  override def property(name: String, namespaceContext: NamespaceSupport): PropertyName = property(name)

  override def or(f: Filter, g: Filter): Or = or(java.util.Arrays.asList(f, g))

  override def or(filters: java.util.List[Filter]): Or = {
    if (filters.isEmpty) {
      return super.or(filters.asInstanceOf[java.util.List[_]])
    }

    val predicates = FilterHelper.flattenOr(filters.asScala)

    val props = scala.collection.mutable.HashSet.empty[String]
    val literals = scala.collection.immutable.HashSet.newBuilder[AnyRef]
    literals.sizeHint(predicates)

    predicates.foreach {
      case p: PropertyIsEqualTo if p.getMatchAction == MatchAction.ANY && p.isMatchingCase =>
        org.locationtech.geomesa.filter.checkOrder(p.getExpression1, p.getExpression2) match {
          case Some(PropertyLiteral(name, lit, _)) if !props.add(name) || props.size == 1 => literals += lit.getValue
          case _ => return super.or(filters.asInstanceOf[java.util.List[_]])
        }

      case _ => return super.or(filters.asInstanceOf[java.util.List[_]])
    }

    // if we've reached here, we have verified that all the child filters are equality matches on the same property
    val prop = property(props.head)
    val values = literals.result
    val isListType = Option(FastFilterFactory.sfts.get.getDescriptor(props.head)).exists(_.isList)

    if (values.size >= OrHashEquality.OrHashThreshold.get.toInt) {
      if (isListType) {
        new OrHashListEquality(prop, values)
      } else {
        new OrHashEquality(prop, values)
      }
    } else if (isListType) {
      new OrSequentialListEquality(prop, values.toSeq)
    } else {
      new OrSequentialEquality(prop, values.toSeq)
    }
  }

  override def equals(exp1: Expression, exp2: Expression): PropertyIsEqualTo =
    equal(exp1, exp2, matchCase = true, MatchAction.ANY)

  override def equal(exp1: Expression, exp2: Expression, matchCase: Boolean): PropertyIsEqualTo =
    equal(exp1, exp2, matchCase, MatchAction.ANY)

  override def equal(exp1: Expression, exp2: Expression, matchCase: Boolean, matchAction: MatchAction): PropertyIsEqualTo = {
    if (matchAction == MatchAction.ANY) {
      org.locationtech.geomesa.filter.checkOrder(exp1, exp2) match {
        case None =>
          super.equal(exp1, exp2, matchCase, matchAction)

        case Some(prop) =>
          val exp1 = prop match {
            case p: PropertyLiteral => property(p.name)
            case p: FunctionLiteral => p.function
          }
          val descriptor = FastFilterFactory.sfts.get.getDescriptor(prop.name)
          if (descriptor != null && descriptor.isList) {
            new FastListIsEqualToAny(exp1, prop.literal)
          } else if (matchCase) {
            new FastIsEqualTo(exp1, prop.literal)
          } else {
            new FastIsEqualToIgnoreCase(exp1, prop.literal)
          }
      }
    } else {
      super.equal(exp1, exp2, matchCase, matchAction)
    }
  }

  override def dwithin(name: String, geom: Geometry, distance: Double, units: String): DWithin =
    dwithin(property(name), literal(geom), distance, units)

  override def dwithin(name: String, geom: Geometry, distance: Double, units: String, action: MatchAction): DWithin =
    dwithin(property(name), literal(geom), distance, units, action)

  override def dwithin(exp1: Expression, exp2: Expression, distance: Double, units: String): DWithin =
    dwithin(exp1, exp2, distance, units, MatchAction.ANY)

  override def dwithin(exp1: Expression, exp2: Expression, distance: Double, units: String, action: MatchAction): DWithin = {
    if (action == MatchAction.ANY) {
      org.locationtech.geomesa.filter.checkOrder(exp1, exp2) match {
        case Some(PropertyLiteral(name, lit, _))     => new DWithinLiteral(property(name), lit, distance, units)
        case Some(FunctionLiteral(name, fn, lit, _)) => new DWithinLiteral(fn, lit, distance, units)
        case _ => super.dwithin(exp1, exp2, distance, units, action)
      }
    } else {
      super.dwithin(exp1, exp2, distance, units, action)
    }
  }
}

object FastFilterFactory {

  val factory = new FastFilterFactory

  val sfts = new ThreadLocal[SimpleFeatureType]()

  def toFilter(sft: SimpleFeatureType, ecql: String): Filter = {
    sfts.set(sft)
    try { ECQL.toFilter(ecql, factory) } finally {
      sfts.remove()
    }
  }

  def toExpression(sft: SimpleFeatureType, ecql: String): Expression = {
    sfts.set(sft)
    try { ECQL.toExpression(ecql, factory) } finally {
      sfts.remove()
    }
  }

  def optimize(sft: SimpleFeatureType, filter: Filter): Filter = {
    sfts.set(sft)
    try { filter.accept(new QueryPlanFilterVisitor(sft), factory).asInstanceOf[Filter] } finally {
      sfts.remove()
    }
  }

  def copy(sft: SimpleFeatureType, filter: Filter): Filter = {
    sfts.set(sft)
    try { filter.accept(new DuplicatingFilterVisitor(factory), null).asInstanceOf[Filter] } finally {
      sfts.remove()
    }
  }
}
