/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import java.lang
import java.util.Date

import com.googlecode.cqengine.attribute.Attribute
import com.googlecode.cqengine.query.simple.{Between, GreaterThan, LessThan, SimpleQuery}
import org.opengis.feature.simple.SimpleFeature

trait CompQueryBuilder {
  // The type the product queries process, and its class object
  type VALUE <: Comparable[VALUE]
  val valueClass: Class[VALUE]    // needed to interact with gt's Java API.
  type ATTRIBUTE = Attribute[SimpleFeature, VALUE]
  // The data needed to build a query, often expressed in terms of VALUE
  type DATA
  // The type of query to produce
  type QUERY <: SimpleQuery[SimpleFeature, VALUE]

  protected [this] def mkQuery(attr: ATTRIBUTE, data: DATA): QUERY

  def apply(name: String, data: DATA)(implicit lookup: SFTAttributes): QUERY = {
    val attr = lookup.lookup[VALUE](name)
    mkQuery(attr, data)
  }
}

trait GTQueryBuilder extends CompQueryBuilder {
  type QUERY = GreaterThan[SimpleFeature, VALUE]
  type DATA = VALUE
  val orEqual: Boolean

  protected [this] def mkQuery(attr: ATTRIBUTE, data: DATA): QUERY =
    new GreaterThan(attr, data, orEqual)
}

trait LTQueryBuilder extends CompQueryBuilder {
  type QUERY = LessThan[SimpleFeature, VALUE]
  type DATA = VALUE
  val orEqual: Boolean

  protected [this] def mkQuery(attr: ATTRIBUTE, data: DATA): QUERY =
    new LessThan(attr, data, orEqual)
}

// an example of how DATA might differ from simply VALUE
trait BetweenQueryBuilder extends CompQueryBuilder {

  type QUERY = Between[SimpleFeature, VALUE]
  type DATA = (VALUE, VALUE)
  // GeoTools PropertyIsBetween is inclusive at both ends
  val lowerInclusive = true
  val upperInclusive = true

  protected [this] def mkQuery(attr: ATTRIBUTE, data: DATA): QUERY =
    new Between(attr, data._1, lowerInclusive, data._2, upperInclusive)
}

object BuildIntGTQuery extends GTQueryBuilder {
  type VALUE = java.lang.Integer
  override val valueClass: Class[Integer] = classOf[java.lang.Integer]
  val orEqual = false
}
object BuildLongGTQuery extends GTQueryBuilder {
  type VALUE = java.lang.Long
  override val valueClass: Class[lang.Long] = classOf[java.lang.Long]
  val orEqual = false
}
object BuildFloatGTQuery extends GTQueryBuilder {
  type VALUE = java.lang.Float
  override val valueClass: Class[lang.Float] = classOf[java.lang.Float]
  val orEqual = false
}
object BuildDoubleGTQuery extends GTQueryBuilder {
  type VALUE = java.lang.Double
  override val valueClass: Class[lang.Double] = classOf[java.lang.Double]
  val orEqual = false
}
object BuildDateGTQuery extends GTQueryBuilder {
  type VALUE = java.util.Date
  override val valueClass: Class[Date] = classOf[java.util.Date]
  val orEqual = false
}
object BuildStringGTQuery extends GTQueryBuilder {
  type VALUE = java.lang.String
  override val orEqual: Boolean = false
  override val valueClass: Class[String] = classOf[VALUE]
}
object BuildIntGTEQuery extends GTQueryBuilder {
  type VALUE = java.lang.Integer
  override val valueClass: Class[Integer] = classOf[java.lang.Integer]
  val orEqual = true
}
object BuildLongGTEQuery extends GTQueryBuilder {
  type VALUE = java.lang.Long
  override val valueClass: Class[lang.Long] = classOf[java.lang.Long]
  val orEqual = true
}
object BuildFloatGTEQuery extends GTQueryBuilder {
  type VALUE = java.lang.Float
  override val valueClass: Class[lang.Float] = classOf[java.lang.Float]
  val orEqual = true
}
object BuildDoubleGTEQuery extends GTQueryBuilder {
  type VALUE = java.lang.Double
  override val valueClass: Class[lang.Double] = classOf[java.lang.Double]
  val orEqual = true
}
object BuildDateGTEQuery extends GTQueryBuilder {
  type VALUE = java.util.Date
  override val valueClass: Class[Date] = classOf[java.util.Date]
  val orEqual = true
}
object BuildStringGTEQuery extends GTQueryBuilder {
  type VALUE = java.lang.String
  override val valueClass: Class[String] = classOf[java.lang.String]
  val orEqual = true
}

object BuildIntLTQuery extends LTQueryBuilder {
  type VALUE = java.lang.Integer
  override val valueClass: Class[Integer] = classOf[java.lang.Integer]
  val orEqual = false
}
object BuildLongLTQuery extends LTQueryBuilder {
  type VALUE = java.lang.Long
  override val valueClass: Class[lang.Long] = classOf[java.lang.Long]
  val orEqual = false
}
object BuildFloatLTQuery extends LTQueryBuilder {
  type VALUE = java.lang.Float
  override val valueClass: Class[lang.Float] = classOf[java.lang.Float]
  val orEqual = false
}
object BuildDoubleLTQuery extends LTQueryBuilder {
  type VALUE = java.lang.Double
  override val valueClass: Class[lang.Double] = classOf[java.lang.Double]
  val orEqual = false
}
object BuildDateLTQuery extends LTQueryBuilder {
  type VALUE = java.util.Date
  override val valueClass: Class[Date] = classOf[java.util.Date]
  val orEqual = false
}
object BuildStringLTQuery extends LTQueryBuilder {
  type VALUE = java.lang.String
  override val valueClass: Class[String] = classOf[java.lang.String]
  val orEqual = false
}

object BuildIntLTEQuery extends LTQueryBuilder {
  type VALUE = java.lang.Integer
  override val valueClass: Class[Integer] = classOf[java.lang.Integer]
  val orEqual = true
}
object BuildLongLTEQuery extends LTQueryBuilder {
  type VALUE = java.lang.Long
  override val valueClass: Class[lang.Long] = classOf[java.lang.Long]
  val orEqual = true
}
object BuildFloatLTEQuery extends LTQueryBuilder {
  type VALUE = java.lang.Float
  override val valueClass: Class[lang.Float] = classOf[java.lang.Float]
  val orEqual = true
}
object BuildDoubleLTEQuery extends LTQueryBuilder {
  type VALUE = java.lang.Double
  override val valueClass: Class[lang.Double] = classOf[java.lang.Double]
  val orEqual = true
}
object BuildDateLTEQuery extends LTQueryBuilder {
  type VALUE = java.util.Date
  override val valueClass: Class[Date] = classOf[java.util.Date]
  val orEqual = true
}
object BuildStringLTEQuery extends LTQueryBuilder {
  type VALUE = java.lang.String
  override val valueClass: Class[String] = classOf[java.lang.String]
  val orEqual = true
}

object BuildIntBetweenQuery extends BetweenQueryBuilder {
  type VALUE = java.lang.Integer
  override val valueClass: Class[Integer] = classOf[java.lang.Integer]
}
object BuildLongBetweenQuery extends BetweenQueryBuilder {
  type VALUE = java.lang.Long
  override val valueClass: Class[lang.Long] = classOf[java.lang.Long]
}
object BuildFloatBetweenQuery extends BetweenQueryBuilder {
  type VALUE = java.lang.Float
  override val valueClass: Class[lang.Float] = classOf[java.lang.Float]
}
object BuildDoubleBetweenQuery extends BetweenQueryBuilder {
  type VALUE = java.lang.Double
  override val valueClass: Class[lang.Double] = classOf[java.lang.Double]
}
object BuildDateBetweenQuery   extends BetweenQueryBuilder {
  type VALUE = java.util.Date
  override val valueClass: Class[Date] = classOf[java.util.Date]
}
object BuildStringBetweenQuery   extends BetweenQueryBuilder {
  type VALUE = java.lang.String
  override val valueClass: Class[String] = classOf[java.lang.String]
}
