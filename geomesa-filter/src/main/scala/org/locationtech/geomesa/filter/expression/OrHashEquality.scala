/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.expression

import java.util.Collections

import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.{Filter, FilterVisitor, Or}

import scala.collection.immutable.HashSet

/**
  * OR filter implementation for many OR'd equality filters that uses a hash lookup instead of evaluating
  * equality for each filter serially
  *
  * @param property property name
  * @param values values to check for equality
  */
class OrHashEquality(property: PropertyName, values: HashSet[AnyRef]) extends Or {

  import org.locationtech.geomesa.filter.factory.FastFilterFactory.factory

  import scala.collection.JavaConverters._

  private val children: Set[Filter] = values.map(value => factory.equals(property, factory.literal(value)))

  override def getChildren: java.util.List[Filter] = children.toList.asJava

  override def evaluate(obj: AnyRef): Boolean = values.contains(property.evaluate(obj))

  override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)

  override def toString: String = children.mkString("[", " OR ", "]")

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: Or => children == o.getChildren.asScala.toSet
      case _ => false
    }
  }

  // note: this may break equals/hashCode contract with other implementations of OR...
  override def hashCode(): Int = children.hashCode()
}

object OrHashEquality {

  val OrHashThreshold = SystemProperty("geomesa.filter.hash.threshold", "5")

  class OrHashListEquality(property: PropertyName, values: HashSet[AnyRef])
      extends OrHashEquality(property: PropertyName, values: HashSet[AnyRef]) {

    override def evaluate(obj: AnyRef): Boolean = {
      val list = property.evaluate(obj).asInstanceOf[java.util.List[AnyRef]]
      val iter = if (list == null) { Collections.emptyIterator } else { list.iterator() }
      while (iter.hasNext) {
        if (values.contains(iter.next)) {
          return true
        }
      }
      false
    }
  }
}