/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.expression

import java.util.Collections

import org.opengis.filter.expression.PropertyName
import org.opengis.filter.{Filter, FilterVisitor, Or}

/**
  * OR filter implementation for several OR'd equality filters that evaluates a property against a list of values
  *
  * @param property property name
  * @param values values to check for equality
  */
class OrSequentialEquality(property: PropertyName, values: Seq[AnyRef]) extends Or {

  import org.locationtech.geomesa.filter.factory.FastFilterFactory.factory

  import scala.collection.JavaConverters._

  private val children: Seq[Filter] = values.map(value => factory.equals(property, factory.literal(value)))

  override def getChildren: java.util.List[Filter] = children.toList.asJava

  override def evaluate(obj: AnyRef): Boolean = values.contains(property.evaluate(obj))

  override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)

  override def toString: String = children.mkString("[", " OR ", "]")

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: Or => children == o.getChildren.asScala
      case _ => false
    }
  }

  // note: this may break equals/hashCode contract with other implementations of OR...
  override def hashCode(): Int = children.hashCode()
}

object OrSequentialEquality {

  class OrSequentialListEquality(property: PropertyName, values: Seq[AnyRef])
      extends OrSequentialEquality(property, values) {

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
