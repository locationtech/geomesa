/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka.utils

import org.opengis.filter._
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._

import scala.collection.JavaConversions._

class GraphVizFilterVisitor extends FilterVisitor {
  override def visit(filter: And, extraData: scala.Any): AnyRef = {
    val count = extraDataToCount(extraData)

    printOperand(filter, "AND", count)
    //filter.getChildren.foreach(_.accept(this, count + 1))
    linkChildren(filter, count)
  }

  override def visit(filter: Or, extraData: scala.Any): AnyRef = {
    val count = extraDataToCount(extraData)

    printOperand(filter, "OR", count)
    //filter.getChildren.foreach(_.accept(this, count + 1))
    linkChildren(filter, count)
  }

  def linkChildren(binary: BinaryLogicOperator, count: Int) = {
    binary.getChildren.foreach { c =>
      val childRand = math.abs(scala.util.Random.nextInt())
      println(s"node_${nodeName(binary, count)} -> node_${nodeName(c, childRand)}")
      c.accept(this, childRand)
    }
    null
  }

  def printOperand(filter: Filter, op: String, count: Int) = {
    println(s"""node_${nodeName(filter, count)} [ label="$op" shape="rectangle"]""")

  }

  override def visit(filter: Not, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(metBy: MetBy, extraData: scala.Any): AnyRef = printRectangle(metBy, extraData)

  override def visit(meets: Meets, extraData: scala.Any): AnyRef = printRectangle(meets, extraData)

  override def visit(ends: Ends, extraData: scala.Any): AnyRef = printRectangle(ends, extraData)

  override def visit(endedBy: EndedBy, extraData: scala.Any): AnyRef = printRectangle(endedBy, extraData)

  override def visit(begunBy: BegunBy, extraData: scala.Any): AnyRef = printRectangle(begunBy, extraData)

  override def visit(begins: Begins, extraData: scala.Any): AnyRef = printRectangle(begins, extraData)

  override def visit(anyInteracts: AnyInteracts, extraData: scala.Any): AnyRef = printRectangle(anyInteracts, extraData)

  override def visit(contains: TOverlaps, extraData: scala.Any): AnyRef = printRectangle(contains, extraData)

  override def visit(equals: TEquals, extraData: scala.Any): AnyRef = printRectangle(equals, extraData)

  override def visit(contains: TContains, extraData: scala.Any): AnyRef = printRectangle(contains, extraData)

  override def visit(overlappedBy: OverlappedBy, extraData: scala.Any): AnyRef = printRectangle(overlappedBy, extraData)

  override def visit(filter: Touches, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: Overlaps, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(during: During, extraData: scala.Any): AnyRef = printRectangle(during, extraData)

  override def visit(before: Before, extraData: scala.Any): AnyRef = printRectangle(before, extraData)

  override def visit(after: After, extraData: scala.Any): AnyRef = printRectangle(after, extraData)

  override def visit(filter: Id, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: Equals, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: IncludeFilter, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: DWithin, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: Within, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsLessThanOrEqualTo, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsLessThan, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsGreaterThanOrEqualTo, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsGreaterThan, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsNotEqualTo, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsEqualTo, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsBetween, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: ExcludeFilter, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsLike, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsNull, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: PropertyIsNil, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: BBOX, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: Beyond, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: Contains, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: Crosses, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visit(filter: Disjoint, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  override def visitNullFilter(extraData: scala.Any): AnyRef = ??? // printRectangle(filter, extraData)

  override def visit(filter: Intersects, extraData: scala.Any): AnyRef = printRectangle(filter, extraData)

  def printRectangle(filter: Filter, extraData: scala.Any) = {
    val count = extraDataToCount(extraData)
    println(s"""node_${nodeName(filter, count)} [ label="${filter.toString}" shape="rectangle"]""")
    null
  }

  def extraDataToCount(extraData: scala.Any): Int = {
    extraData match {
      case i: Int => i
      case _ => 0
    }
  }

  def nodeName(filter: Filter, count: Int): String = {
    s"${count}_${filter.hashCode().toLong.toHexString}"
  }
}
