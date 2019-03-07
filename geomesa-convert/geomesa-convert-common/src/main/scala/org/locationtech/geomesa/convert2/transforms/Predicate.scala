/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.locationtech.geomesa.convert.EvaluationContext

trait Predicate {
  def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean
}

object Predicate {

  def apply(e: String): Predicate = PredicateParser.parse(e)

  case class BinaryEquals(left: Expression, right: Expression) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean =
      left.eval(args) == right.eval(args)
  }

  case class BinaryNotEquals(left: Expression, right: Expression) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean =
      left.eval(args) != right.eval(args)
  }

  case class BinaryLessThan(left: Expression, right: Expression) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean =
      left.eval(args).asInstanceOf[Comparable[Any]].compareTo(right.eval(args)) < 0
  }

  case class BinaryLessThanOrEquals(left: Expression, right: Expression) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean =
      left.eval(args).asInstanceOf[Comparable[Any]].compareTo(right.eval(args)) <= 0
  }

  case class BinaryGreaterThan(left: Expression, right: Expression) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean =
      left.eval(args).asInstanceOf[Comparable[Any]].compareTo(right.eval(args)) > 0
  }

  case class BinaryGreaterThanOrEquals(left: Expression, right: Expression) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean =
      left.eval(args).asInstanceOf[Comparable[Any]].compareTo(right.eval(args)) >= 0
  }

  case class And(clause: Predicate, clauses: Seq[Predicate]) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean =
      clause.eval(args) && clauses.forall(_.eval(args))
  }

  case class Or(clause: Predicate, clauses: Seq[Predicate])  extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean =
      clause.eval(args) || clauses.exists(_.eval(args))
  }

  case class Not(p: Predicate) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean = !p.eval(args)
  }
}
