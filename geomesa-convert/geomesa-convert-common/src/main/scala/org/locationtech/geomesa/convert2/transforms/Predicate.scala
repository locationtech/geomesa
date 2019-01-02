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

  class BinaryPredicate[T](left: Expression, right: Expression, compare: (T, T) => Boolean) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean =
      compare(left.eval(args).asInstanceOf[T], right.eval(args).asInstanceOf[T])
  }

  class BinaryLogicPredicate(l: Predicate, r: Predicate, f: (Boolean, Boolean) => Boolean) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean = f(l.eval(args), r.eval(args))
  }

  case class BinaryEquals[T](left: Expression, right: Expression) extends BinaryPredicate[T](left, right, _ == _)

  case class BinaryNotEquals[T](left: Expression, right: Expression) extends BinaryPredicate[T](left, right, _ != _)

  case class BinaryLessThan[T](left: Expression, right: Expression)(implicit ordering: Ordering[T])
      extends BinaryPredicate[T](left, right, ordering.lt)

  case class BinaryLessThanOrEquals[T](left: Expression, right: Expression)(implicit ordering: Ordering[T])
      extends BinaryPredicate[T](left, right, ordering.lteq)

  case class BinaryGreaterThan[T](left: Expression, right: Expression)(implicit ordering: Ordering[T])
      extends BinaryPredicate[T](left, right, ordering.gt)

  case class BinaryGreaterThanOrEquals[T](left: Expression, right: Expression)(implicit ordering: Ordering[T])
      extends BinaryPredicate[T](left, right, ordering.gteq)

  case class And(left: Predicate, right: Predicate) extends BinaryLogicPredicate(left, right, _ && _)

  case class Or(l: Predicate, r: Predicate) extends BinaryLogicPredicate(l, r, _ || _)

  case class Not(p: Predicate) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean = !p.eval(args)
  }
}
