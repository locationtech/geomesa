/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.EvaluationContext.ContextDependent

sealed trait Predicate extends ContextDependent[Predicate] {
<<<<<<< HEAD
  def apply(args: Array[AnyRef]): Boolean
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======

  def apply(args: Array[AnyRef]): Boolean

  @deprecated("Use `withContext` and `evaluate`")
  def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean =
    withContext(ctx).apply(args.asInstanceOf[Array[AnyRef]])
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
  def apply(args: Array[AnyRef]): Boolean
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
}

object Predicate {

  def apply(e: String): Predicate = PredicateParser.parse(e)

  case class BinaryEquals(left: Expression, right: Expression) extends Predicate {
    override def apply(args: Array[AnyRef]): Boolean = left(args) == right(args)
    override def withContext(ec: EvaluationContext): Predicate =
      BinaryEquals(left.withContext(ec), right.withContext(ec))
  }

  case class BinaryNotEquals(left: Expression, right: Expression) extends Predicate {
    override def apply(args: Array[AnyRef]): Boolean = left(args) != right(args)
    override def withContext(ec: EvaluationContext): Predicate =
      BinaryNotEquals(left.withContext(ec), right.withContext(ec))
  }

  case class BinaryLessThan(left: Expression, right: Expression) extends Predicate {
    override def apply(args: Array[AnyRef]): Boolean =
      left(args).asInstanceOf[Comparable[AnyRef]].compareTo(right(args)) < 0
    override def withContext(ec: EvaluationContext): Predicate =
      BinaryLessThan(left.withContext(ec), right.withContext(ec))
  }

  case class BinaryLessThanOrEquals(left: Expression, right: Expression) extends Predicate {
    override def apply(args: Array[AnyRef]): Boolean =
      left(args).asInstanceOf[Comparable[AnyRef]].compareTo(right(args)) <= 0
    override def withContext(ec: EvaluationContext): Predicate =
      BinaryLessThanOrEquals(left.withContext(ec), right.withContext(ec))
  }

  case class BinaryGreaterThan(left: Expression, right: Expression) extends Predicate {
    override def apply(args: Array[AnyRef]): Boolean =
      left(args).asInstanceOf[Comparable[AnyRef]].compareTo(right(args)) > 0
    override def withContext(ec: EvaluationContext): Predicate =
      BinaryGreaterThan(left.withContext(ec), right.withContext(ec))
  }

  case class BinaryGreaterThanOrEquals(left: Expression, right: Expression) extends Predicate {
    override def apply(args: Array[AnyRef]): Boolean =
      left(args).asInstanceOf[Comparable[AnyRef]].compareTo(right(args)) >= 0
    override def withContext(ec: EvaluationContext): Predicate =
      BinaryGreaterThanOrEquals(left.withContext(ec), right.withContext(ec))
  }

  case class And(clause: Predicate, clauses: Seq[Predicate]) extends Predicate {
    override def apply(args: Array[AnyRef]): Boolean =
      clause(args) && clauses.forall(_.apply(args))
    override def withContext(ec: EvaluationContext): Predicate =
      And(clause.withContext(ec), clauses.map(_.withContext(ec)))
  }

  case class Or(clause: Predicate, clauses: Seq[Predicate])  extends Predicate {
    override def apply(args: Array[AnyRef]): Boolean =
      clause(args) || clauses.exists(_.apply(args))
    override def withContext(ec: EvaluationContext): Predicate =
      Or(clause.withContext(ec), clauses.map(_.withContext(ec)))
  }

  case class Not(p: Predicate) extends Predicate {
    override def apply(args: Array[AnyRef]): Boolean = !p(args)
    override def withContext(ec: EvaluationContext): Predicate = Not(p.withContext(ec))
  }
}
