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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)

  def apply(args: Array[AnyRef]): Boolean

  @deprecated("Use `withContext` and `evaluate`")
  def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean =
    withContext(ctx).apply(args.asInstanceOf[Array[AnyRef]])
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
  def apply(args: Array[AnyRef]): Boolean
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
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
