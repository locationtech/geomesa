/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import org.locationtech.geomesa.utils.io.IsCloseableImplicits.{ArrayIsCloseable, IterableIsCloseable, OptionIsCloseable}

import scala.util.{Failure, Success, Try}

/**
  * Closes anything with a 'close' method without throwing an exception
  */
trait SafeClose {

  def apply[C : IsCloseable](c: C): Option[Throwable]

  def apply[C1 : IsCloseable, C2 : IsCloseable](c1: C1, c2: C2): Option[Throwable] = {
    apply(c1) match {
      case None => apply(c2)
      case Some(e) => apply(c2).foreach(e.addSuppressed); Some(e)
    }
  }

  def raise[C : IsCloseable](c: C): Unit = apply(c).foreach(e => throw e)

  def raise[C1 : IsCloseable, C2 : IsCloseable](c1: C1, c2: C2): Unit = apply(c1, c2).foreach(e => throw e)
}

trait IsCloseable[-C] {
  def close(obj: C): Try[Unit]
}

object IsCloseable extends IsCloseableImplicits[AutoCloseable] {
  override protected def close(c: AutoCloseable): Try[Unit] = Try(c.close())
}

trait IsCloseableImplicits[C] {

  protected def close(c: C): Try[Unit]

  implicit val closeableIsCloseable: IsCloseable[C] = new IsCloseable[C] {
    override def close(obj: C): Try[Unit] = IsCloseableImplicits.this.close(obj)
  }

  implicit val iterableIsCloseable: IterableIsCloseable[C] = new IterableIsCloseable()
  implicit val optionIsCloseable: OptionIsCloseable[C] = new OptionIsCloseable()
  implicit val arrayIsCloseable: ArrayIsCloseable[C] = new ArrayIsCloseable()
}

object IsCloseableImplicits {

  class IterableIsCloseable[C : IsCloseable] extends IsCloseable[Iterable[_ <: C]] {

    private val ev = implicitly[IsCloseable[C]]

    override def close(obj: Iterable[_ <: C]): Try[Unit] = {
      var error: Throwable = null
      obj.foreach { f =>
        ev.close(f).failed.foreach { e =>
          if (error == null) { error = e } else { error.addSuppressed(e) }
        }
      }
      if (error == null) { Success() } else { Failure(error) }
    }
  }

  class OptionIsCloseable[C : IsCloseable] extends IsCloseable[Option[_ <: C]] {

    private val ev = implicitly[IsCloseable[C]]

    override def close(obj: Option[_ <: C]): Try[Unit] = {
      obj match {
        case None => Success()
        case Some(o) => ev.close(o)
      }
    }
  }

  class ArrayIsCloseable[C : IsCloseable] extends IsCloseable[Array[_ <: C]] {

    private val ev = implicitly[IsCloseable[C]]

    override def close(obj: Array[_ <: C]): Try[Unit] = {
      var error: Throwable = null
      obj.foreach { f =>
        ev.close(f).failed.foreach(e => if (error == null) { error = e } else { error.addSuppressed(e) })
      }
      if (error == null) { Success() } else { Failure(error) }
    }
  }
}
