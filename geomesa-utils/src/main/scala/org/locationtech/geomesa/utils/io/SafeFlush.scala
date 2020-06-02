/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io.Flushable

import org.locationtech.geomesa.utils.io.IsFlushableImplicits.{ArrayIsFlushable, IterableIsFlushable, OptionIsFlushable}

import scala.util.{Failure, Success, Try}

/**
 * Flushes anything with a 'flush' method without throwing an exception
 */
trait SafeFlush {

  def apply[F : IsFlushable](f: F): Option[Throwable]

  def apply[F1 : IsFlushable, F2 : IsFlushable](f1: F1, f2: F2): Option[Throwable] = {
    apply(f1) match {
      case None => apply(f2)
      case Some(e) => apply(f2).foreach(e.addSuppressed); Some(e)
    }
  }

  def raise[F : IsFlushable](f: F): Unit = apply(f).foreach(e => throw e)

  def raise[F1 : IsFlushable, F2 : IsFlushable](f1: F1, f2: F2): Unit = apply(f1, f2).foreach(e => throw e)
}

trait IsFlushable[-F] {
  def flush(obj: F): Try[Unit]
}

object IsFlushable extends IsFlushableImplicits[Flushable] {
  override protected def flush(f: Flushable): Try[Unit] = Try(f.flush())
}

trait IsFlushableImplicits[F] {

  protected def flush(f: F): Try[Unit]

  implicit val flushableIsFlushable: IsFlushable[F] = new IsFlushable[F] {
    override def flush(obj: F): Try[Unit] = IsFlushableImplicits.this.flush(obj)
  }

  implicit val iterableIsFlushable: IterableIsFlushable[F] = new IterableIsFlushable()
  implicit val optionIsFlushable: OptionIsFlushable[F] = new OptionIsFlushable()
  implicit val arrayIsFlushable: ArrayIsFlushable[F] = new ArrayIsFlushable()
}

object IsFlushableImplicits {

  class IterableIsFlushable[F : IsFlushable] extends IsFlushable[Iterable[_ <: F]] {

    private val ev = implicitly[IsFlushable[F]]

    override def flush(obj: Iterable[_ <: F]): Try[Unit] = {
      var error: Throwable = null
      obj.foreach { f =>
        ev.flush(f).failed.foreach(e => if (error == null) { error = e } else { error.addSuppressed(e) })
      }
      if (error == null) { Success() } else { Failure(error) }
    }
  }

  class OptionIsFlushable[F : IsFlushable] extends IsFlushable[Option[_ <: F]] {

    private val ev = implicitly[IsFlushable[F]]

    override def flush(obj: Option[_ <: F]): Try[Unit] = {
      obj match {
        case None => Success()
        case Some(o) => ev.flush(o)
      }
    }
  }

  class ArrayIsFlushable[F : IsFlushable] extends IsFlushable[Array[_ <: F]] {

    private val ev = implicitly[IsFlushable[F]]

    override def flush(obj: Array[_ <: F]): Try[Unit] = {
      var error: Throwable = null
      obj.foreach { f =>
        ev.flush(f).failed.foreach(e => if (error == null) { error = e } else { error.addSuppressed(e) })
      }
      if (error == null) { Success() } else { Failure(error) }
    }
  }
}
