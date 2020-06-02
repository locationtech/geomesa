/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils

import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success}

package object io {

  /**
   * Closes and logs any exceptions
   */
  object CloseWithLogging extends SafeClose with LazyLogging {
    override def apply[C : IsCloseable](c: C): Option[Throwable] = {
      implicitly[IsCloseable[C]].close(c) match {
        case _: Success[Unit] => None
        case Failure(e) => logger.warn(s"Error calling close on '$c': ", e); Some(e)
      }
    }
  }

  /**
   * Closes and catches any exceptions
   */
  object CloseQuietly extends SafeClose {
    override def apply[C : IsCloseable](c: C): Option[Throwable] =
      implicitly[IsCloseable[C]].close(c).failed.toOption
  }

  /**
   * Flushes and logs any exceptions
   */
  object FlushWithLogging extends SafeFlush with LazyLogging {
    override def apply[F : IsFlushable](f: F): Option[Throwable] = {
      implicitly[IsFlushable[F]].flush(f) match {
        case _: Success[Unit] => None
        case Failure(e) => logger.warn(s"Error calling flush on '$f': ", e); Some(e)
      }
    }
  }

  /**
   * Flushes and catches any exceptions
   */
  object FlushQuietly extends SafeFlush {
    override def apply[F : IsFlushable](f: F): Option[Throwable] =
      implicitly[IsFlushable[F]].flush(f).failed.toOption
  }

  /**
   * Similar to java's try-with-resources, allows for using an object then closing in a finally block
   */
  object WithClose {

    def apply[C : IsCloseable, T](c: C)(fn: C => T): T = {
      val ev = implicitly[IsCloseable[C]]
      try { fn(c) } finally { if (c != null) { ev.close(c) }}
    }

    def apply[C1 : IsCloseable, C2 : IsCloseable, T](c1: C1, c2: => C2)(fn: (C1, C2) => T): T =
      apply(c1) { c1 => apply(c2) { c2 => fn(c1, c2) } }
  }
}
