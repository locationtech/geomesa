/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.io.SafeClose.AnyCloseable
import org.locationtech.geomesa.utils.io.SafeFlush.AnyFlushable

import scala.util.control.NonFatal

/**
  * Closes anything with a 'close' method without throwing an exception
  */
trait SafeClose {
  def apply(c: AnyCloseable): Option[Throwable]
}

object SafeClose {
  type AnyCloseable = Any { def close(): Unit }
}

/**
  * Closes and logs any exceptions
  */
object CloseWithLogging extends SafeClose with LazyLogging {
  override def apply(c: AnyCloseable): Option[Throwable] = try { c.close(); None } catch {
    case NonFatal(e) => logger.warn(s"Error calling close on '$c': ", e); Some(e)
  }
}

/**
  * Closes and catches any exceptions
  */
object CloseQuietly extends SafeClose {
  override def apply(c: AnyCloseable): Option[Throwable] = try { c.close(); None } catch {
    case NonFatal(e) => Some(e)
  }
}

/**
  * Similar to java's try-with-resources, allows for using an object then closing in a finally block
  */
object WithClose {
  // defined for up to 3 variables, implement more methods if needed
  def apply[A <: AnyCloseable, B](a: A)(fn: (A) => B): B = try { fn(a) } finally { if (a != null) { a.close() }}
  def apply[A <: AnyCloseable, B <: AnyCloseable, C](a: A, b: => B)(fn: (A, B) => C): C =
    apply(a) { a => apply(b) { b => fn(a, b) } }
  def apply[A <: AnyCloseable, B <: AnyCloseable, C <: AnyCloseable, D](a: A, b: => B, c: => C)(fn: (A, B, C) => D): D = {
    apply(a) { a => apply(b) { b => apply(c) { c => fn(a, b, c) } } }
  }
}

/**
  * Flushes anything with a 'flush' method without throwing an exception
  */
trait SafeFlush {
  def apply(c: AnyFlushable): Option[Throwable]
}

object SafeFlush {
  type AnyFlushable = Any { def flush(): Unit }
}

/**
  * Flushes and logs any exceptions
  */
object FlushWithLogging extends SafeFlush with LazyLogging {
  override def apply(c: AnyFlushable): Option[Throwable] = try { c.flush(); None } catch {
    case NonFatal(e) => logger.warn(s"Error calling flush on '$c': ", e); Some(e)
  }
}

/**
  * Flushes and catches any exceptions
  */
object FlushQuietly extends SafeFlush {
  override def apply(c: AnyFlushable): Option[Throwable] = try { c.flush(); None } catch {
    case NonFatal(e) => Some(e)
  }
}