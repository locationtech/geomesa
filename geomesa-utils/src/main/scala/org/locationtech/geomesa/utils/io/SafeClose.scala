/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.io

import com.typesafe.scalalogging.LazyLogging

import scala.util.control.NonFatal

/**
  * Closes anything with a 'close' method without throwing an exception
  */
trait SafeClose {
  def apply(c: Any { def close(): Unit }): Option[Throwable]
}

/**
  * Flushes anything with a 'flush' method without throwing an exception
  */
trait SafeFlush {
  def apply(c: Any { def flush(): Unit }): Option[Throwable]
}

/**
  * Closes and logs any exceptions
  */
object CloseWithLogging extends SafeClose with LazyLogging {
  override def apply(c: Any { def close(): Unit }): Option[Throwable] = try { c.close(); None } catch {
    case NonFatal(e) => logger.warn(s"Error calling close on '$c': ", e); Some(e)
  }
}

/**
  * Closes and catches any exceptions
  */
object CloseQuietly extends SafeClose {
  override def apply(c: Any { def close(): Unit }): Option[Throwable] = try { c.close(); None } catch {
    case NonFatal(e) => Some(e)
  }
}

/**
  * Similar to java's try-with-resources, allows for using an object then closing in a finally block
  */
object WithClose {
  // defined for up to 3 variables, implement more methods if needed
  def apply[T <: Any { def close(): Unit }, V](c: T)(fn: (T) => V): V = try { fn(c) } finally { c.close() }
  def apply[T <: Any { def close(): Unit }, V](c0: T, c1: T)(fn: Tuple2[T, T] => V): V =
    try { try { fn(c0, c1) } finally { c0.close() } } finally { c1.close() }
  def apply[T <: Any { def close(): Unit }, V](c0: T, c1: T, c2: T)(fn: Tuple3[T, T, T] => V): V =
    try { try { try { fn(c0, c1, c2) } finally { c0.close() } } finally { c1.close() } } finally { c2.close() }
}

/**
  * Flushes and logs any exceptions
  */
object FlushWithLogging extends SafeFlush with LazyLogging {
  override def apply(c: Any { def flush(): Unit }): Option[Throwable] = try { c.flush(); None } catch {
    case NonFatal(e) => logger.warn(s"Error calling flush on '$c': ", e); Some(e)
  }
}

/**
  * Flushes and catches any exceptions
  */
object FlushQuietly extends SafeFlush {
  override def apply(c: Any { def flush(): Unit }): Option[Throwable] = try { c.flush(); None } catch {
    case NonFatal(e) => Some(e)
  }
}