/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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