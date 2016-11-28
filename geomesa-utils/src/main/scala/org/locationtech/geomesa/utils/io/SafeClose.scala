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
  * Closes anything with a 'close' method without propagating exceptions
  */
trait SafeClose {
  def apply(c: Any { def close(): Unit }): Unit
}

/**
  * Closes and logs any exceptions
  */
object CloseWithLogging extends SafeClose with LazyLogging {
  override def apply(c: Any { def close(): Unit }): Unit = try { c.close() } catch {
    case NonFatal(e) => logger.warn(s"Error calling close on '$c': ", e)
  }
}

/**
  * Closes and swallows any exceptions
  */
object CloseQuietly extends SafeClose {
  override def apply(c: Any { def close(): Unit }): Unit = try { c.close() } catch {
    case NonFatal(e) => // ignore
  }
}
