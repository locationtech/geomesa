/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import com.typesafe.scalalogging.LazyLogging
import org.ehcache.sizeof.SizeOf
import org.ehcache.sizeof.impl.PassThroughFilter

import scala.util.control.NonFatal

trait Sizable {

  /**
   * Approximate size in memory, in bytes
   *
   * @return
   */
  def calculateSizeOf(): Long
}

object Sizable extends LazyLogging {

  private val sizeOf = try { SizeOf.newInstance() } catch {
    case NonFatal(e) =>
      logger.warn("Could not load SizeOf, memory estimates will be off:", e)
      new SizeOf(new PassThroughFilter(), true, true) {
        override def sizeOf(obj: Any): Long = 64
      }
  }

  def sizeOf(obj: Any): Long = sizeOf.sizeOf(obj)

  def deepSizeOf(obj: Any*): Long = sizeOf.deepSizeOf(obj)
}
