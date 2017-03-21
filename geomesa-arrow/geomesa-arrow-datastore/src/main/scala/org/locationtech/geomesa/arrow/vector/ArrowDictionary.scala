/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.security.SecureRandom
import java.util.concurrent.atomic.AtomicLong

import com.google.common.collect.ImmutableBiMap

case class ArrowDictionary(values: ImmutableBiMap[AnyRef, Int], id: Long = ArrowDictionary.nextId)

object ArrowDictionary {
  private val r = new SecureRandom
  private val ids = new AtomicLong(r.nextLong)

  def nextId: Long = ids.getAndSet(r.nextLong)
}