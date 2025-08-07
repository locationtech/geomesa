/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import java.util.concurrent.atomic.AtomicInteger

object TestFunctionFactory {

  val LazyAccess: ThreadLocal[AtomicInteger] = new ThreadLocal[AtomicInteger]() {
    override def initialValue(): AtomicInteger = new AtomicInteger(0)
  }

  def lazyTest(args: Array[Any]): Any = LazyAccess.get().getAndIncrement()
}

class TestFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(lazyTest)

  private val lazyTest = TransformerFunction("lazyTest")(TestFunctionFactory.lazyTest)
}
