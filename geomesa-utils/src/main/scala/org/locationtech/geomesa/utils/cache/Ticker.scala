/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.cache

trait Ticker {
  def currentTimeMillis(): Long
}

object Ticker {

  def mock(start: Long = 0L): MockTicker = new MockTicker(start)

  object SystemTicker extends Ticker {
    override def currentTimeMillis(): Long = System.currentTimeMillis()
  }

  class MockTicker(var millis: Long = 0L) extends Ticker {
    override def currentTimeMillis(): Long = millis
  }
}
