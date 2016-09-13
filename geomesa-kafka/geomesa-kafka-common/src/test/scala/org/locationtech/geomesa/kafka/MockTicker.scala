package org.locationtech.geomesa.kafka

import com.google.common.base.Ticker

class MockTicker extends Ticker {

  var tic: Long = 0L

  def read(): Long = tic
}