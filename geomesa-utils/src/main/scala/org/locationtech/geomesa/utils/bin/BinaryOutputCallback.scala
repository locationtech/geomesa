/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.bin

import java.nio.ByteBuffer

trait BinaryOutputCallback {

  /**
    * Callback for reduced (16-byte) values
    */
  def apply(trackId: Int, lat: Float, lon: Float, dtg: Long): Unit

  /**
    * Callback for expanded (24-byte) values
    */
  def apply(trackId: Int, lat: Float, lon: Float, dtg: Long, label: Long): Unit

  /**
    * Fills in basic values
    */
  protected def put(buffer: ByteBuffer, trackId: Int, lat: Float, lon: Float, dtg: Long): Unit = {
    buffer.putInt(trackId)
    buffer.putInt((dtg / 1000).toInt)
    buffer.putFloat(lat)
    buffer.putFloat(lon)
  }


  /**
    * Fills in extended values
    */
  protected def put(buffer: ByteBuffer, trackId: Int, lat: Float, lon: Float, dtg: Long, label: Long): Unit = {
    put(buffer, trackId, lat, lon, dtg)
    buffer.putLong(label)
  }
}
