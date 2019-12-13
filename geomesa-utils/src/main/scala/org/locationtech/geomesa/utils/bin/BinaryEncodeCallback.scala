/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.bin

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.{ByteBuffer, ByteOrder}

/**
  * Callback interface to encode binary format. Call `apply` repeatedly, then call `result`
  *
  * @tparam T result of encoding
  */
trait BinaryEncodeCallback[T] extends BinaryOutputCallback {

  protected def stream: OutputStream

  /**
    * Encode the values in 16-byte binary format
    */
  override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long): Unit = {
    val buffer = BinaryEncodeCallback.buffers.get
    put(buffer, trackId, lat, lon, dtg)
    stream.write(buffer.array(), 0, 16)
  }

  /**
    * Encode the values in 24-byte binary format
    */
  override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long, label: Long): Unit = {
    val buffer = BinaryEncodeCallback.buffers.get
    put(buffer, trackId, lat, lon, dtg, label)
    stream.write(buffer.array(), 0, 24)
  }

  /**
    * Get the result of any previous calls to apply
    *
    * @return
    */
  def result: T
}

object BinaryEncodeCallback {

  private val buffers: ThreadLocal[ByteBuffer] = new ThreadLocal[ByteBuffer] {
    override def initialValue: ByteBuffer = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN)
    override def get: ByteBuffer = { val out = super.get; out.clear(); out }
  }

  private val streams: ThreadLocal[ByteArrayOutputStream] = new ThreadLocal[ByteArrayOutputStream] {
    override def initialValue: ByteArrayOutputStream = new ByteArrayOutputStream(24)
  }

  /**
    * Callback to serialize to a byte array. Uses thread-local state, so be sure to apply a given
    * operation (apply * n + result) from a single thread
    */
  object ByteArrayCallback extends BinaryEncodeCallback[Array[Byte]] {

    override protected def stream: ByteArrayOutputStream = streams.get

    override def result: Array[Byte] = {
      val result = stream.toByteArray
      stream.reset()
      result
    }
  }

  /**
    * Callback to serialize to an output stream
    *
    * @param stream stream to write to
    */
  class ByteStreamCallback(override protected val stream: OutputStream) extends BinaryEncodeCallback[Long] {
    private var count = 0L

    override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long): Unit = {
      super.apply(trackId, lat, lon, dtg)
      count += 1L
    }

    override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long, label: Long): Unit = {
      super.apply(trackId, lat, lon, dtg, label)
      count += 1L
    }

    override def result: Long = count
  }
}