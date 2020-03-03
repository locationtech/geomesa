/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io
package records

import java.io.ByteArrayInputStream
import java.nio.channels.Channels

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.{FieldVector, VectorLoader}
import org.locationtech.geomesa.utils.io.WithClose

/**
 * Record batch unloader
 *
 * For some reason, this only works if the loaded vector is created new from the field
 *
 * @param vector vector to load
 */
class RecordBatchLoader[T <: FieldVector](vector: T) {

  // note: root is closeable but only closes the vector, which we don't want
  private val root = createRoot(vector)
  private val loader = new VectorLoader(root)

  def load(bytes: Array[Byte]): Unit = load(bytes, 0, bytes.length)

  def load(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    WithClose(new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes, offset, length)))) { in =>
      WithClose(MessageSerializer.deserializeMessageBatch(in, vector.getAllocator)) { batch =>
        loader.load(batch.asInstanceOf[ArrowRecordBatch])
      }
    }
  }
}

object RecordBatchLoader {

  /**
   * Convenience method to load a vector. If loading multiple batches, create a single RecordBatchLoader and
   * re-use it
   *
   * @param vector vector to load
   * @param bytes record batch
   */
  def load(vector: FieldVector, bytes: Array[Byte]): Unit = load(vector, bytes, 0, bytes.length)

  /**
   * Convenience method to load a vector. If loading multiple batches, create a single RecordBatchLoader and
   * re-use it
   *
   * @param vector vector to load
   * @param bytes record batch
   * @param offset offset into the byte array to start loading
   * @param length number of bytes to load
   */
  def load(vector: FieldVector, bytes: Array[Byte], offset: Int, length: Int): Unit =
    new RecordBatchLoader(vector).load(bytes, offset, length)
}
