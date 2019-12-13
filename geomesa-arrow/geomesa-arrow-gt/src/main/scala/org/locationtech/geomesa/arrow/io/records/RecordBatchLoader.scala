/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io.records

import java.io.ByteArrayInputStream
import java.nio.channels.Channels

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.{FieldVector, VectorLoader}
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowIO
import org.locationtech.geomesa.utils.io.WithClose

/**
  * Note: be sure to close the vector
  *
  * @param vector vector
  * @param allocator allocator
  */
class RecordBatchLoader(val vector: FieldVector)(implicit allocator: BufferAllocator) {

  private val root = SimpleFeatureArrowIO.createRoot(vector)
  private val loader = new VectorLoader(root)

  def load(bytes: Array[Byte]): Unit = load(bytes, 0, bytes.length)

  def load(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    WithClose(new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes, offset, length)))) { in =>
      WithClose(MessageSerializer.deserializeMessageBatch(in, allocator).asInstanceOf[ArrowRecordBatch]) { recordBatch =>
        loader.load(recordBatch)
      }
    }
  }
}

object RecordBatchLoader {

  def apply(field: Field)(implicit allocator: BufferAllocator): RecordBatchLoader =
    apply(field.createVector(allocator))

  def apply(vector: FieldVector)(implicit allocator: BufferAllocator): RecordBatchLoader =
    new RecordBatchLoader(vector)
}
