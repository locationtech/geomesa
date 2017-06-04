/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io.records

import java.io.ByteArrayInputStream
import java.nio.channels.Channels

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.file.ReadChannel
import org.apache.arrow.vector.schema.ArrowRecordBatch
import org.apache.arrow.vector.stream.MessageSerializer
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.locationtech.geomesa.utils.io.WithClose

class RecordBatchLoader(field: Field)(implicit allocator: BufferAllocator) {
  import scala.collection.JavaConversions._

  val vector = field.createVector(allocator)
  private val root = new VectorSchemaRoot(Seq(field), Seq(vector), 0)
  private val loader = new VectorLoader(root)

  def load(bytes: Array[Byte]): Unit = {
    WithClose(new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes)))) { in =>
      WithClose(MessageSerializer.deserializeMessageBatch(in, allocator).asInstanceOf[ArrowRecordBatch]) { recordBatch =>
        loader.load(recordBatch)
      }
    }
  }
}
