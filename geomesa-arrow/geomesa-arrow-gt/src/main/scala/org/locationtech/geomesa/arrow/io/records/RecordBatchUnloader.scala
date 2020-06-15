/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io.records

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.util.Collections

import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.{IpcOption, MessageSerializer}
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot, VectorUnloader}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector
import org.locationtech.geomesa.utils.io.WithClose

class RecordBatchUnloader(vector: SimpleFeatureVector, ipcOpts: IpcOption) {

  private val root = {
    val fields = Collections.singletonList(vector.underlying.getField)
    new VectorSchemaRoot(fields, Collections.singletonList[FieldVector](vector.underlying), 0)
  }
  private val unloader = new VectorUnloader(root)
  private val os = new ByteArrayOutputStream()

  def unload(count: Int): Array[Byte] = {
    os.reset()
    vector.writer.setValueCount(count)
    root.setRowCount(count)
    WithClose(unloader.getRecordBatch) { batch =>
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(os)), batch, ipcOpts)
    }
    os.toByteArray
  }
}
