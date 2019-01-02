/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.apache.kudu.client.{AlterTableOptions, PartialRow}
import org.locationtech.geomesa.kudu.schema.KuduColumnAdapter
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

package object kudu {

  val KuduSplitterOptions = "geomesa.kudu.splitters"

  case class KuduValue[T](value: T, adapter: KuduColumnAdapter[T]) {
    def writeToRow(row: PartialRow): Unit = adapter.writeToRow(row, value)
  }

  case class Partitioning(table: String, alteration: AlterTableOptions)

  object KuduSystemProperties {
    val AdminOperationTimeout = SystemProperty("geomesa.kudu.admin.timeout")
    val OperationTimeout      = SystemProperty("geomesa.kudu.operation.timeout")
    val SocketReadTimeout     = SystemProperty("geomesa.kudu.socket.timeout")
    val BlockSize             = SystemProperty("geomesa.kudu.block.size")
    val Compression           = SystemProperty("geomesa.kudu.compression", "lz4")
    val MutationBufferSpace   = SystemProperty("geomesa.kudu.mutation.buffer", "10000")
  }
}
