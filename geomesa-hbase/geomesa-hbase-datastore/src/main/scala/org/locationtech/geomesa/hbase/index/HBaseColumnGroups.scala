/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.util.Bytes
import org.locationtech.geomesa.index.conf.ColumnGroups

object HBaseColumnGroups extends ColumnGroups[Array[Byte]] {

  override val default: Array[Byte] = Bytes.toBytes("d")

  override protected val reserved: Set[Array[Byte]] = Set.empty

  override protected def convert(group: String): Array[Byte] = Bytes.toBytes(group)

  override protected def convert(group: Array[Byte]): String = Bytes.toString(group)
}
