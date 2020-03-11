/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.rpc.filter

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.filter.Filter.ReturnCode
import org.apache.hadoop.hbase.filter.FilterBase

class LastReadFilter extends FilterBase {
  var lastRead: Array[Byte] = _
  var count = 0L
  override def filterKeyValue(v: Cell): ReturnCode = {
    lastRead = v.getRowArray
    count += 1
    super.filterKeyValue(v)
  }
}
