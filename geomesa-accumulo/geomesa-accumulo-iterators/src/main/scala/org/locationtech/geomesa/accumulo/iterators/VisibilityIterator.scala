/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope
import org.apache.accumulo.core.iterators.user.ReqVisFilter

object VisibilityIterator {

  val Name = "ReqVisFilter"
  val Priority = 15 // run before the accumulo versioning iterator at 20, and before any of our custom iterators

  def set(tableOps: TableOperations, table: String): Unit =
    tableOps.attachIterator(table, new IteratorSetting(Priority, Name, classOf[ReqVisFilter]))

  def clear(tableOps: TableOperations, table: String): Unit = {
    if (IteratorScope.values.exists(scope => tableOps.getIteratorSetting(table, Name, scope) != null)) {
      tableOps.removeIterator(table, Name, java.util.EnumSet.allOf(classOf[IteratorScope]))
    }
  }
}
