/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.locationtech.geomesa.index.filters.S2Filter
import org.locationtech.geomesa.index.index.s2.S2IndexValues

class S2Iterator extends RowFilterIterator[S2Filter](S2Filter)

object S2Iterator {
  def configure(values: S2IndexValues, prefix: Int, priority: Int): IteratorSetting = {
    val is = new IteratorSetting(priority, "s2", classOf[S2Iterator])
    // index space values for comparing in the iterator
    S2Filter.serializeToStrings(S2Filter(values)).foreach { case (k, v) => is.addOption(k, v) }
    // account for shard and table sharing bytes
    is.addOption(RowFilterIterator.RowOffsetKey, prefix.toString)
    is
  }
}
