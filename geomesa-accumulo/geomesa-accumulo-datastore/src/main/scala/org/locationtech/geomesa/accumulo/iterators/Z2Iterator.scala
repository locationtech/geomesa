/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.locationtech.geomesa.index.filters.Z2Filter
import org.locationtech.geomesa.index.index.z2.Z2IndexValues

class Z2Iterator extends RowFilterIterator[Z2Filter](Z2Filter)

object Z2Iterator {
  def configure(values: Z2IndexValues, offset: Int, priority: Int): IteratorSetting = {
    val is = new IteratorSetting(priority, "z2", classOf[Z2Iterator])
    // index space values for comparing in the iterator
    Z2Filter.serializeToStrings(Z2Filter(values)).foreach { case (k, v) => is.addOption(k, v) }
    // account for shard and table sharing bytes
    is.addOption(RowFilterIterator.RowOffsetKey, offset.toString)
    is
  }
}
