/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.locationtech.geomesa.index.filters.Z3Filter
import org.locationtech.geomesa.index.index.z3.Z3IndexValues

class Z3Iterator extends RowFilterIterator[Z3Filter](Z3Filter)

object Z3Iterator {
  def configure(
      values: Z3IndexValues,
      offset: Int,
      priority: Int): IteratorSetting = {
    val is = new IteratorSetting(priority, "z3", classOf[Z3Iterator])
    is.addOption(RowFilterIterator.RowOffsetKey, offset.toString)
    // index space values for comparing in the iterator
    Z3Filter.serializeToStrings(Z3Filter(values)).foreach { case (k, v) => is.addOption(k, v) }
    is
  }
}