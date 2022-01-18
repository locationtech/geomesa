/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.locationtech.geomesa.index.filters.S3Filter
import org.locationtech.geomesa.index.index.s3.S3IndexValues

class S3Iterator extends RowFilterIterator[S3Filter](S3Filter)

object S3Iterator {
  def configure(values: S3IndexValues, prefix: Int, priority: Int): IteratorSetting = {
    val is = new IteratorSetting(priority, "s3", classOf[S3Iterator])
    // index space values for comparing in the iterator
    S3Filter.serializeToStrings(S3Filter(values)).foreach { case (k, v) => is.addOption(k, v) }
    // account for shard and table sharing bytes
    is.addOption(RowFilterIterator.RowOffsetKey, prefix.toString)
    is
  }
}
