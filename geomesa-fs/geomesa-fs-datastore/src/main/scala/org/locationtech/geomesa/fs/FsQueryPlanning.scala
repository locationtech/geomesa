/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api.{FileSystemStorage, Partition}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

object FsQueryPlanning {

  def getPartitionsForQuery(storage: FileSystemStorage,
                            sft: SimpleFeatureType,
                            q: Query): Seq[Partition] = {
    import scala.collection.JavaConversions._

    // Get the partitions from the partition scheme
    // if the result is empty, then scan all partitions
    // TODO: can we short-circuit if the query is outside the bounds
    val storagePartitions = storage.listPartitions(sft.getTypeName)
    if (q.getFilter == Filter.INCLUDE) {
      storagePartitions
    }
    else {
      val partitionScheme = storage.getPartitionScheme(sft.getTypeName)
      val coveringPartitions = partitionScheme.getCoveringPartitions(q.getFilter).map(storage.getPartition)
      if (coveringPartitions.isEmpty) {
        storagePartitions //TODO should this ever happen?
      } else {
        coveringPartitions.intersect(storagePartitions)
      }
    }
  }
}
