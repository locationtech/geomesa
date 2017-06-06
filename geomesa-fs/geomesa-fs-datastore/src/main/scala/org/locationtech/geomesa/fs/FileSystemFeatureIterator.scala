/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs

import org.apache.hadoop.fs.FileSystem
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Created by afox on 5/29/17.
  */
class FileSystemFeatureIterator(fs: FileSystem,
                                partitionScheme: PartitionScheme,
                                sft: SimpleFeatureType,
                                q: Query,
                                storage: FileSystemStorage) extends java.util.Iterator[SimpleFeature] {
  // TODO: don't list partitions as there could be too many
  import scala.collection.JavaConversions._
  private val partitions: java.util.List[String] = {
      // Get the partitions from the partition scheme
    // if the result is empty, then scan all partitions
    // TODO: can we short-circuit if the query is outside the bounds
    val res = partitionScheme.coveringPartitions(q.getFilter).toList
    if(res.isEmpty) storage.listPartitions(sft.getTypeName)
    else res
  }



  private val internal = partitions.flatMap { i => storage.getReader(q, i) }.iterator

  override def hasNext: Boolean = internal.hasNext

  override def next(): SimpleFeature = internal.next
}
