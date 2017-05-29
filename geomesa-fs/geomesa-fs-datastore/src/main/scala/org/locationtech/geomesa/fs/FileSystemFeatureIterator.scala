/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.fs

import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Created by afox on 5/29/17.
  */
class FileSystemFeatureIterator(sft: SimpleFeatureType, q: Query, storage: FileSystemStorage) extends java.util.Iterator[SimpleFeature] {
  // TODO: don't list partitions as there could be too many
  private val partitions = storage.listPartitions(sft.getTypeName)
  private val partitionCount = partitions.size()
  private var curIdx = 0
  private var cur = storage.getReader(q, partitions.get(curIdx)).filterFeatures(q)
  private var staged: SimpleFeature = _

  override def hasNext: Boolean = {
    staged = null
    while(curIdx < partitionCount && staged == null) {
      if(cur.hasNext) staged = cur.next()
      if(staged == null && !cur.hasNext) {
        curIdx += 1
        if(curIdx < partitionCount) {
          cur = storage.getReader(q, partitions.get(curIdx)).filterFeatures(q)
        }
      }
    }
    staged != null
  }

  override def next(): SimpleFeature = staged
}
