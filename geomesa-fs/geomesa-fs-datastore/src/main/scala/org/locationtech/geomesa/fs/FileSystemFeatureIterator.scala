/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import java.util.concurrent._

import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api.{FileSystemReader, FileSystemStorage, PartitionScheme}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class FileSystemFeatureIterator(sft: SimpleFeatureType,
                                storage: FileSystemStorage,
                                partitionScheme: PartitionScheme,
                                q: Query,
                                readThreads: Int) extends java.util.Iterator[SimpleFeature] with AutoCloseable {

  private val partitions = storage.getPartitions(sft.getTypeName, q)

  private val iter: FileSystemReader =
    if (partitions.isEmpty) {
      FileSystemFeatureIterator.EmptyReader
    } else {
      storage.getReader(sft.getTypeName, partitions, q, readThreads)
    }

  override def hasNext: Boolean = iter.hasNext
  override def next(): SimpleFeature = iter.next()
  override def close(): Unit = iter.close()
}

object FileSystemFeatureIterator {
  object EmptyReader extends FileSystemReader {
    override def next(): SimpleFeature = throw new NoSuchElementException
    override def hasNext: Boolean = false
    override def close(): Unit = {}
    override def close(wait: Long, unit: TimeUnit): Boolean = true
  }
}
