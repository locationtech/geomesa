/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.parquet.hadoop.ParquetReader
import org.locationtech.geomesa.fs.storage.api.FileSystemPartitionIterator
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.SimpleFeature

class FilteringIterator(partition: String,
                        builder: ParquetReader.Builder[SimpleFeature],
                        gtFilter: org.opengis.filter.Filter) extends FileSystemPartitionIterator with LazyLogging {


  private lazy val reader: ParquetReader[SimpleFeature] = {
    logger.info(s"Opening reader for partition $partition")
    builder.build()
  }

  private var staged: SimpleFeature = _
  private var done: Boolean = false

  override def close(): Unit = {
    logger.debug(s"Closing parquet reader for partition $partition")
    reader.close()
  }

  override def next(): SimpleFeature = {
    val res = staged
    staged = null
    res
  }

  override def hasNext: Boolean = {
    while (staged == null && !done) {
      val f = reader.read()
      if (f == null) {
        done = true
      } else if (gtFilter.evaluate(f)) {
        staged = f
      }
    }
    staged != null
  }

  override def getPartition: String = partition
}


class MultiIterator(partition: String,
                    itrs: Iterator[FileSystemPartitionIterator])
  extends FileSystemPartitionIterator with LazyLogging {

  private var cur: FileSystemPartitionIterator = _

  override def getPartition: String = partition

  override def close(): Unit = {
    if (cur != null) {
      CloseQuietly(cur)
    }
  }

  override def next(): SimpleFeature = {
    hasNext()
    cur.next()
  }

  private def loadNext() = {
    if (cur != null) {
      CloseQuietly(cur)
    }
    if (itrs.hasNext) {
      cur = itrs.next()
    } else {
      cur = null
    }
  }

  override def hasNext: Boolean = {
    if (cur == null || !cur.hasNext) {
      loadNext()
    }
    cur != null && cur.hasNext
  }

}

class EmptyFsIterator(partition: String) extends FileSystemPartitionIterator {
  override def close(): Unit = {}
  override def next(): SimpleFeature = throw new NoSuchElementException
  override def hasNext: Boolean = false
  override def getPartition: String = partition
}