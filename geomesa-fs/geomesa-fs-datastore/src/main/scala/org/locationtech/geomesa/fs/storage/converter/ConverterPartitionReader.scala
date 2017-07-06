/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import java.io.{File, FileInputStream}

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.convert.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.api.{FileSystemPartitionIterator, Partition}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class ConverterPartitionReader(root: Path,
                               partition: Partition,
                               sft: SimpleFeatureType,
                               converter: SimpleFeatureConverter[_],
                               gtFilter: org.opengis.filter.Filter) extends FileSystemPartitionIterator with LazyLogging {


  private val path = new Path(root, partition.getName)
  private val fis = new FileInputStream(new File(path.toUri.toString))
  private val iter = try {
    converter.process(fis)
  } catch {
    case e: Exception =>
      logger.error(s"Error processing uri ${path.toString}", e)
      Iterator.empty
  }

  private var cur: SimpleFeature = _

  private var nextStaged: Boolean = false
  private def stageNext() = {
    while (cur == null && iter.hasNext) {
      val possible = iter.next()
      if (gtFilter.evaluate(possible)) {
        cur = possible
      }
    }
    nextStaged = true
  }

  override def close(): Unit = {
    fis.close()
    converter.close()
  }

  override def next(): SimpleFeature = {
    if (!nextStaged) {
      stageNext()
    }

    if (cur == null) throw new NoSuchElementException

    val ret = cur
    cur = null
    nextStaged = false
    ret
  }

  override def hasNext: Boolean = {
    if (!nextStaged) {
      stageNext()
    }

    cur != null
  }

  override def getPartition: Partition = partition
}