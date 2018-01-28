/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import java.io.{File, FileInputStream}
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.convert.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.api.FileSystemReader
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

class ConverterPartitionReader(root: Path,
                               partitions: Seq[String],
                               sft: SimpleFeatureType,
                               converter: SimpleFeatureConverter[_],
                               gtFilter: org.opengis.filter.Filter)
    extends FileSystemReader with LazyLogging {

  private val iter = CloseableIterator(partitions.iterator).flatMap { partition =>
    val path = new Path(root, partition)
    val fis = new FileInputStream(new File(path.toUri.toString))
    val iter = try { converter.process(fis) } catch {
      case NonFatal(e) => logger.error(s"Error processing uri ${path.toString}", e); Iterator.empty
    }
    CloseableIterator(iter.filter(gtFilter.evaluate), fis.close())
  }

  override def hasNext: Boolean = iter.hasNext

  override def next(): SimpleFeature = iter.next

  override def close(): Unit = {
    CloseWithLogging(iter)
    CloseWithLogging(converter)
  }

  override def close(wait: Long, unit: TimeUnit): Boolean = {
    close()
    true
  }
}
