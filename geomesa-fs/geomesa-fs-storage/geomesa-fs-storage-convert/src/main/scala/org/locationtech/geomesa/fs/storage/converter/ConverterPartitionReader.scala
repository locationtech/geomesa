/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.fs.storage.api.FileSystemReader
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeature

import scala.util.control.NonFatal

class ConverterPartitionReader(storage: ConverterStorage,
                               partitions: Seq[String],
                               converter: SimpleFeatureConverter,
                               gtFilter: org.opengis.filter.Filter)
    extends FileSystemReader with LazyLogging {

  import scala.collection.JavaConverters._

  private val iter = {
    val files = CloseableIterator(partitions.flatMap(storage.getFilePaths(_).asScala).iterator)
    files.flatMap { file =>
      logger.debug(s"Opening file $file")
      val fis = storage.getMetadata.getFileContext.open(file)
      val iter = try { converter.process(fis) } catch {
        case NonFatal(e) => logger.error(s"Error processing uri ${file.toString}", e); Iterator.empty
      }
      CloseableIterator(iter.filter(gtFilter.evaluate), fis.close())
    }
  }

  override def hasNext: Boolean = iter.hasNext

  override def next(): SimpleFeature = iter.next

  override def close(): Unit = iter.close()

  override def close(wait: Long, unit: TimeUnit): Boolean = {
    close()
    true
  }
}
