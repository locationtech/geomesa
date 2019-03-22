/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.parquet.jobs.SimpleFeatureReadSupport
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

class FilteringReader(conf: Configuration,
                      sft: SimpleFeatureType,
                      parquetFilter: FilterCompat.Filter,
                      gtFilter: org.opengis.filter.Filter,
                      transform: Option[(String, SimpleFeatureType)]) extends FileSystemPathReader with LazyLogging {

  private val createFeature: SimpleFeature => SimpleFeature = transform match {
    case None => f => f
    case Some((_, tsft)) =>
      import scala.collection.JavaConversions._
      val transformIndices = tsft.getAttributeDescriptors.map(d => sft.indexOf(d.getLocalName)).toArray
      f => {
        val attributes = transformIndices.map(f.getAttribute)
        new ScalaSimpleFeature(tsft, f.getID, attributes, f.getUserData)
      }
  }

  override def read(path: Path): CloseableIterator[SimpleFeature] = new CloseableIterator[SimpleFeature] {
    // WARNING it is important to create a new conf per query
    // because we communicate the transform SFT set here
    // with the init() method on SimpleFeatureReadSupport via
    // the parquet api. Thus we need to deep copy conf objects
    // It may be possibly to move this high up the chain as well
    // TODO consider this with GEOMESA-1954 but we need to test it well
      private val support = new SimpleFeatureReadSupport
      private val queryConf = {
        val c = new Configuration(conf)
        StorageConfiguration.setSft(c, sft)
        c
      }

      private val builder = ParquetReader.builder[SimpleFeature](support, path)
          .withFilter(parquetFilter)
          .withConf(queryConf)

      private lazy val reader: ParquetReader[SimpleFeature] = {
        logger.debug(s"Opening reader for path $path")
        builder.build()
      }

      private var staged: SimpleFeature = _
      private var done: Boolean = false

      override def close(): Unit = {
        logger.debug(s"Closing parquet reader for path $path")
        reader.close()
      }

      override def next(): SimpleFeature = {
        val res = staged
        staged = null
        res
      }

      override def hasNext: Boolean = {
        while (staged == null && !done) {
          val f = try { reader.read() } catch {
            case NonFatal(e) => logger.error(s"Error reading file '$path'", e); null
          }
          if (f == null) {
            done = true
          } else if (gtFilter.evaluate(f)) {
            staged = createFeature(f)
          }
        }
        staged != null
      }
  }
}
