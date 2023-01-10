/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.TransformSimpleFeature
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureReadSupport
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.Transform.Transforms

import scala.annotation.tailrec
import scala.util.control.NonFatal

class ParquetPathReader(
    conf: Configuration,
    readSft: SimpleFeatureType,
    parquetFilter: FilterCompat.Filter,
    gtFilter: Option[org.geotools.api.filter.Filter],
    transform: Option[(String, SimpleFeatureType)]
  ) extends FileSystemPathReader with LazyLogging {

  private val gtf = gtFilter.orNull

  private val transformFeature: SimpleFeature => SimpleFeature = transform match {
    case None => null
    case Some((tdefs, tsft)) =>
      val definitions = Transforms(readSft, tdefs).toArray
      f => new TransformSimpleFeature(tsft, definitions, f)
  }

  override def read(path: Path): CloseableIterator[SimpleFeature] = {
    logger.debug(s"Opening reader for path $path")
    new ParquetFileIterator(path)
  }

  private class ParquetFileIterator(path: Path) extends CloseableIterator[SimpleFeature] {

    private val reader: ParquetReader[SimpleFeature] =
      ParquetReader.builder(new SimpleFeatureReadSupport, path).withFilter(parquetFilter).withConf(conf).build()

    private var staged: SimpleFeature = _

    override def close(): Unit = {
      logger.debug(s"Closing reader for path $path")
      reader.close()
    }

    override def next(): SimpleFeature = {
      val res = staged
      staged = null
      res
    }

    @tailrec
    override final def hasNext: Boolean = {
      if (staged != null) { true } else {
        val read = try { reader.read() } catch {
          case NonFatal(e) => logger.error(s"Error reading file '$path'", e); null
        }
        if (read == null) {
          false
        } else if (gtf == null || gtf.evaluate(read)) {
          staged = if (transformFeature == null) { read } else { transformFeature(read) }
          true
        } else {
          hasNext
        }
      }
    }
  }
}
