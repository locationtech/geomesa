/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.hadoop.HadoopDelegate.{HadoopFileHandle, HadoopTarHandle, HadoopZipHandle}
import org.locationtech.geomesa.utils.io.PathUtils

import java.util.Locale
import scala.util.control.NonFatal

class ConverterFileSystemReader(
    fs: FileSystem,
    converter: SimpleFeatureConverter,
    filter: Option[Filter],
    transform: Option[(String, SimpleFeatureType)]
  ) extends FileSystemPathReader with StrictLogging {

  import ArchiveStreamFactory.{JAR, TAR, ZIP}

  override def read(path: Path): CloseableIterator[SimpleFeature] = {
    logger.debug(s"Opening file $path")
    val iter = try {
      val handle = PathUtils.getUncompressedExtension(path.getName).toLowerCase(Locale.US) match {
        case TAR       => new HadoopTarHandle(fs, path)
        case ZIP | JAR => new HadoopZipHandle(fs, path)
        case _         => new HadoopFileHandle(fs, path)
      }
      handle.open.flatMap { case (name, is) =>
        val params = EvaluationContext.inputFileParam(name.getOrElse(handle.path))
        converter.process(is, converter.createEvaluationContext(params))
      }
    } catch {
      case NonFatal(e) => logger.error(s"Error processing uri '$path'", e); CloseableIterator.empty
    }
    transformed(filtered(iter))
  }

  private def filtered(in: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
    filter match {
      case None => in
      case Some(f) => in.filter(f.evaluate)
    }
  }

  private def transformed(in: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
    transform match {
      case None => in
      case Some((tdefs, tsft)) =>
        val feature = TransformSimpleFeature(converter.targetSft, tsft, tdefs)
        in.map(f => ScalaSimpleFeature.copy(feature.setFeature(f)))
    }
  }
}
