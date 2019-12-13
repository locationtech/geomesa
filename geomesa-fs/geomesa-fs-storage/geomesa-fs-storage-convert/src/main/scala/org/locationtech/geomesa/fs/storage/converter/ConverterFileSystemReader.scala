/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.{FileContext, Path}
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

class ConverterFileSystemReader(
    fc: FileContext,
    converter: SimpleFeatureConverter,
    filter: Option[Filter],
    transform: Option[(String, SimpleFeatureType)]
  ) extends FileSystemPathReader with StrictLogging {

  override def read(path: Path): CloseableIterator[SimpleFeature] = {
    logger.debug(s"Opening file $path")
    val iter = try { converter.process(fc.open(path)) } catch {
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
