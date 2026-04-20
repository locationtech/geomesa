/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.{Path, PathFilter}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.fs.storage.converter.pathfilter.PathFiltering
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.fs.storage.core.fs.ObjectStore
import org.locationtech.geomesa.utils.collection.CloseableIterator

import java.net.URI
import scala.util.control.NonFatal

class ConverterFileSystemReader(
    fs: ObjectStore,
    val root: URI,
    converter: SimpleFeatureConverter,
    filter: Option[Filter],
    transform: Option[(String, SimpleFeatureType)],
    pathFiltering: Option[PathFiltering]
  ) extends FileSystemPathReader with StrictLogging {

  private lazy val pathFilter: Option[PathFilter] = pathFiltering.flatMap(pf => filter.map(pf.apply))

  override def read(file: URI): CloseableIterator[SimpleFeature] = {
    if (pathFilter.forall(_.accept(new Path(file)))) {
      logger.debug(s"Opening file $file")
      val iter = try {
        val streams = fs.format(file) match {
          case None => CloseableIterator.wrap(fs.read(file).map(fs.toString -> _).toIterator)
          case Some(f) => fs.read(file, f).map(a => (a.name, a.is))
        }
        streams.flatMap { case (name, is) =>
          val params = EvaluationContext.inputFileParam(name) ++ filter.map(EvaluationContext.FilterKey -> _)
          converter.process(is, converter.createEvaluationContext(params))
        }
      } catch {
        case NonFatal(e) => logger.error(s"Error processing uri '$file'", e); CloseableIterator.empty
      }
      transformed(filtered(iter))
    } else {
      CloseableIterator.empty
    }
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
