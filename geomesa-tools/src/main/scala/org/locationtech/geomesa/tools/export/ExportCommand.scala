/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io._
import java.util.zip.GZIPOutputStream

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.{DataStore, Query}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.tools.export.formats.{BinExporter, FeatureExporter, ShapefileExporter}
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.tools.utils.DataFormats.DataFormat
import org.locationtech.geomesa.tools.{DataStoreCommand, OptionalIndexParam, TypeNameParam}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

abstract class ExportCommand[DS <: DataStore] extends DataStoreCommand[DS] with MethodProfiling {

  protected def export(ds: DS): Option[Long]

  protected def export(exporter: FeatureExporter, collection: SimpleFeatureCollection): Option[Long] =
    WithClose(CloseableIterator(collection.features()))(exporter.export)

  protected def getSchema(ds: DS): SimpleFeatureType = params match {
    case p: TypeNameParam => ds.getSchema(p.featureName)
  }

  protected def getFeatures(ds: DS, query: Query): SimpleFeatureCollection =
    ds.getFeatureSource(query.getTypeName).getFeatures(query)
}

object ExportCommand extends LazyLogging {

  def createQuery(toSft: => SimpleFeatureType,
                  fmt: Option[DataFormat],
                  params: ExportQueryParams): (Query, Option[ExportAttributes]) = {
    val typeName = Option(params).collect { case p: TypeNameParam => p.featureName }.orNull
    val filter = Option(params.cqlFilter).getOrElse(Filter.INCLUDE)
    lazy val sft = toSft // only evaluate it once

    val query = new Query(typeName, filter)
    Option(params.maxFeatures).map(Int.unbox).foreach(query.setMaxFeatures)
    Option(params).collect { case p: OptionalIndexParam => p }.foreach { p =>
      Option(p.index).foreach { index =>
        logger.debug(s"Using index $index")
        query.getHints.put(QueryHints.QUERY_INDEX, index)
      }
    }

    fmt match {
      case Some(DataFormats.Arrow) =>
        query.getHints.put(QueryHints.ARROW_ENCODE, java.lang.Boolean.TRUE)
      case Some(DataFormats.Bin) =>
        // this indicates to run a BIN query, will be overridden by hints if specified
        query.getHints.put(QueryHints.BIN_TRACK, "id")
      case _ => // NoOp
    }

    Option(params.hints).foreach { hints =>
      query.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, hints)
      ViewParams.setHints(query)
    }

    val attributes = {
      import scala.collection.JavaConversions._
      val provided = Option(params.attributes).collect { case a if !a.isEmpty => a.toSeq }.orElse {
        fmt.find(_ == DataFormats.Bin) match {
          case Some(_) => Some(BinExporter.getAttributeList(sft, query.getHints))
          case None => None
        }
      }

      fmt.find(_ == DataFormats.Shp) match {
        case Some(_) =>
          val attributes = provided.map(ShapefileExporter.replaceGeom(sft, _)).getOrElse(ShapefileExporter.modifySchema(sft))
          Some(ExportAttributes(attributes, fid = true))
        case None =>
          provided.map { p =>
            val (id, attributes) = p.partition(_.equalsIgnoreCase("id"))
            ExportAttributes(attributes, id.nonEmpty)
          }
      }
    }

    query.setPropertyNames(attributes.map(_.names.toArray).orNull)

    logger.debug(s"Applying CQL filter ${ECQL.toCQL(filter)}")
    logger.debug(s"Applying transform ${Option(query.getPropertyNames).map(_.mkString(",")).orNull}")

    (query, attributes)
  }

  def createOutputStream(file: File, compress: Integer): OutputStream = {
    val out = Option(file).map(new FileOutputStream(_)).getOrElse(System.out)
    val compressed = if (compress == null) { out } else new GZIPOutputStream(out) {
      `def`.setLevel(compress) // hack to access the protected deflate level
    }
    new BufferedOutputStream(compressed)
  }

  def getWriter(file: File, compression: Integer): Writer = new OutputStreamWriter(createOutputStream(file, compression))

  case class ExportAttributes(names: Seq[String], fid: Boolean)
}