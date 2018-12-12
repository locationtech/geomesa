/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io._
import java.util.zip.{Deflater, GZIPOutputStream}

import com.beust.jcommander.ParameterException
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.{DataStore, Query}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.iterators.BinAggregatingScan
import org.locationtech.geomesa.tools.export.formats.{BinExporter, NullExporter, ShapefileExporter, _}
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.tools.utils.DataFormats._
import org.locationtech.geomesa.tools.{Command, DataStoreCommand, OptionalIndexParam, TypeNameParam}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.util.control.NonFatal

trait ExportCommand[DS <: DataStore] extends DataStoreCommand[DS] with MethodProfiling {

  override val name = "export"
  override def params: ExportParams

  override def execute(): Unit = {
    def complete(count: Option[Long], time: Long): Unit =
      Command.user.info(s"Feature export complete to ${Option(params.file).map(_.getPath).getOrElse("standard out")} " +
          s"in ${time}ms${count.map(" for " + _ + " features").getOrElse("")}")

    profile(complete _)(withDataStore(export))
  }

  protected def export(ds: DS): Option[Long] = {
    import ExportCommand._
    import org.locationtech.geomesa.tools.utils.DataFormats._

    val (query, attributes) = createQuery(getSchema(ds), params.outputFormat, params)

    val features = try { getFeatures(ds, query) } catch {
      case NonFatal(e) =>
        throw new RuntimeException("Could not execute export query. Please ensure " +
            "that all arguments are correct", e)
    }

    lazy val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
    val exporter = params.outputFormat match {
      case Csv | Tsv => new DelimitedExporter(getWriter(params), params.outputFormat, attributes, !params.noHeader)
      case Shp       => new ShapefileExporter(checkShpFile(params))
      case Json      => new GeoJsonExporter(getWriter(params))
      case Gml | Xml => new GmlExporter(createOutputStream(params.file, params.gzip))
      case Avro      => new AvroExporter(createOutputStream(params.file, null), avroCompression)
      case Arrow     => new ArrowExporter(query.getHints, createOutputStream(params.file, params.gzip), ArrowExporter.queryDictionaries(ds, query))
      case Bin       => new BinExporter(query.getHints, createOutputStream(params.file, params.gzip))
      case Leaflet   => new LeafletMapExporter(params)
      case Null      => NullExporter
      // shouldn't happen unless someone adds a new format and doesn't implement it here
      case _         => throw new UnsupportedOperationException(s"Format ${params.outputFormat} can't be exported")
    }

    try {
      exporter.start(features.getSchema)
      export(exporter, features)
    } finally {
      CloseWithLogging(exporter)
    }
  }

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
                  fmt: DataFormat,
                  params: ExportParams): (Query, Option[ExportAttributes]) = {
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

    Option(params.hints).foreach { hints =>
      query.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, hints)
      ViewParams.setHints(query)
    }

    if (fmt == DataFormats.Arrow) {
      query.getHints.put(QueryHints.ARROW_ENCODE, java.lang.Boolean.TRUE)
    } else if (fmt == DataFormats.Bin) {
      // if not specified in hints, set it here to trigger the bin query
      if (!query.getHints.containsKey(QueryHints.BIN_TRACK)) {
        query.getHints.put(QueryHints.BIN_TRACK, "id")
      }
    }

    val attributes = {
      import scala.collection.JavaConversions._
      val provided = Option(params.attributes).collect { case a if !a.isEmpty => a.toSeq }.orElse {
        if (fmt == DataFormats.Bin) { Some(BinAggregatingScan.propertyNames(query.getHints, sft)) } else { None }
      }
      if (fmt == DataFormats.Shp) {
        val attributes = provided.map(ShapefileExporter.replaceGeom(sft, _)).getOrElse(ShapefileExporter.modifySchema(sft))
        Some(ExportAttributes(attributes, fid = true))
      } else {
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

  def getWriter(params: FileExportParams): Writer = new OutputStreamWriter(createOutputStream(params.file, params.gzip))

  def getWriter(file: File, compress: Integer): Writer = new OutputStreamWriter(createOutputStream(file, compress))

  def checkShpFile(params: FileExportParams): File = {
    if (params.file != null) { params.file } else {
      throw new ParameterException("Error: -o or --output for file-based output is required for " +
          "shapefile export (stdout not supported for shape files)")
    }
  }

  case class ExportAttributes(names: Seq[String], fid: Boolean)
}
