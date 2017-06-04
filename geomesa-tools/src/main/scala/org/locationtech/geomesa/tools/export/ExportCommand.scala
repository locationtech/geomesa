/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
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
import org.apache.commons.io.IOUtils
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.tools.export.formats.{BinExporter, NullExporter, ShapefileExporter, _}
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.tools.utils.DataFormats._
import org.locationtech.geomesa.tools.{Command, DataStoreCommand}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timing}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.util.control.NonFatal

trait ExportCommand[DS <: GeoMesaDataStore[_, _, _]] extends DataStoreCommand[DS] with MethodProfiling {

  override val name = "export"
  override def params: ExportParams

  override def execute(): Unit = {
    implicit val timing = new Timing
    val count = profile(withDataStore(export))
    Command.user.info(s"Feature export complete to ${Option(params.file).map(_.getPath).getOrElse("standard out")} " +
        s"in ${timing.time}ms${count.map(" for " + _ + " features").getOrElse("")}")
  }

  protected def export(ds: DS): Option[Long] = {
    import ExportCommand._
    import org.locationtech.geomesa.tools.utils.DataFormats._

    lazy val sft = ds.getSchema(params.featureName)
    val fmt = DataFormats.values.find(_.toString.equalsIgnoreCase(params.outputFormat)).getOrElse {
      throw new ParameterException(s"Invalid format '${params.outputFormat}'. Valid values are " +
          DataFormats.values.map(_.toString.toLowerCase).mkString("'", "', '", "'"))
    }

    val (query, attributes) = createQuery(ds, sft, fmt, params)
    val features = try {
      ds.getFeatureSource(query.getTypeName).getFeatures(query)
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException("Could not execute export query. Please ensure " +
            "that all arguments are correct.", e)
    }

    lazy val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
    val exporter = fmt match {
      case Csv | Tsv      => new DelimitedExporter(getWriter(params), fmt, attributes, !params.noHeader)
      case Shp            => new ShapefileExporter(checkShpFile(params))
      case GeoJson | Json => new GeoJsonExporter(getWriter(params))
      case Gml            => new GmlExporter(createOutputStream(params.file, params.gzip))
      case Avro           => new AvroExporter(features.getSchema, createOutputStream(params.file, null), avroCompression)
      case Arrow          => new ArrowExporter(query.getHints, createOutputStream(params.file, null), ArrowExporter.queryDictionaries(ds, query))
      case Bin            => new BinExporter(query.getHints, createOutputStream(params.file, null))
      case Null           => NullExporter
      // shouldn't happen unless someone adds a new format and doesn't implement it here
      case _              => throw new UnsupportedOperationException(s"Format $fmt can't be exported")
    }

    try {
      exporter.export(features)
    } finally {
      IOUtils.closeQuietly(exporter)
    }
  }
}

object ExportCommand extends LazyLogging {

  def createQuery(ds: GeoMesaDataStore[_, _, _],
                  sft: => SimpleFeatureType,
                  fmt: DataFormat,
                  params: ExportParams): (Query, Option[ExportAttributes]) = {
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)

    val query = new Query(params.featureName, filter)
    Option(params.maxFeatures).map(Int.unbox).foreach(query.setMaxFeatures)
    params.loadIndex(ds, IndexMode.Read).foreach { index =>
      query.getHints.put(QueryHints.QUERY_INDEX, index)
      logger.debug(s"Using index ${index.identifier}")
    }

    if (fmt == DataFormats.Arrow) {
      query.getHints.put(QueryHints.ARROW_ENCODE, java.lang.Boolean.TRUE)
    } else if (fmt == DataFormats.Bin) {
      // this indicates to run a BIN query, will be overridden by hints if specified
      query.getHints.put(QueryHints.BIN_TRACK, "id")
    }

    Option(params.hints).foreach { hints =>
      query.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, hints)
      ViewParams.setHints(sft, query, ds)
    }

    val attributes = {
      import scala.collection.JavaConversions._
      val provided = Option(params.attributes).collect { case a if !a.isEmpty => a.toSeq }.orElse {
        if (fmt == DataFormats.Bin) { Some(BinExporter.getAttributeList(sft, query.getHints)) } else { None }
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

  def checkShpFile(params: FileExportParams): File = {
    if (params.file != null) { params.file } else {
      throw new ParameterException("Error: -o or --output for file-based output is required for " +
          "shapefile export (stdout not supported for shape files)")
    }
  }

  case class ExportAttributes(names: Seq[String], fid: Boolean)
}
