/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
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
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.tools.export.formats.{BinExporter, NullExporter, ShapefileExporter, _}
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.tools.utils.DataFormats._
import org.locationtech.geomesa.tools.{Command, DataStoreCommand}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timing}
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

    val fmt = DataFormats.values.find(_.toString.equalsIgnoreCase(params.outputFormat)).getOrElse {
      throw new ParameterException(s"Invalid format '${params.outputFormat}'. Valid values are " +
          DataFormats.values.filter(_ != Bin).map(_.toString.toLowerCase).mkString("'", "', '", "'"))
    }
    if (fmt == Bin) {
      throw new ParameterException(s"This operation has been deprecated. Use the command 'export-bin' instead.")
    }

    val attributes = getAttributes(ds, fmt, params)
    val features = getFeatureCollection(ds, fmt, attributes, params)

    lazy val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
    val exporter = fmt match {
      case Csv | Tsv      => new DelimitedExporter(getWriter(params), fmt, attributes, !params.noHeader)
      case Shp            => new ShapefileExporter(checkShpFile(params))
      case GeoJson | Json => new GeoJsonExporter(getWriter(params))
      case Gml            => new GmlExporter(createOutputStream(params.file, params.gzip))
      case Avro           => new AvroExporter(features.getSchema, createOutputStream(params.file, null), avroCompression)
      case Null           => NullExporter
      // shouldn't happen unless someone adds a new format and doesn't implement it here
      case _              => throw new UnsupportedOperationException(s"Format $fmt can't be exported")
    }

    try {
      val count = exporter.export(features)
      exporter.flush()
      count
    } finally {
      IOUtils.closeQuietly(exporter)
    }
  }
}

object ExportCommand extends LazyLogging {

  def getFeatureCollection(ds: GeoMesaDataStore[_, _, _],
                           fmt: DataFormat,
                           attributes: Option[ExportAttributes],
                           params: BaseExportParams): SimpleFeatureCollection = {

    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)

    logger.debug(s"Applying CQL filter ${ECQL.toCQL(filter)}")
    logger.debug(s"Applying transform ${attributes.map(_.names.mkString(",")).orNull}")

    val q = new Query(params.featureName, filter, attributes.map(_.names.toArray).orNull)
    Option(params.maxFeatures).map(Int.unbox).foreach(q.setMaxFeatures)
    params.loadIndex(ds, IndexMode.Read).foreach { index =>
      q.getHints.put(QueryHints.QUERY_INDEX, index)
      logger.debug(s"Using index ${index.identifier}")
    }

    // get the feature store used to query the GeoMesa data
    val fs = ds.getFeatureSource(params.featureName)

    try {
      fs.getFeatures(q)
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException("Could not execute export query. Please ensure " +
            "that all arguments are correct.", e)
    }
  }

  def getAttributes(ds: GeoMesaDataStore[_, _, _], fmt: DataFormat, params: BaseExportParams): Option[ExportAttributes] = {
    import scala.collection.JavaConversions._

    lazy val sft = ds.getSchema(params.featureName)
    val provided = Option(params.attributes).collect { case a if !a.isEmpty => a.toSeq }
    if (fmt == DataFormats.Shp) {
      val attributes = provided.map(ShapefileExporter.replaceGeom(sft, _)).getOrElse(ShapefileExporter.modifySchema(sft))
      Some(ExportAttributes(attributes, fid = true))
    } else if (fmt == DataFormats.Bin) {
      val attributes = provided.getOrElse {
        import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
        BinExporter.getAttributeList(params.asInstanceOf[BinExportParams], sft.getDtgField)
      }
      Some(ExportAttributes(attributes, fid = false))
    } else {
      provided.map { p =>
        val (id, attributes) = p.partition(_.equalsIgnoreCase("id"))
        ExportAttributes(attributes, id.nonEmpty)
      }
    }
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
