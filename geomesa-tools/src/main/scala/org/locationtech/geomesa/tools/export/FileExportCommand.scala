/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io._
import java.util.zip.Deflater

import com.beust.jcommander.ParameterException
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataStore
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.export.formats.{BinExporter, NullExporter, ShapefileExporter, _}
import org.locationtech.geomesa.utils.io.CloseWithLogging

import scala.util.control.NonFatal

trait FileExportCommand[DS <: DataStore] extends ExportCommand[DS] {

  import org.locationtech.geomesa.tools.export.ExportCommand._

  override val name = "export"
  override def params: FileExportParams

  override def execute(): Unit = {
    profile(withDataStore(export)) { (count, time) =>
      Command.user.info(s"Feature export complete to ${Option(params.file).map(_.getPath).getOrElse("standard out")} " +
        s"in ${time}ms${count.map(" for " + _ + " features").getOrElse("")}")
    }
  }

  protected def export(ds: DS): Option[Long] = {
    import FileExportCommand._
    import org.locationtech.geomesa.tools.utils.DataFormats._

    val (query, attributes) = createQuery(getSchema(ds), Option(params.outputFormat), params)

    val features = try {
      getFeatures(ds, query)
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException("Could not execute export query. Please ensure " +
          "that all arguments are correct", e)
    }

    lazy val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
    val exporter = params.outputFormat match {
      case Csv | Tsv      => new DelimitedExporter(getWriter(params.file, params.gzip), params.outputFormat, attributes, !params.noHeader)
      case Shp            => new ShapefileExporter(checkShpFile(params))
      case GeoJson | Json => new GeoJsonExporter(getWriter(params.file, params.gzip))
      case Gml            => new GmlExporter(createOutputStream(params.file, params.gzip))
      case Avro           => new AvroExporter(createOutputStream(params.file, null), avroCompression)
      case Arrow          => new ArrowExporter(query.getHints, createOutputStream(params.file, params.gzip), ArrowExporter.queryDictionaries(ds, query))
      case Bin            => new BinExporter(query.getHints, createOutputStream(params.file, params.gzip))
      case Null           => NullExporter
      // Shouldn't happen unless someone adds a new format and doesn't implement it here
      case _              => throw new UnsupportedOperationException(s"Format ${params.outputFormat} can't be exported")
    }

    try {
      exporter.start(features.getSchema)
      export(exporter, features)
    } finally {
      CloseWithLogging(exporter)
    }
  }
}

object FileExportCommand extends LazyLogging {

  def checkShpFile(params: FilePropertyParams): File = {
    if (params.file != null) { params.file } else {
      throw new ParameterException("Error: -o or --output for file-based output is required for " +
          "shapefile export (stdout not supported for shape files)")
    }
  }
}
