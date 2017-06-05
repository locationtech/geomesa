/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.tools.export

import java.util.zip.Deflater

import com.beust.jcommander.{ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.arrow.data.ArrowDataStore
import org.locationtech.geomesa.arrow.tools.{ArrowDataStoreCommand, UrlParam}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.export.formats._
import org.locationtech.geomesa.tools.export.{DataExportParams, ExportCommand, FileExportParams}
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timing}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

class ArrowExportCommand extends ArrowDataStoreCommand with MethodProfiling with LazyLogging {

  import scala.collection.JavaConversions._

  override val name = "export"
  override val params = new ArrowExportParams

  override def execute(): Unit = {
    implicit val timing = new Timing
    val count = profile(withDataStore(export))
    Command.user.info(s"Feature export complete to ${Option(params.file).map(_.getPath).getOrElse("standard out")} " +
        s"in ${timing.time}ms${count.map(" for " + _ + " features").getOrElse("")}")
  }

  protected def export(ds: ArrowDataStore): Option[Long] = {
    import ExportCommand._
    import org.locationtech.geomesa.tools.utils.DataFormats._

    val fmt = DataFormats.values.find(_.toString.equalsIgnoreCase(params.outputFormat)).getOrElse {
      throw new ParameterException(s"Invalid format '${params.outputFormat}'. Valid values are " +
          DataFormats.values.filter(_ != Bin).map(_.toString.toLowerCase).mkString("'", "', '", "'"))
    }
    if (fmt == Bin) {
      throw new ParameterException(s"This operation has been deprecated. Use the command 'export-bin' instead.")
    }

    val attributes = Option(params.attributes).collect { case a if !a.isEmpty => a.toSeq }.map { p =>
      val (id, attributes) = p.partition(_.equalsIgnoreCase("id"))
      ExportAttributes(attributes, id.nonEmpty)
    }
    val features = {
      val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)

      logger.debug(s"Applying CQL filter ${ECQL.toCQL(filter)}")
      logger.debug(s"Applying transform ${attributes.map(_.names.mkString(",")).orNull}")

      val q = new Query(null, filter, attributes.map(_.names.toArray).orNull)
      Option(params.maxFeatures).map(Int.unbox).foreach(q.setMaxFeatures)

      // get the feature store used to query the GeoMesa data
      val fs = ds.getFeatureSource()

      try {
        fs.getFeatures(q)
      } catch {
        case NonFatal(e) =>
          throw new RuntimeException("Could not execute export query. Please ensure " +
              "that all arguments are correct.", e)
      }
    }

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
      exporter.export(features)
    } finally {
      CloseQuietly(exporter)
    }
  }
}

@Parameters(commandDescription = "Export features from a GeoMesa data store")
class ArrowExportParams extends UrlParam with FileExportParams with DataExportParams
