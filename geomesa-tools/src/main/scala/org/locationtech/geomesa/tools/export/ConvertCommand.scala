/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io._
import java.util.zip.Deflater

import com.beust.jcommander.{ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.tools.export.ConvertCommand.ConvertParameters
import org.locationtech.geomesa.tools.export.formats._
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.utils.DataFormats._
import org.locationtech.geomesa.tools.{OptionalConverterConfigParam, OptionalFeatureSpecParam, OptionalInputFormatParam, OptionalTypeNameParam, _}
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.StdInHandle
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.locationtech.geomesa.utils.text.TextTools.getPlural
import org.opengis.feature.simple.SimpleFeature

class ConvertCommand extends Command with MethodProfiling with LazyLogging {

  override val name = "convert"
  override val params = new ConvertParameters

  override def execute(): Unit = {
    def complete(count: Option[Long], time: Long): Unit =
      Command.user.info(s"Conversion complete to ${Option(params.file).map(_.getPath).getOrElse("standard out")} " +
          s"in ${time}ms${count.map(c => s" for $c features").getOrElse("")}")

    profile(complete _)(convertAndExport())
  }

  private def convertAndExport(): Option[Long] = {
    import EvaluationContext.RichEvaluationContext

    import scala.collection.JavaConverters._

    if (params.files.isEmpty && !StdInHandle.isAvailable) {
      throw new ParameterException("Missing option: <files>... is required")
    }

    val inputs = params.files.asScala
    val format = IngestCommand.getDataFormat(params, inputs)

    // use .get to re-throw the exception if we fail
    IngestCommand.getSftAndConverter(params, inputs, format, None).get.flatMap { case (sft, config) =>
      WithClose(SimpleFeatureConverter(sft, config)) { converter =>
        val filter = Option(params.cqlFilter)
        filter.foreach(f => Command.user.debug(s"Applying CQL filter $f"))

        val ec = converter.createEvaluationContext(EvaluationContext.inputFileParam(""))
        val maxFeatures = Option(params.maxFeatures).map(_.intValue())

        def features(): CloseableIterator[SimpleFeature] = {
          val files = if (inputs.isEmpty) { StdInHandle.available().iterator } else {
            inputs.iterator.flatMap(PathUtils.interpretPath)
          }
          var iter = CloseableIterator(files).flatMap { file =>
            ec.setInputFilePath(file.path)
            converter.process(PathUtils.handleCompression(file.open, file.path), ec)
          }
          filter.foreach { f =>
            iter = iter.filter(f.evaluate)
          }
          maxFeatures.foreach { m =>
            iter = iter.take(m)
          }
          iter
        }

        WithClose(ConvertCommand.getExporter(params, features())) { exporter =>
          exporter.start(sft)
          val count = WithClose(features())(exporter.export)
          val records = ec.counter.getLineCount - (if (params.noHeader) { 0 } else { params.files.size })
          Command.user.info(s"Converted ${getPlural(records, "line")} "
              + s"with ${getPlural(ec.counter.getSuccess, "success", "successes")} "
              + s"and ${getPlural(ec.counter.getFailure, "failure")}")
          count
        }
      }
    }
  }
}

object ConvertCommand extends LazyLogging {

  @Parameters(commandDescription = "Convert files using GeoMesa's internal converter framework")
  class ConvertParameters extends FileExportParams with OptionalTypeNameParam with OptionalFeatureSpecParam
      with OptionalConverterConfigParam with OptionalInputFormatParam with OptionalForceParam

  def getExporter(params: ConvertParameters, features: => CloseableIterator[SimpleFeature]): FeatureExporter = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    lazy val outputStream: OutputStream = ExportCommand.createOutputStream(params.file, params.gzip)
    lazy val writer: Writer = ExportCommand.getWriter(params)
    lazy val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
    lazy val hints = {
      val q = new Query("")
      Option(params.hints).foreach { hints =>
        q.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, hints)
        ViewParams.setHints(q)
      }
      q.getHints
    }
    lazy val arrowDictionaries: Map[String, Array[AnyRef]] = {
      val attributes = hints.getArrowDictionaryFields
      if (attributes.isEmpty) { Map.empty } else {
        val values = attributes.map(a => a -> scala.collection.mutable.HashSet.empty[AnyRef])
        SelfClosingIterator(features).foreach(f => values.foreach { case (a, v) => v.add(f.getAttribute(a))})
        values.map { case (attribute, value) => attribute -> value.toArray }.toMap
      }
    }

    params.outputFormat match {
      case Csv | Tsv => new DelimitedExporter(writer, params.outputFormat, None, !params.noHeader)
      case Shp       => new ShapefileExporter(ExportCommand.checkShpFile(params))
      case Json      => new GeoJsonExporter(writer)
      case Gml | Xml => new GmlExporter(outputStream)
      case Avro      => new AvroExporter(outputStream, avroCompression)
      case Arrow     => new ArrowExporter(hints, outputStream, arrowDictionaries)
      case Bin       => new BinExporter(hints, outputStream)
      case Leaflet   => new LeafletMapExporter(params)
      case Null      => NullExporter
      // shouldn't happen unless someone adds a new format and doesn't implement it here
      case _         => throw new UnsupportedOperationException(s"Format ${params.outputFormat} can't be exported")
    }
  }
}
