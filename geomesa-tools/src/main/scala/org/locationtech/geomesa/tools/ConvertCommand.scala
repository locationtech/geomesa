/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import java.io._
import java.util.zip.Deflater

import com.beust.jcommander.{ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.tools.ConvertParameters.ConvertParameters
import org.locationtech.geomesa.tools.export._
import org.locationtech.geomesa.tools.export.formats._
import org.locationtech.geomesa.tools.utils.CLArgResolver
import org.locationtech.geomesa.tools.utils.DataFormats._
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.StdInHandle
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.locationtech.geomesa.utils.text.TextTools.getPlural
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

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
    import ConvertCommand.{getConverter, getExporter}

    import scala.collection.JavaConversions._

    val files = if (params.files.nonEmpty) { params.files.iterator.flatMap(PathUtils.interpretPath) } else {
      StdInHandle.available().map(Iterator.single).getOrElse {
        throw new ParameterException("Missing option: <files>... is required")
      }
    }

    val sft = CLArgResolver.getSft(params.spec)

    Command.user.info(s"Using SFT definition: ${SimpleFeatureTypes.encodeType(sft)}")

    val converter = getConverter(params, sft)
    val filter = Option(params.cqlFilter)
    filter.foreach(f => Command.user.debug(s"Applying CQL filter $f"))
    val ec = converter.createEvaluationContext(EvaluationContext.inputFileParam(""))
    val maxFeatures = Option(params.maxFeatures).map(_.intValue())

    def features() = ConvertCommand.convertFeatures(files, converter, ec, filter, maxFeatures)

    val exporter = getExporter(params, features())

    try {
      exporter.start(sft)
      val count = WithClose(features())(exporter.export)
      val records = ec.counter.getLineCount - (if (params.noHeader) { 0 } else { params.files.size })
      Command.user.info(s"Converted ${getPlural(records, "line")} "
          + s"with ${getPlural(ec.counter.getSuccess, "success", "successes")} "
          + s"and ${getPlural(ec.counter.getFailure, "failure")}")
      count
    } finally {
      IOUtils.closeQuietly(exporter)
    }
  }
}

object ConvertCommand extends LazyLogging {

  def getConverter(params: ConvertParameters, sft: SimpleFeatureType): SimpleFeatureConverter = {
    val converterConfig = {
      if (params.config != null)
        CLArgResolver.getConfig(params.config)
      else throw new ParameterException("Unable to parse Simple Feature type from sft config or string")
    }
    SimpleFeatureConverter(sft, converterConfig)
  }

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
      case Csv | Tsv      => new DelimitedExporter(writer, params.outputFormat, None, !params.noHeader)
      case Shp            => new ShapefileExporter(ExportCommand.checkShpFile(params))
      case GeoJson | Json => new GeoJsonExporter(writer)
      case Gml | Xml      => new GmlExporter(outputStream)
      case Avro           => new AvroExporter(outputStream, avroCompression)
      case Bin            => new BinExporter(hints, outputStream)
      case Arrow          => new ArrowExporter(hints, outputStream, arrowDictionaries)
      case Leaflet        => new LeafletMapExporter(params)
      case _              => throw new ParameterException(s"Format ${params.outputFormat} is not supported.")
    }
  }

  def convertFeatures(files: Iterator[FileHandle],
                      converter: SimpleFeatureConverter,
                      ec: EvaluationContext,
                      filter: Option[Filter],
                      maxFeatures: Option[Int]): CloseableIterator[SimpleFeature] = {

    import EvaluationContext.RichEvaluationContext

    val all = CloseableIterator(files).flatMap { file =>
      ec.setInputFilePath(file.path)
      val is = PathUtils.handleCompression(file.open, file.path)
      converter.process(is, ec)
    }
    val filtered = filter.map(f => all.filter(f.evaluate)).getOrElse(all)
    val limited = maxFeatures.map(filtered.take).getOrElse(filtered)
    limited
  }
}

object ConvertParameters {
  @Parameters(commandDescription = "Convert files using GeoMesa's internal converter framework")
  class ConvertParameters extends FileExportParams with InputFilesParam with OptionalTypeNameParam
    with RequiredFeatureSpecParam with RequiredConverterConfigParam
}
