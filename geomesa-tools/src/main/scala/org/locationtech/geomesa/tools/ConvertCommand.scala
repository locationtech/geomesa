/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools

import java.io.{FileInputStream, _}
import java.util.zip.Deflater

import com.beust.jcommander.{ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.geotools.data.Query
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.convert.{SimpleFeatureConverter, SimpleFeatureConverters, Transformers}
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.tools.ConvertParameters.ConvertParameters
import org.locationtech.geomesa.tools.export._
import org.locationtech.geomesa.tools.export.formats._
import org.locationtech.geomesa.tools.utils.DataFormats._
import org.locationtech.geomesa.tools.utils.{CLArgResolver, DataFormats}
import org.locationtech.geomesa.utils.text.TextTools.getPlural
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class ConvertCommand extends Command with LazyLogging {

  override val name = "convert"
  override val params = new ConvertParameters

  override def execute(): Unit = {

    import ConvertCommand.{getConverter, getExporter, loadFeatureCollection}

    import scala.collection.JavaConversions._

    val sft = CLArgResolver.getSft(params.spec)

    Command.user.info(s"Using SFT definition: $sft")

    val converter = getConverter(params, sft)
    val exporter = getExporter(params, sft)
    val filter = Option(params.cqlFilter).map(ECQL.toFilter)
    val fc = new ListFeatureCollection(sft)

    Option(params.cqlFilter).foreach(f => Command.user.debug(s"Applying CQL filter $f"))

    val ec = converter.createEvaluationContext(Map("inputFilePath" -> ""))
    try {
      loadFeatureCollection(fc, params.files, converter, ec, filter, Option(params.maxFeatures).map(_.intValue))
      exporter.export(fc)
      val records = ec.counter.getLineCount - (if (params.noHeader) { 0 } else { params.files.size })
      Command.user.info(s"Converted ${getPlural(records, "line")} "
          + s"with ${getPlural(ec.counter.getSuccess, "success", "successes")} "
          + s"and ${getPlural(ec.counter.getFailure, "failure")}")
    } finally {
      IOUtils.closeQuietly(exporter)
      IOUtils.closeQuietly(converter)
    }
  }
}

object ConvertCommand extends LazyLogging {

  def getConverter(params: ConvertParameters, sft: SimpleFeatureType): SimpleFeatureConverter[Any] = {
    val converterConfig = {
      if (params.config != null)
        CLArgResolver.getConfig(params.config)
      else throw new ParameterException("Unable to parse Simple Feature type from sft config or string")
    }
    SimpleFeatureConverters.build(sft, converterConfig)
  }

  def getExporter(params: ConvertParameters, sft: SimpleFeatureType): FeatureExporter = {
    val outFmt = DataFormats.values.find(_.toString.equalsIgnoreCase(params.outputFormat)).getOrElse {
      throw new ParameterException("Unable to parse output file type.")
    }

    lazy val outputStream: OutputStream = ExportCommand.createOutputStream(params.file, params.gzip)
    lazy val writer: Writer = ExportCommand.getWriter(params)
    lazy val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
    lazy val hints = {
      val q = new Query("")
      Option(params.hints).foreach { hints =>
        q.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, hints)
        ViewParams.setHints(sft, q, null)
      }
      q.getHints
    }

    outFmt match {
      case Csv | Tsv      => new DelimitedExporter(writer, outFmt, None, !params.noHeader)
      case Shp            => new ShapefileExporter(ExportCommand.checkShpFile(params))
      case GeoJson | Json => new GeoJsonExporter(writer)
      case Gml            => new GmlExporter(outputStream)
      case Avro           => new AvroExporter(sft, outputStream, avroCompression)
      case Bin            => new BinExporter(hints, outputStream)
      case _              => throw new ParameterException(s"Format $outFmt is not supported.")
    }
  }

  def loadFeatureCollection(fc: java.util.Collection[SimpleFeature],
                            files: Iterable[String],
                            converter: SimpleFeatureConverter[Any],
                            ec: Transformers.EvaluationContext,
                            filter: Option[Filter],
                            maxFeatures: Option[Int]): Unit = {
    val features = files.iterator.flatMap { file =>
      ec.set(ec.indexOf("inputFilePath"), file)
      converter.process(new FileInputStream(file.toString), ec)
    }
    val filtered = filter.map(f => features.filter(f.evaluate)).getOrElse(features)
    val limited = maxFeatures.map(filtered.take).getOrElse(filtered)
    limited.foreach(fc.add)
  }
}

object ConvertParameters {
  @Parameters(commandDescription = "Convert files using GeoMesa's internal converter framework")
  class ConvertParameters extends FileExportParams with InputFilesParam with OptionalTypeNameParam
    with RequiredFeatureSpecParam with RequiredConverterConfigParam
}
