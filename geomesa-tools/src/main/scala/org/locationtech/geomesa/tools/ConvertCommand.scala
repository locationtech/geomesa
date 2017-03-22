/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools

import java.io.{FileInputStream, _}
import java.util.zip.Deflater

import com.beust.jcommander.{ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.convert.{SimpleFeatureConverter, SimpleFeatureConverters}
import org.locationtech.geomesa.tools.ConvertParameters.ConvertParameters
import org.locationtech.geomesa.tools.export.formats._
import org.locationtech.geomesa.tools.export.{OptionalBinExportParams, _}
import org.locationtech.geomesa.tools.utils.DataFormats._
import org.locationtech.geomesa.tools.utils.{CLArgResolver, DataFormats}
import org.locationtech.geomesa.utils.text.TextTools.getPlural
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class ConvertCommand extends Command with LazyLogging {

  override val name = "convert"
  override val params = new ConvertParameters

  override def execute(): Unit = {

    import ConvertCommand.{convertFile, getConverter, getExporter}

    import scala.collection.JavaConversions._

    val sft = CLArgResolver.getSft(params.spec)

    Command.user.info(s"Using SFT definition: $sft")

    val converter = getConverter(params, sft)
    val exporter = getExporter(params, sft)
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    val fc = new DefaultFeatureCollection(sft.getTypeName, sft)

    try {
      params.files.foreach { file =>
        convertFile(file, converter, params, fc)
      }
      Command.user.debug(s"Applying CQL filter ${filter.toString}")
      exporter.export(fc.subCollection(filter))
    } finally {
      IOUtils.closeQuietly(converter)
      IOUtils.closeQuietly(exporter)
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
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val outFmt = DataFormats.values.find(_.toString.equalsIgnoreCase(params.outputFormat)).getOrElse {
      throw new ParameterException("Unable to parse output file type.")
    }

    lazy val outputStream: OutputStream = ExportCommand.createOutputStream(params.file, params.gzip)
    val writer: Writer = ExportCommand.getWriter(params)

    outFmt match {
      case Csv | Tsv      => new DelimitedExporter(writer, outFmt, None, !params.noHeader)
      case Shp            =>
        Seq(ShapefileExporter.modifySchema(sft))
        new ShapefileExporter(ExportCommand.checkShpFile(params))
      case GeoJson | Json => new GeoJsonExporter(writer)
      case Gml            => new GmlExporter(outputStream)
      case Avro           =>
        val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
        new AvroExporter(sft, outputStream, avroCompression)
      case Bin            => BinExporter(outputStream, params.asInstanceOf[BinExportParams], sft.getDtgField)
      case _              => throw new ParameterException(s"Format $outFmt is not supported.")
    }
  }

  def convertFile(file: String,
                  converter: SimpleFeatureConverter[Any],
                  params: ConvertParameters,
                  fc: DefaultFeatureCollection) = {
    val ec = converter.createEvaluationContext(Map("inputFilePath" -> file))
    val dataIter = converter.process(new FileInputStream(file.toString), ec)
    if (params.maxFeatures != null && params.maxFeatures >= 0) {
      Command.user.info(s"Converting ${getPlural(params.maxFeatures.toLong, "feature")} from $file")
      for (i <- 1 to params.maxFeatures) {
        if (dataIter.hasNext) fc.add(dataIter.next())
      }
    } else {
      dataIter.foreach(fc.add)
    }
    val records = ec.counter.getLineCount - (if (params.noHeader) 0 else 1)
    Command.user.info(s"Converted ${getPlural(records, "simple feature")} "
      + s"with ${getPlural(ec.counter.getSuccess, "success", "successes")} "
      + s"and ${getPlural(ec.counter.getFailure, "failure")}")
  }
}

object ConvertParameters {
  @Parameters(commandDescription = "Convert files using GeoMesa's internal converter framework")
  class ConvertParameters extends FileExportParams
    with MaxFeaturesParam
    with InputFilesParam
    with OptionalTypeNameParam
    with OptionalBinExportParams
    with OptionalCqlFilterParam
    with RequiredFeatureSpecParam
    with RequiredConverterConfigParam
}
