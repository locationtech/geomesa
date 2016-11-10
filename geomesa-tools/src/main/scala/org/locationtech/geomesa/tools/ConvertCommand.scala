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
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.tools.ConvertParameters.ConvertParameters
import org.locationtech.geomesa.tools.export.OptionalBinExportParams
import org.locationtech.geomesa.tools.export.formats._
import org.locationtech.geomesa.tools.export._
import org.locationtech.geomesa.tools.utils.DataFormats._
import org.locationtech.geomesa.tools.utils.{CLArgResolver, DataFormats}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType
import org.locationtech.geomesa.utils.text.TextTools.getPlural
import org.opengis.filter.Filter

class ConvertCommand extends Command with LazyLogging {

  override val name = "convert"
  override val params = new ConvertParameters

  override def execute() = {
    import scala.collection.JavaConversions._

    val fmtParam = Option(params.format).flatMap(f => DataFormats.values.find(_.toString.equalsIgnoreCase(f)))
    lazy val fmtFile = params.files.flatMap(DataFormats.fromFileName(_).right.toOption).headOption
    val fmt = fmtParam.orElse(fmtFile).orNull

    val sft = CLArgResolver.getSft(params.spec)
    lazy val rsft = RichSimpleFeatureType.RichSimpleFeatureType(sft)

    logger.info(s"Using SFT definition: ${sft}")

    val converterConfig = {
      if (params.config != null)
        CLArgResolver.getConfig(params.config)
      else throw new ParameterException("Unable to parse Simple Feature type from sft config or string")
    }
    val converter = SimpleFeatureConverters.build(sft, converterConfig)

    val outFMT = DataFormats.values.find(_.toString.equalsIgnoreCase(params.outputFormat)).getOrElse {
      throw new ParameterException("Unable to parse output file type.")
    }
    if (outFMT == Bin && Seq(params.idAttribute, params.latAttribute, params.lonAttribute, params.labelAttribute).contains(null))
      throw new ParameterException("Missing parameters for binary export. For more information use: ./geomesa convert --help")

    lazy val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
    lazy val outputStream: OutputStream = ExportCommand.createOutputStream(params.file, params.gzip)
    val writer: Writer = ExportCommand.getWriter(params.asInstanceOf[RootExportParams])

    val exporter = outFMT match {
      case Csv | Tsv      => new DelimitedExporter(writer, outFMT, !params.noHeader)
      case Shp            => Seq(ShapefileExporter.modifySchema(sft))
                             new ShapefileExporter(ExportCommand.checkShpFile(params.asInstanceOf[RootExportParams]))
      case GeoJson | Json => new GeoJsonExporter(writer)
      case Gml            => new GmlExporter(outputStream)
      case Avro           => new AvroExporter(sft, outputStream, avroCompression)
      case Bin            => BinExporter(outputStream, params.asInstanceOf[BinExportParams], rsft.getDtgField)
      case _              => throw new UnsupportedOperationException(s"Format $outFMT can't be exported")
    }

    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    val fc = new DefaultFeatureCollection(sft.getTypeName, sft)

    try {
      params.files.foreach{ file =>
        val ec = converter.createEvaluationContext(Map("inputFilePath" -> file))
        val dataIter = converter.process(new FileInputStream(file.toString), ec)
        if (params.maxFeatures != null && params.maxFeatures >= 0) {
          logger.info(s"Converting ${getPlural(params.maxFeatures.toLong, "feature")} from $file")
          for (i <- 1 to params.maxFeatures) {
            if (dataIter.hasNext) fc.add(dataIter.next())
          }
        } else dataIter.foreach(fc.add)
        logger.info(s"Converted ${getPlural(ec.counter.getLineCount, "simple feature")} with ${getPlural(ec.counter.getSuccess, "success", "successes")} and ${getPlural(ec.counter.getFailure, "failure", "failures")}")
      }
      logger.debug(s"Applying CQL filter ${filter.toString}")
      exporter.export(fc.subCollection(filter))
    } finally {
      exporter.flush()
      exporter.close()
      IOUtils.closeQuietly(converter)
    }
  }
}

object ConvertParameters {
  @Parameters(commandDescription = "Convert files using GeoMesa's internal SFT converter framework")
  class ConvertParameters extends RootExportParams
    with OptionalTypeNameParam
    with OptionalBinExportParams
    with RequiredFeatureSpecParam
    with InputFileParams
}
