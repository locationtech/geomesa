/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.`export`

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.index.planning.LocalQueryRunner
import org.locationtech.geomesa.index.stats.RunnableStats
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.export.ConvertCommand.ConvertParameters
import org.locationtech.geomesa.tools.export.ExportCommand.{ChunkedExporter, ExportOptions, ExportParams, Exporter}
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.Inputs
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.StdInHandle
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Stat}
import org.locationtech.geomesa.utils.text.TextTools.getPlural

class ConvertCommand extends Command with MethodProfiling with LazyLogging {

  override val name = "convert"
  override val params = new ConvertParameters

  override def execute(): Unit = {
    def complete(count: Option[Long], time: Long): Unit =
      Command.user.info(s"Conversion complete to ${Option(params.file).getOrElse("standard out")} " +
          s"in ${time}ms${count.map(c => s" for $c features").getOrElse("")}")

    profile(complete _)(convertAndExport())
  }

  private def convertAndExport(): Option[Long] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    import scala.collection.JavaConverters._

    val inputs: Inputs = {
      val files = Inputs(params.files.asScala.toSeq)
      if (files.stdin && !StdInHandle.isAvailable) {
        if (files.paths.isEmpty) {
          throw new ParameterException("Missing option: <files>... is required, or use `-` to ingest from standard in")
        } else {
          Command.user.info("Waiting for input...")
          while (!StdInHandle.isAvailable) {
            Thread.sleep(10)
          }
        }
      }
      if (params.srcList) { files.asSourceList } else { files }
    }

    val format = IngestCommand.getDataFormat(params, inputs.paths)

    // use .get to re-throw the exception if we fail
    IngestCommand.getSftAndConverter(params, inputs, format, None).get.flatMap { case (sft: SimpleFeatureType, config: com.typesafe.config.Config) =>
      val files = inputs.handles.iterator

      WithClose(SimpleFeatureConverter(sft, config)) { converter =>
        val ec = converter.createEvaluationContext()
        val query = ExportCommand.createQuery(sft, params)
        val exporter = Option(params.chunkSize) match {
          case None    => new Exporter(ExportOptions(params), query.getHints)
          case Some(c) => new ChunkedExporter(ExportOptions(params), query.getHints, c)
        }
        try {
          val count = WithClose(ConvertCommand.convertFeatures(files, converter, ec, query)) { iter =>
            if (!params.suppressEmpty || iter.hasNext) {
              exporter.start(query.getHints.getReturnSft)
              exporter.export(iter)
            } else {
              Some(0L)
            }
          }
          val records = ec.line - (if (params.noHeader) { 0 } else { params.files.size })
          Command.user.info(s"Converted ${getPlural(records, "line")} "
              + s"with ${getPlural(ec.success.getCount, "success", "successes")} "
              + s"and ${getPlural(ec.failure.getCount, "failure")}")
          count
        } finally {
          exporter.close()
        }
      }
    }
  }
}

object ConvertCommand extends LazyLogging {

  /**
    * Convert features
    *
    * @param files inputs
    * @param converter converter
    * @param ec evaluation context
    * @param query query used to filter/transform inputs
    * @return
    */
  def convertFeatures(
      files: Iterator[FileHandle],
      converter: SimpleFeatureConverter,
      ec: EvaluationContext,
      query: Query): CloseableIterator[SimpleFeature] = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    def convert(): CloseableIterator[SimpleFeature] = CloseableIterator(files).flatMap { file =>
      file.open.flatMap { case (name, is) =>
        val params = EvaluationContext.inputFileParam(name.getOrElse(file.path))
        val context = converter.createEvaluationContext(params, ec.success, ec.failure)
        val features = converter.process(is, context)
        CloseableIterator(features, { features.close(); ec.line += context.line })
      }
    }

    def filter(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] =
      if (query.getFilter == Filter.INCLUDE) { iter } else { iter.filter(query.getFilter.evaluate) }

    def limit(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      query.getHints.getMaxFeatures match {
        case None    => iter
        case Some(m) => iter.take(m)
      }
    }

    def transform(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      import org.locationtech.geomesa.filter.filterToString

      val stats = new RunnableStats(null) {
        override protected def query[T <: Stat](sft: SimpleFeatureType, ignored: Filter, stats: String, queryHints: Hints) : Option[T] = {
          val stat = Stat(sft, stats).asInstanceOf[T]
          try {
            WithClose(limit(filter(convert())))(_.foreach(stat.observe))
            Some(stat)
          } catch {
            case e: Exception =>
              logger.error(s"Error running stats query with stats '$stats' and filter '${filterToString(ignored)}'", e)
              None
          }
        }
      }
      LocalQueryRunner.transform(converter.targetSft, iter, query.getHints.getTransform, query.getHints)
    }

    transform(limit(filter(convert())))
  }

  @Parameters(commandDescription = "Convert files using GeoMesa's internal converter framework")
  class ConvertParameters extends ExportParams with OptionalInputFormatParam with OptionalTypeNameParam
      with OptionalFeatureSpecParam with ConverterConfigParam with OptionalForceParam {
    @Parameter(names = Array("--src-list"), description = "Input files are text files with lists of files, one per line, to ingest.")
    var srcList: Boolean = false
  }
}
