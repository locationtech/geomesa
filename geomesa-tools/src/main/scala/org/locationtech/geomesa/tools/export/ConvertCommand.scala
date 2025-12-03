/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.tools.`export`

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.index.planning.LocalQueryRunner.LocalProcessor
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.`export`.ConvertCommand.ResultTracker
import org.locationtech.geomesa.tools.export.ConvertCommand.ConvertParameters
import org.locationtech.geomesa.tools.export.ExportCommand.{ChunkedExporter, ExportOptions, ExportParams, Exporter}
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.Inputs
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.StdInHandle
import org.locationtech.geomesa.utils.text.TextTools.getPlural

class ConvertCommand extends Command with LazyLogging {

  override val name = "convert"
  override val params = new ConvertParameters

  override def execute(): Unit = {
    val start = System.currentTimeMillis()
    val count = convertAndExport()
    Command.user.info(s"Conversion complete to ${Option(params.file).getOrElse("standard out")} " +
      s"in ${System.currentTimeMillis() - start}ms${count.map(c => s" for $c features").getOrElse("")}")
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
        val results = new ResultTracker()
        val query = ExportCommand.createQuery(sft, params)
        val exporter = Option(params.chunkSize) match {
          case None    => new Exporter(ExportOptions(params), query.getHints)
          case Some(c) => new ChunkedExporter(ExportOptions(params), query.getHints, c)
        }
        try {
          val count = WithClose(ConvertCommand.convertFeatures(files, converter, results, query)) { iter =>
            if (!params.suppressEmpty || iter.hasNext) {
              exporter.start(query.getHints.getReturnSft)
              exporter.export(iter)
            } else {
              Some(0L)
            }
          }
          Command.user.info(s"Converted ${getPlural(results.lines, "line")} "
              + s"with ${getPlural(results.successes, "success", "successes")} "
              + s"and ${getPlural(results.failures, "failure")}")
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
    * @param tracker holder for returning conversion results
    * @param query query used to filter/transform inputs
    * @return
    */
  private def convertFeatures(
      files: Iterator[FileHandle],
      converter: SimpleFeatureConverter,
      tracker: ResultTracker,
      query: Query): CloseableIterator[SimpleFeature] = {

    def convert(): CloseableIterator[SimpleFeature] = CloseableIterator(files).flatMap { file =>
      file.open.flatMap { case (name, is) =>
        val params = EvaluationContext.inputFileParam(name.getOrElse(file.path))
        val context = converter.createEvaluationContext(params)
        val features = converter.process(is, context)
        CloseableIterator(features, { features.close(); tracker.update(context) })
      }
    }

    def filter(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] =
      if (query.getFilter == Filter.INCLUDE) { iter } else { iter.filter(query.getFilter.evaluate) }

    val processor = LocalProcessor(converter.targetSft, query.getHints, None)

    def reduce(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] =
      processor.reducer.fold(iter)(_.apply(iter))

    reduce(processor(filter(convert())))
  }

  private class ResultTracker {
    var failures: Long = 0
    var successes: Long = 0
    var lines: Long = 0

    def update(context: EvaluationContext): Unit = {
      failures += context.stats.failure(0)
      successes += context.stats.success(0)
      lines += context.line
    }
  }

  @Parameters(commandDescription = "Convert files using GeoMesa's internal converter framework")
  class ConvertParameters extends ExportParams with OptionalInputFormatParam with OptionalTypeNameParam
      with OptionalFeatureSpecParam with ConverterConfigParam with OptionalForceParam {
    @Parameter(names = Array("--src-list"), description = "Input files are text files with lists of files, one per line, to ingest.")
    var srcList: Boolean = false
  }
}
