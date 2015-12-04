/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert.text

import java.io.{PipedReader, PipedWriter}
import java.util.concurrent.Executors

import com.google.common.collect.Queues
import com.typesafe.config.Config
import org.apache.commons.csv.{CSVFormat, QuoteMode}
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, DefaultCounter, Counter, Expr}
import org.locationtech.geomesa.convert.{Field, SimpleFeatureConverterFactory, ToSimpleFeatureConverter}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class DelimitedTextConverterFactory extends SimpleFeatureConverterFactory[String] {

  override def canProcess(conf: Config): Boolean = canProcessType(conf, "delimited-text")

  val QUOTED                    = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL)
  val QUOTE_ESCAPE              = CSVFormat.DEFAULT.withEscape('"')
  val QUOTED_WITH_QUOTE_ESCAPE  = QUOTE_ESCAPE.withQuoteMode(QuoteMode.ALL)

  def buildConverter(targetSFT: SimpleFeatureType, conf: Config): DelimitedTextConverter = {
    val baseFmt = conf.getString("format").toUpperCase match {
      case "CSV" | "DEFAULT"          => CSVFormat.DEFAULT
      case "EXCEL"                    => CSVFormat.EXCEL
      case "MYSQL"                    => CSVFormat.MYSQL
      case "TDF" | "TSV" | "TAB"      => CSVFormat.TDF
      case "RFC4180"                  => CSVFormat.RFC4180
      case "QUOTED"                   => QUOTED
      case "QUOTE_ESCAPE"             => QUOTE_ESCAPE
      case "QUOTED_WITH_QUOTE_ESCAPE" => QUOTED_WITH_QUOTE_ESCAPE
      case _ => throw new IllegalArgumentException("Unknown delimited text format")
    }

    val opts = {
      import org.locationtech.geomesa.utils.conf.ConfConversions._
      val o = "options"
      var dOpts = new DelimitedOptions()
      dOpts = conf.getIntOpt(s"$o.skip-lines").map(s => dOpts.copy(skipLines = s)).getOrElse(dOpts)
      dOpts = conf.getIntOpt(s"$o.pipe-size").map(p => dOpts.copy(pipeSize = p)).getOrElse(dOpts)
      dOpts
    }

    val fields    = buildFields(conf.getConfigList("fields"))
    val idBuilder = buildIdBuilder(conf.getString("id-field"))
    new DelimitedTextConverter(baseFmt, targetSFT, idBuilder, fields, opts)
  }
}

case class DelimitedOptions(skipLines: Int = 0,
                            pipeSize: Int = 16*1024)

class DelimitedTextConverter(format: CSVFormat,
                             val targetSFT: SimpleFeatureType,
                             val idBuilder: Expr,
                             val inputFields: IndexedSeq[Field],
                             val options: DelimitedOptions)
  extends ToSimpleFeatureConverter[String] {

  var curString: String = null
  val q = Queues.newArrayBlockingQueue[String](32)
  // if the record to write is bigger than the buffer size of the PipedReader
  // then the writer will block until the reader reads data off of the pipe.
  // For this reason, we have to separate the reading and writing into two
  // threads
  val writer = new PipedWriter()
  val reader = new PipedReader(writer, options.pipeSize)  // record size
  val parser = format.parse(reader).iterator()
  val separator = format.getRecordSeparator

  val es = Executors.newSingleThreadExecutor()
  es.submit(new Runnable {
    override def run(): Unit = {
      while (true) {
        val s = q.take()

        // make sure the input is not null and is nonempty...if it is empty the threads will deadlock
        if (s != null && s.nonEmpty) {
          writer.write(s)
          writer.write(separator)
          writer.flush()
        }
      }
    }
  })

  override def fromInputType(string: String): Seq[Array[Any]] = {
    import spire.syntax.cfor._

    // empty strings cause deadlock
    if (string == null || string.isEmpty) throw new IllegalArgumentException("Invalid input (empty)")
    q.put(string)
    val rec = parser.next()
    val len = rec.size()
    val ret = Array.ofDim[Any](len + 1)
    ret(0) = string
    cfor(0)(_ < len, _ + 1) { i =>
      ret(i+1) = rec.get(i)
    }
    Seq(ret)
  }

  private var skipLines = options.skipLines
  override def preProcess(i: String)(implicit ec: EvaluationContext): Option[String] =
    if (skipLines > 0) {
      skipLines -= 1
      None
    } else Some(i)

  override def close(): Unit = {
    es.shutdownNow()
    writer.close()
    reader.close()
  }

}
