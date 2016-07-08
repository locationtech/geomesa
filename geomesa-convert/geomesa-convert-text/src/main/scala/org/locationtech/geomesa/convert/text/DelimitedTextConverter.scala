/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert.text

import java.io._
import java.util.concurrent.Executors

import com.google.common.collect.Queues
import com.typesafe.config.Config
import org.apache.commons.csv.{CSVFormat, QuoteMode}
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Expr}
import org.locationtech.geomesa.convert.{AbstractSimpleFeatureConverterFactory, Field, LinesToSimpleFeatureConverter}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.immutable.IndexedSeq

class DelimitedTextConverterFactory extends AbstractSimpleFeatureConverterFactory[String] {

  override protected val typeToProcess = "delimited-text"

  val QUOTED                    = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL)
  val QUOTE_ESCAPE              = CSVFormat.DEFAULT.withEscape('"')
  val QUOTED_WITH_QUOTE_ESCAPE  = QUOTE_ESCAPE.withQuoteMode(QuoteMode.ALL)

  override protected def buildConverter(sft: SimpleFeatureType,
                                        conf: Config,
                                        idBuilder: Expr,
                                        fields: IndexedSeq[Field],
                                        userDataBuilder: Map[String, Expr],
                                        validating: Boolean): DelimitedTextConverter = {
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
      val dOpts = new DelimitedOptions()
      conf.getIntOpt(s"$o.skip-lines").foreach(s => dOpts.skipLines = s)
      conf.getIntOpt(s"$o.pipe-size").foreach(p => dOpts.pipeSize = p)
      dOpts
    }

    new DelimitedTextConverter(baseFmt, sft, idBuilder, fields, userDataBuilder, opts, validating)
  }
}

class DelimitedOptions(var skipLines: Int = 0, var pipeSize: Int = 16 * 1024)

class DelimitedTextConverter(format: CSVFormat,
                             val targetSFT: SimpleFeatureType,
                             val idBuilder: Expr,
                             val inputFields: IndexedSeq[Field],
                             val userDataBuilder: Map[String, Expr],
                             val options: DelimitedOptions,
                             val validating: Boolean)
  extends LinesToSimpleFeatureConverter {

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

  override def processInput(is: Iterator[String], ec: EvaluationContext): Iterator[SimpleFeature] = {
    ec.counter.incLineCount(options.skipLines)
    super.processInput(is.drop(options.skipLines), ec)
  }

  override def fromInputType(string: String): Seq[Array[Any]] = {
    // empty strings cause deadlock
    if (string == null || string.isEmpty) {
      throw new IllegalArgumentException("Invalid input (empty)")
    }
    q.put(string)
    val rec = parser.next()
    val len = rec.size()
    val ret = Array.ofDim[Any](len + 1)
    ret(0) = string
    var i = 0
    while (i < len) {
      ret(i+1) = rec.get(i)
      i += 1
    }
    Seq(ret)
  }

  override def close(): Unit = {
    es.shutdownNow()
    writer.close()
    reader.close()
  }

}
