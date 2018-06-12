/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.text

import java.io._
import java.nio.charset.Charset

import com.typesafe.config.Config
import org.apache.commons.csv.{CSVFormat, QuoteMode}
import org.locationtech.geomesa.convert.ErrorMode.ErrorMode
import org.locationtech.geomesa.convert.ParseMode.ParseMode
import org.locationtech.geomesa.convert.text.DelimitedTextConverter.{DelimitedTextConfig, DelimitedTextOptions}
import org.locationtech.geomesa.convert.{EvaluationContext, SimpleFeatureValidator}
import org.locationtech.geomesa.convert2.AbstractConverter.BasicField
import org.locationtech.geomesa.convert2._
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeatureType

import scala.annotation.tailrec

class DelimitedTextConverter(targetSft: SimpleFeatureType,
                             config: DelimitedTextConfig,
                             fields: Seq[BasicField],
                             options: DelimitedTextOptions)
    extends AbstractConverter(targetSft, config, fields, options) {

  private val format = {
    var format = DelimitedTextConverter.formats.getOrElse(config.format.toUpperCase,
      throw new IllegalArgumentException(s"Unknown delimited text format '${config.format}'"))
    options.quote.foreach(c => format = format.withQuote(c))
    options.escape.foreach(c => format = format.withEscape(c))
    options.delimiter.foreach(c => format = format.withDelimiter(c))
    format
  }

  override protected def read(is: InputStream, ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    var array = Array.empty[Any]
    val writer = new StringWriter
    val printer = format.print(writer)

    val parser = format.parse(new InputStreamReader(is, options.encoding))
    val records = parser.iterator()

    val elements = new Iterator[Array[Any]] {

      private var lastLine = 0L
      private var staged: Array[Any] = _

      @tailrec
      override final def hasNext: Boolean = staged != null || {
        if (!records.hasNext) {
          false
        } else {
          val record = records.next
          val line = parser.getCurrentLineNumber
          if (line == lastLine) {
            // commons-csv doesn't always increment the line count for the final line in a file...
            ec.counter.incLineCount()
            lastLine = line + 1
          } else {
            ec.counter.incLineCount(line - lastLine)
            lastLine = line
          }

          if (options.skipLines.exists(lastLine <= _)) {
            hasNext
          } else {
            writer.getBuffer.setLength(0)

            val len = record.size()
            if (array.length != len + 1) {
              array = Array.ofDim[Any](len + 1)
            }

            var i = 0
            while (i < len) {
              val value = record.get(i)
              array(i + 1) = value
              printer.print(value)
              i += 1
            }

            printer.println()
            array(0) = writer.toString

            staged = array
            true
          }
        }
      }

      override def next(): Array[Any] = {
        val res = staged
        staged = null
        res
      }
    }

    CloseableIterator(elements, parser.close())
  }
}

object DelimitedTextConverter {

  object Formats {
    val Default          : CSVFormat = CSVFormat.DEFAULT
    val Excel            : CSVFormat = CSVFormat.EXCEL
    val MySql            : CSVFormat = CSVFormat.MYSQL
    val Tabs             : CSVFormat = CSVFormat.TDF
    val Rfc4180          : CSVFormat = CSVFormat.RFC4180
    val Quoted           : CSVFormat = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL)
    val QuoteEscape      : CSVFormat = CSVFormat.DEFAULT.withEscape('"')
    val QuotedQuoteEscape: CSVFormat = CSVFormat.DEFAULT.withEscape('"').withQuoteMode(QuoteMode.ALL)
  }

  private [text] val formats = Map(
    "CSV"                      -> Formats.Default,
    "DEFAULT"                  -> Formats.Default,
    "EXCEL"                    -> Formats.Excel,
    "MYSQL"                    -> Formats.MySql,
    "TDF"                      -> Formats.Tabs,
    "TSV"                      -> Formats.Tabs,
    "TAB"                      -> Formats.Tabs,
    "RFC4180"                  -> Formats.Rfc4180,
    "QUOTED"                   -> Formats.Quoted,
    "QUOTE_ESCAPE"             -> Formats.QuoteEscape,
    "QUOTED_WITH_QUOTE_ESCAPE" -> Formats.QuotedQuoteEscape
  )

  case class DelimitedTextConfig(`type`: String,
                                 format: String,
                                 idField: Option[Expression],
                                 caches: Map[String, Config],
                                 userData: Map[String, Expression]) extends ConverterConfig

  case class DelimitedTextOptions(skipLines: Option[Int],
                                  quote: Option[Char],
                                  escape: Option[Char],
                                  delimiter: Option[Char],
                                  validators: SimpleFeatureValidator,
                                  parseMode: ParseMode,
                                  errorMode: ErrorMode,
                                  encoding: Charset,
                                  verbose: Boolean) extends ConverterOptions
}
