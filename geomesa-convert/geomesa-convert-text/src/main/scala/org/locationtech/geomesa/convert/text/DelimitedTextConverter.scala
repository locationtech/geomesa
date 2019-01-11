/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.text

import java.io._
import java.nio.charset.{Charset, StandardCharsets}

import com.typesafe.config.Config
import org.apache.commons.csv.{CSVFormat, QuoteMode}
import org.geotools.factory.GeoTools
import org.geotools.util.Converters
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.convert.Modes.ParseMode
import org.locationtech.geomesa.convert.text.DelimitedTextConverter.{DelimitedTextConfig, DelimitedTextOptions}
import org.locationtech.geomesa.convert.{EvaluationContext, SimpleFeatureValidator}
import org.locationtech.geomesa.convert2.AbstractConverter.BasicField
import org.locationtech.geomesa.convert2._
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.converters.StringCollectionConverterFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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

    // printer used to re-create the original line
    // suppress the final newline so that we match the original behavior of splitting on newlines
    val printer = format.withRecordSeparator(null).print(writer)

    val parser = format.parse(new InputStreamReader(is, options.encoding))
    val records = parser.iterator()

    val elements: Iterator[Array[Any]] = new Iterator[Array[Any]] {

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
    val QuotedMinimal    : CSVFormat = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.MINIMAL)
    val TabsQuotedMinimal: CSVFormat = CSVFormat.TDF.withQuoteMode(QuoteMode.MINIMAL)
  }

  /**
    * Parses a delimited file with a 'magic' header. The first column must be the feature ID, and the header
    * must be `id`. Subsequent columns must be the feature attributes, in order. For each attribute, the header
    * must be a simple feature type attribute specification, consisting of the attribute name and the attribute
    * binding
    *
    * For example:
    *
    * id,name:String,age:Int,*geom:Point:srid=4326
    * fid-0,name0,0,POINT(40 50)
    * fid-1,name1,1,POINT(41 51)
    *
    * @param typeName simple feature type name
    * @param is input stream
    * @param format parsing format
    * @return
    */
  def magicParsing(typeName: String,
                   is: InputStream,
                   format: CSVFormat = Formats.QuotedMinimal): CloseableIterator[SimpleFeature] = {
    import scala.collection.JavaConverters._

    val parser = format.parse(new InputStreamReader(is, StandardCharsets.UTF_8))
    val records = parser.iterator()

    val header = if (records.hasNext) { records.next() } else { null }

    require(header != null && header.get(0) == "id",
      "Badly formatted file detected - expected header row with attributes")

    // drop the 'id' field, at index 0
    val sftString = (1 until header.size()).map(header.get).mkString(",")
    val sft = SimpleFeatureTypes.createType(typeName, sftString)

    val converters = sft.getAttributeDescriptors.asScala.map { ad =>
      import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
      val hints = GeoTools.getDefaultHints
      // for maps/lists, we have to pass along the subtype info during type conversion
      if (ad.isList) {
        hints.put(StringCollectionConverterFactory.ListTypeKey, ad.getListType())
      } else if (ad.isMap) {
        val (k, v) = ad.getMapTypes()
        hints.put(StringCollectionConverterFactory.MapKeyTypeKey, k)
        hints.put(StringCollectionConverterFactory.MapValueTypeKey, v)
      }
      (ad.getType.getBinding, hints)
    }.toArray

    val features = records.asScala.map { record =>
      val attributes = Array.ofDim[AnyRef](sft.getAttributeCount)
      var i = 1 // skip id field
      while (i < record.size()) {
        // convert the attributes directly so we can pass the collection hints
        val (clas, hints) = converters(i - 1)
        attributes(i - 1) = Converters.convert(record.get(i), clas, hints).asInstanceOf[AnyRef]
        i += 1
      }
      // we can use the no-convert constructor since we've already converted everything
      new ScalaSimpleFeature(sft, record.get(0), attributes)
    }

    CloseableIterator(features, parser.close())
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
    "QUOTED_WITH_QUOTE_ESCAPE" -> Formats.QuotedQuoteEscape,
    "QUOTED_MINIMAL"           -> Formats.QuotedMinimal,
    "TSV_QUOTED_MINIMAL"       -> Formats.TabsQuotedMinimal
  )

  // check quoted before default - if values are quoted, we don't want the quotes to be captured as part of the value
  private [text] val inferences = Seq(Formats.Tabs, Formats.Quoted, Formats.Default)

  case class DelimitedTextConfig(`type`: String,
                                 format: String,
                                 idField: Option[Expression],
                                 caches: Map[String, Config],
                                 userData: Map[String, Expression]) extends ConverterConfig

  case class DelimitedTextOptions(skipLines: Option[Int],
                                  quote: OptionalChar,
                                  escape: OptionalChar,
                                  delimiter: Option[Char],
                                  validators: SimpleFeatureValidator,
                                  parseMode: ParseMode,
                                  errorMode: ErrorMode,
                                  encoding: Charset,
                                  verbose: Boolean) extends ConverterOptions

  sealed trait OptionalChar {
    def foreach[U](f: Character => U): Unit
  }

  final case object CharNotSpecified extends OptionalChar {
    override def foreach[U](f: Character => U): Unit = {}
  }
  final case class CharEnabled(char: Char) extends OptionalChar {
    override def foreach[U](f: Character => U): Unit = f.apply(char)
  }
  final case object CharDisabled extends OptionalChar {
    override def foreach[U](f: Character => U): Unit = f.apply(null)
  }
}
