/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.convert.text

import java.io.{PipedReader, PipedWriter}
import java.util.concurrent.{TimeUnit, Executors}

import com.google.common.collect.Queues
import com.typesafe.config.Config
import org.apache.commons.csv.{CSVFormat, QuoteMode}
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert.{Field, SimpleFeatureConverterFactory, ToSimpleFeatureConverter}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class DelimitedTextConverterFactory extends SimpleFeatureConverterFactory[String] {

  override def canProcess(conf: Config): Boolean = canProcessType(conf, "delimited-text")

  val QUOTED                    = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL)
  val QUOTE_ESCAPE              = CSVFormat.DEFAULT.withEscape('"')
  val QUOTED_WITH_QUOTE_ESCAPE  = QUOTE_ESCAPE.withQuoteMode(QuoteMode.ALL)

  def buildConverter(targetSFT: SimpleFeatureType, conf: Config): DelimitedTextConverter = {
    val format    = conf.getString("format") match {
      case "DEFAULT"                  => CSVFormat.DEFAULT
      case "EXCEL"                    => CSVFormat.EXCEL
      case "MYSQL"                    => CSVFormat.MYSQL
      case "TDF"                      => CSVFormat.TDF
      case "RFC4180"                  => CSVFormat.RFC4180
      case "QUOTED"                   => QUOTED
      case "QUOTE_ESCAPE"             => QUOTE_ESCAPE
      case "QUOTED_WITH_QUOTE_ESCAPE" => QUOTED_WITH_QUOTE_ESCAPE
      case _ => throw new IllegalArgumentException("Unknown delimited text format")
    }
    val fields    = buildFields(conf.getConfigList("fields"))
    val idBuilder = buildIdBuilder(conf.getString("id-field"))
    val pipeSize  = if(conf.hasPath("pipe-size")) conf.getInt("pipe-size") else 16*1024
    new DelimitedTextConverter(format, targetSFT, idBuilder, fields, pipeSize)
  }
}

class DelimitedTextConverter(format: CSVFormat,
                             val targetSFT: SimpleFeatureType,
                             val idBuilder: Expr,
                             val inputFields: IndexedSeq[Field],
                             val inputSize: Int = 16*1024)
  extends ToSimpleFeatureConverter[String] {

  var curString: String = null
  val q = Queues.newArrayBlockingQueue[String](32)
  // if the record to write is bigger than the buffer size of the PipedReader
  // then the writer will block until the reader reads data off of the pipe.
  // For this reason, we have to separate the reading and writing into two
  // threads
  val writer = new PipedWriter()
  val reader = new PipedReader(writer, inputSize) // 16k records
  val parser = format.parse(reader).iterator()
  val separator = format.getRecordSeparator

  val es = Executors.newSingleThreadExecutor()
  es.submit(new Runnable {
    override def run(): Unit = {
      while (true) {
        val s = q.take()
        if(s != null) {
          writer.write(s)
          writer.write(separator)
          writer.flush()
        }
      }
    }
  })

  def fromInputType(string: String): Array[Any] = {
    import spire.syntax.cfor._

    q.put(string)
    val rec = parser.next()
    val len = rec.size()
    val ret = Array.ofDim[Any](len + 1)
    ret(0) = string
    cfor(0)(_ < len, _ + 1) { i =>
      ret(i+1) = rec.get(i)
    }
    ret
  }

  override def close(): Unit = {
    es.shutdownNow()
    writer.close()
    reader.close()
  }
}
