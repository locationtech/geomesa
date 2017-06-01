/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.{Closeable, InputStream, InputStreamReader}

import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord, QuoteMode}
import org.apache.hadoop.fs.{Path, Seekable}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.geotools.data.DataStore
import org.geotools.factory.GeoTools
import org.geotools.util.Converters
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.jobs.mapreduce.{FileStreamInputFormat, FileStreamRecordReader}
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.tools.utils.DataFormats.DataFormat
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.{ConverterFactories, SimpleFeatureTypes}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

/**
 * These classes operate on files in a specific format. The format is used by the
 * geomesa tools export, for convenience.
 *
 * The first line of each file is expected to be a delimited (tab or comma) list
 * of attributes for a simple feature type, starting with the reserved word 'id'.
 * For example:
 *
 * id,name:String,dtg:Date,geom:Point:srid=4326
 *
 * The first line of the file will be used to create a simple feature type.
 * All subsequent lines must be type convertible to the appropriate simple feature attribute.
 */
object AutoIngestDelimited {

  val FormatConfig   = "org.locationtech.geomesa.jobs.input.delimited.format"
  val TypeNameConfig = "org.locationtech.geomesa.jobs.input.delimited.typeName"

  object Counters {
    val Group = "org.locationtech.geomesa.jobs.input.delimited"
    val Read  = "read"
  }

  def getCsvFormat(format: DataFormat): CSVFormat = format match {
    case DataFormats.Csv => CSVFormat.DEFAULT.withQuoteMode(QuoteMode.MINIMAL)
    case DataFormats.Tsv => CSVFormat.TDF.withQuoteMode(QuoteMode.MINIMAL)
  }

  /**
    * Convert delimited records into simple features. Assumes a non-empty iterator.
    *
    * First line is expected to contain sft definition.
    *
    * @param iter iterator of records (non-empty)
    * @return simple feature type, iterator of simple features
    */
  def createSimpleFeatures(typeName: String, iter: Iterator[CSVRecord]): (SimpleFeatureType, Iterator[SimpleFeature]) = {
    val header = iter.next()
    require(header.get(0) == "id", "Badly formatted file detected - expected header row with attributes")
    // drop the 'id' field, at index 0
    val sftString = (1 until header.size()).map(header.get).mkString(",")
    val sft = SimpleFeatureTypes.createType(typeName, sftString)

    val converters = sft.getAttributeDescriptors.zipWithIndex.map { case (ad, i) =>
      val hints = GeoTools.getDefaultHints
      // for maps/lists, we have to pass along the subtype info during type conversion
      if (ad.isList) {
        hints.put(ConverterFactories.ListTypeKey, ad.getListType())
      } else if (ad.isMap) {
        val (k, v) = ad.getMapTypes()
        hints.put(ConverterFactories.MapKeyTypeKey, k)
        hints.put(ConverterFactories.MapValueTypeKey, v)
      }
      (ad.getType.getBinding, hints)
    }.toArray

    val features = iter.map { record =>
      val attributes = Array.ofDim[AnyRef](sft.getAttributeCount)
      var i = 1 // skip id field
      while (i < record.size()) {
        // convert the attributes directly so we can pass the collection hints
        val (clas, hints) = converters(i - 1)
        attributes(i - 1) = Converters.convert(record.get(i), clas, hints).asInstanceOf[AnyRef]
        i += 1
      }
      // we can use the no-convert constructor since we've already converted everything
      new ScalaSimpleFeature(record.get(0), sft, attributes)
    }

    (sft, features)
  }
}

/**
 * Takes a specially formatted file and turns it into simple features. Will create the
 * simple feature type if it does not already exist.
 *
 * @param ds data store - used to create the schema once determined
 * @param typeName simple feature type name to create
 * @param format CSV or TSV
 */
class DelimitedIngestConverter(ds: DataStore, typeName: String, format: DataFormat) extends LocalIngestConverter {

  var reader: CSVParser = null
  val csvFormat = AutoIngestDelimited.getCsvFormat(format)

  override def convert(is: InputStream): (SimpleFeatureType, Iterator[SimpleFeature]) = {
    reader = csvFormat.parse(new InputStreamReader(is, "UTF-8"))
    val iter = reader.iterator()
    if (!iter.hasNext) {
      (null, Iterator.empty)
    } else {
      AutoIngestDelimited.createSimpleFeatures(typeName, iter)
    }
  }

  override def close(): Unit = if (reader != null) { reader.close() }
}

/**
 * Takes specially formatted files and turns them into simple features. Will create the
 * simple feature type if it does not already exist.
 *
 * @param typeName simple feature type name
 * @param format csv or tsv
 */
class DelimitedIngestJob(typeName: String, format: DataFormat) extends AbstractIngestJob {

  import AutoIngestDelimited.Counters

  override val inputFormatClass: Class[_ <: FileInputFormat[_, SimpleFeature]] =
    classOf[DelimitedIngestInputFormat]

  override def configureJob(job: Job): Unit = {
    job.getConfiguration.set(AutoIngestDelimited.TypeNameConfig, typeName)
    job.getConfiguration.set(AutoIngestDelimited.FormatConfig, format.toString)
  }

  override def written(job: Job): Long = job.getCounters.findCounter(Counters.Group, Counters.Read).getValue
  override def failed(job: Job): Long = 0L
}

class DelimitedIngestInputFormat extends FileStreamInputFormat {
  override def createRecordReader(): FileStreamRecordReader = new DelimitedIngestRecordReader
}

class DelimitedIngestRecordReader extends FileStreamRecordReader {

  import AutoIngestDelimited.Counters

  override def createIterator(stream: InputStream with Seekable,
                              filePath: Path,
                              context: TaskAttemptContext): Iterator[SimpleFeature] with Closeable = {
    val format = DataFormats.withName(context.getConfiguration.get(AutoIngestDelimited.FormatConfig))
    val typeName = context.getConfiguration.get(AutoIngestDelimited.TypeNameConfig)
    val reader = AutoIngestDelimited.getCsvFormat(format).parse(new InputStreamReader(stream, "UTF-8"))
    val iter = reader.iterator()
    if (!iter.hasNext) {
      new Iterator[SimpleFeature] with Closeable {
        override def hasNext: Boolean = false
        override def next(): SimpleFeature = Iterator.empty.next()
        override def close(): Unit = {}
      }
    } else {
      val (_, features) = AutoIngestDelimited.createSimpleFeatures(typeName, iter)
      val counter = context.getCounter(Counters.Group, Counters.Read)
      new Iterator[SimpleFeature] with Closeable {
        override def hasNext: Boolean = features.hasNext
        override def next(): SimpleFeature = {
          counter.increment(1)
          features.next()
        }
        override def close(): Unit = reader.close()
      }
    }
  }
}
