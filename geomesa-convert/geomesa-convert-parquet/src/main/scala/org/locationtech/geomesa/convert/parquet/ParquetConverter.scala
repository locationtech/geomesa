/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.parquet

import java.io._

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicField, BasicOptions}
import org.locationtech.geomesa.convert2._
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseWithLogging, PathUtils}
import org.opengis.feature.simple.SimpleFeatureType

class ParquetConverter(
    sft: SimpleFeatureType,
    config: BasicConfig,
    fields: Seq[BasicField],
    options: BasicOptions
  ) extends AbstractConverter[GenericRecord, BasicConfig, BasicField, BasicOptions](sft, config, fields, options) {

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[GenericRecord] = {
    CloseWithLogging(is) // we don't use the input stream, just close it

    val path = ec.getInputFilePath.getOrElse {
      throw new IllegalArgumentException(s"Parquet converter requires '${EvaluationContext.InputFilePathKey}' " +
          "to be set in the evaluation context")
    }

    // note: get the path as a URI so that we handle local vs hdfs files appropriately
    ParquetConverter.iterator(new Path(PathUtils.getUrl(path).toURI), ec)
  }

  override protected def values(
      parsed: CloseableIterator[GenericRecord],
      ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    val array = Array.ofDim[Any](1)
    parsed.map { record => array(0) = record; array }
  }
}

object ParquetConverter {

  /**
    * Creates a parquet iterator for a given file
    *
    * @param path file path
    * @param ec evalution context
    * @return
    */
  def iterator(
      path: Path,
      ec: EvaluationContext,
      conf: Configuration = new Configuration()): CloseableIterator[GenericRecord] = {
    new ParquetIterator(path, ec, conf)
  }

  /**
    * Parses a file into parquet (generic avro) records
    *
    * @param path input file
    * @param ec evaluation context
    * @param conf hadoop configuration
    */
  private class ParquetIterator(path: Path, ec: EvaluationContext, conf: Configuration)
      extends CloseableIterator[GenericRecord] {

    import org.apache.avro.generic.GenericRecord

    private val reader = ParquetReader.builder[GenericRecord](new AvroReadSupport, path).withConf(conf).build()

    private var staged: GenericRecord = _

    override final def hasNext: Boolean = staged != null || {
      staged = reader.read()
      staged != null
    }

    override def next(): GenericRecord = {
      if (!hasNext) { Iterator.empty.next() } else {
        ec.line += 1
        val record = staged
        staged = null
        record
      }
    }

    override def close(): Unit = reader.close()
  }
}
