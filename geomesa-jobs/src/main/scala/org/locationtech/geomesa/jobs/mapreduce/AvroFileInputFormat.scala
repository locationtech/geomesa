/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import java.io.{Closeable, InputStream}

import org.apache.hadoop.fs.{Path, Seekable}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature

/**
 * Class for reading avro files written using
 * <code>org.locationtech.geomesa.features.avro.AvroDataFileWriter</code>.
 */
class AvroFileInputFormat extends FileStreamInputFormat {
  override def createRecordReader(): FileStreamRecordReader = new AvroFileRecordReader
}

object AvroFileInputFormat {
  object Counters {
    val Group = "org.locationtech.geomesa.jobs.input.avro"
    val Read  = "read"
  }

  def setTypeName(job: Job, typeName: String): Unit =
    job.getConfiguration.set(FileStreamInputFormat.TypeNameKey, typeName)
}

class AvroFileRecordReader extends FileStreamRecordReader {

  import AvroFileInputFormat.Counters

  override def createIterator(stream: InputStream with Seekable,
                              filePath: Path,
                              context: TaskAttemptContext): Iterator[SimpleFeature] with Closeable = {
    val typeName = context.getConfiguration.get(FileStreamInputFormat.TypeNameKey)
    val reader = new AvroDataFileReader(stream)
    val dataSft = reader.getSft
    val counter = context.getCounter(Counters.Group, Counters.Read)

    if (typeName == null || dataSft.getTypeName == typeName) {
      new Iterator[SimpleFeature] with Closeable {
        override def hasNext: Boolean = reader.hasNext
        override def next(): SimpleFeature = {
          counter.increment(1)
          reader.next()
        }
        override def close(): Unit = reader.close()
      }
    } else {
      val sft = SimpleFeatureTypes.renameSft(dataSft, typeName)
      new Iterator[SimpleFeature] with Closeable {
        override def hasNext: Boolean = reader.hasNext
        override def next(): SimpleFeature = {
          val sf = reader.next()
          counter.increment(1)
          new ScalaSimpleFeature(sft, sf.getID, sf.getAttributes.toArray)
        }
        override def close(): Unit = reader.close()
      }
    }
  }
}
