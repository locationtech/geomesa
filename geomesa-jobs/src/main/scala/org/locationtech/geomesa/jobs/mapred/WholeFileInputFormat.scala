/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.jobs.mapred

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, IOUtils, Text}
import org.apache.hadoop.mapred._

class WholeFileInputFormat extends FileInputFormat[Text, BytesWritable] {

  override protected def isSplitable(fs: FileSystem, filename: Path): Boolean = false

  override def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter): RecordReader[Text, BytesWritable] = {
    new WholeFileRecordReader(split.asInstanceOf[FileSplit], job, reporter)
  }
}

class WholeFileRecordReader(split: FileSplit, job: JobConf, reporter: Reporter)
    extends RecordReader[Text, BytesWritable] {

  private var processed = false

  override def next(key: Text, value: BytesWritable): Boolean = {
    if (processed) {
      false
    } else {
      key.set(split.getPath.toString)
      val length = split.getLength.toInt
      value.setSize(0)
      value.setSize(length)

      val fs = FileSystem.get(job)
      var in: FSDataInputStream = null
      try {
        in = fs.open(split.getPath)
        IOUtils.readFully(in, value.getBytes, 0, length)
      } finally {
        IOUtils.closeStream(in)
      }
      processed = true
      true
    }
  }

  override def getProgress: Float = reporter.getProgress

  override def getPos: Long = 0

  override def createKey(): Text = new Text()

  override def createValue(): BytesWritable = new BytesWritable()

  override def close(): Unit = {}
}