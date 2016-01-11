/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.jobs.mapreduce

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, IOUtils, Text}
import org.apache.hadoop.mapreduce.{JobContext, RecordReader, TaskAttemptContext, InputSplit}

import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat}

class WholeFileInputFormat extends FileInputFormat[Text, BytesWritable] {

  override protected def isSplitable(context: JobContext, filename: Path): Boolean = false

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, BytesWritable] = {
    new WholeFileRecordReader(split.asInstanceOf[FileSplit], context)
  }
}

class WholeFileRecordReader(split: FileSplit, context: TaskAttemptContext)
    extends RecordReader[Text, BytesWritable] {

  private var processed = false
  private val key = new Text()
  private val value = new BytesWritable()

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    processed = false
  }

  override def getProgress: Float = context.getProgress

  override def nextKeyValue(): Boolean = {
    if (processed) {
      false
    } else {
      key.set(split.getPath.toString)
      val length = split.getLength.toInt
      value.setSize(0)
      value.setSize(length)

      val fs = FileSystem.get(context.getConfiguration)
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

  override def getCurrentKey: Text = key

  override def getCurrentValue: BytesWritable = value

  override def close(): Unit = {}
}