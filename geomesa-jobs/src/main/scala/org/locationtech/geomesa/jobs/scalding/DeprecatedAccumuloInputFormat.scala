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

package org.locationtech.geomesa.jobs.scalding

import java.io.{DataInput, DataOutput}

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.InputFormatBase.RangeInputSplit
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce

import scala.collection.JavaConversions._

/**
 * Wraps accumulo code in old mapreduce API for compatibility with hadoop 0.20.x
 */
class DeprecatedAccumuloInputFormat extends InputFormat[Key,Value]  {

  val inputFormat = new AccumuloInputFormat()

  override def getSplits(conf: JobConf, numSplits: Int): Array[InputSplit] = {
    val ranges = inputFormat.getSplits(new mapreduce.JobContext(conf, new JobID()))
    ranges.map(is => new DeprecatedInputSplit(is.asInstanceOf[RangeInputSplit])).toArray
  }

  override def getRecordReader(split: InputSplit, conf: JobConf, reporter: Reporter): RecordReader[Key, Value] = {
    val taskAttemptContext = new mapreduce.TaskAttemptContext(conf, new TaskAttemptID())
    val delegate = inputFormat.createRecordReader(null, taskAttemptContext)
    delegate.initialize(split.asInstanceOf[DeprecatedInputSplit].is, taskAttemptContext)
    new DeprecatedRecordReader(delegate)
  }
}

class DeprecatedInputSplit extends InputSplit {

  var is: RangeInputSplit = new RangeInputSplit()

  def this(inputSplit: RangeInputSplit) {
    this()
    is = inputSplit
  }

  def getLength: Long = is.getLength

  def getLocations: Array[String] = is.getLocations

  def write(p1: DataOutput) = is.write(p1)

  def readFields(p1: DataInput) = is.readFields(p1)
}

class DeprecatedRecordReader(delegate: org.apache.hadoop.mapreduce.RecordReader[Key, Value])
    extends RecordReader[Key,Value] {

  def close() = delegate.close()

  def next(key: Key, value: Value): Boolean =
    if (delegate.nextKeyValue()) {
      key.set(delegate.getCurrentKey)
      value.set(delegate.getCurrentValue.get())
      true
    } else {
      false
    }

  def createKey(): Key = new Key()

  def createValue(): Value = new Value()

  def getPos: Long = 0

  def getProgress: Float = delegate.getProgress
}