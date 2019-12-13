/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc.jobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.{OrcMapreduceRecordWriter, OrcOutputFormat}
import org.apache.orc.{OrcConf, OrcFile, TypeDescription}
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.orc.jobs.OrcSimpleFeatureOutputFormat.OrcRecordWriter
import org.locationtech.geomesa.fs.storage.orc.utils.OrcOutputFormatWriter
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class OrcSimpleFeatureOutputFormat extends FileOutputFormat[Void, SimpleFeature] {

  private val delegate = new OrcOutputFormat[OrcStruct]

  def getRecordWriter(context: TaskAttemptContext, file: Path): RecordWriter[Void, SimpleFeature] = {
    val options = org.apache.orc.mapred.OrcOutputFormat.buildOptions(context.getConfiguration)
    val writer = new OrcMapreduceRecordWriter[OrcStruct](OrcFile.createWriter(file, options))
    getRecordWriter(context, writer)
  }

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Void, SimpleFeature] =
    getRecordWriter(context, delegate.getRecordWriter(context))

  override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path =
    delegate.getDefaultWorkFile(context, extension)

  private def getRecordWriter(context: TaskAttemptContext,
                              orcWriter: RecordWriter[NullWritable, OrcStruct]): RecordWriter[Void, SimpleFeature] = {
    val sft = StorageConfiguration.getSft(context.getConfiguration)
    val description = OrcSimpleFeatureOutputFormat.getDescription(context.getConfiguration)
    new OrcRecordWriter(sft, description, orcWriter)
  }
}

object OrcSimpleFeatureOutputFormat {

  def setDescription(conf: Configuration, description: TypeDescription): Unit =
    conf.set(OrcConf.MAPRED_OUTPUT_SCHEMA.getAttribute, description.toString)

  def getDescription(conf: Configuration): TypeDescription =
    TypeDescription.fromString(OrcConf.MAPRED_OUTPUT_SCHEMA.getString(conf))

  class OrcRecordWriter(sft: SimpleFeatureType,
                        description: TypeDescription,
                        delegate: RecordWriter[NullWritable, OrcStruct])
      extends RecordWriter[Void, SimpleFeature] {

    private val writer = OrcOutputFormatWriter(sft, description)
    private val key = NullWritable.get()
    private val struct = new OrcStruct(description)

    override def write(key: Void, value: SimpleFeature): Unit = {
      writer.apply(value, struct)
      delegate.write(this.key, struct)
    }

    override def close(context: TaskAttemptContext): Unit = delegate.close(context)
  }
}