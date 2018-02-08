/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.{File, InputStream}

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.geotools.data.DataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.jobs.mapreduce.AvroFileInputFormat
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * These classes operate on files in the specific avro format used by
 * <code>org.locationtech.geomesa.features.avro.AvroDataFileWriter</code>.
 * The format is used by the geomesa tools export, for convenience.
 *
 * @param ds data store used to create the schema
 * @param typeName simple feature type name to use
 */
class AvroIngestConverter(ds: DataStore, typeName: String) extends LocalIngestConverter {

  var reader: AvroDataFileReader = _

  override def convert(is: InputStream): (SimpleFeatureType, Iterator[SimpleFeature]) = {
    reader = new AvroDataFileReader(is)
    val dataSft = reader.getSft
    val sft = if (typeName == dataSft.getTypeName) dataSft else SimpleFeatureTypes.renameSft(dataSft, typeName)
    ds.createSchema(sft)
    val features = if (dataSft == sft) { reader } else { reader.map(ScalaSimpleFeature.copy(sft, _)) }
    (sft, features)
  }

  override def close(): Unit = if (reader != null) { reader.close() }
}

/**
 * Distributed job to ingest files in the specific avro format.
 *
 * @param typeName simple feature type name
 */
class AvroIngestJob(dsParams: Map[String, String],
                    typeName: String,
                    paths: Seq[String],
                    libjarsFile: String,
                    libjarsPaths: Iterator[() => Seq[File]])
    extends AbstractIngestJob(dsParams, typeName, paths, libjarsFile, libjarsPaths) {

  import AvroFileInputFormat.Counters._

  override val inputFormatClass: Class[_ <: FileInputFormat[_, SimpleFeature]] = classOf[AvroFileInputFormat]

  override def configureJob(job: Job): Unit = {
    super.configureJob(job)
    AvroFileInputFormat.setTypeName(job, typeName)
  }

  override def written(job: Job): Long = job.getCounters.findCounter(Group, Read).getValue
  override def failed(job: Job): Long = 0L
}
