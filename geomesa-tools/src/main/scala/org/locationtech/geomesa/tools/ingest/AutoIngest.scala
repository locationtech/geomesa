/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.tools.Utils.Formats
import org.locationtech.geomesa.tools.Utils.Formats.Formats

/**
 * Attempts to ingest files based on metadata stored in the file itself. Operates
 * on csv, tsv, and avro files.
 *
 * @param dsParams data store connection parameters
 * @param typeName simple feature type name
 * @param inputs files to ingest
 * @param numLocalThreads for local ingests, how many threads to use
 * @param format format of the file (must be one of TSV, CSV, AVRO)
 */
class AutoIngest(dsParams: Map[String, String],
                 typeName: String,
                 inputs: Seq[String],
                 numLocalThreads: Int,
                 format: Formats)
    extends AbstractIngest(dsParams, typeName, inputs, numLocalThreads) with LazyLogging {

  require(Seq(Formats.TSV, Formats.CSV, Formats.AVRO).contains(format),
    "Only Avro or delimited text files are supported for auto ingest")

  override def beforeRunTasks(): Unit = {}

  override def createLocalConverter(file: File, failures: AtomicLong): LocalIngestConverter = {
    format match {
      case Formats.AVRO              => new AvroIngestConverter(ds, typeName)
      case Formats.TSV | Formats.CSV => new DelimitedIngestConverter(ds, typeName, format)
      // in case someone forgets to add a new type here
      case _ => throw new UnsupportedOperationException(s"Invalid input format $format")
    }
  }

  override def runDistributedJob(statusCallback: (Float, Long, Long, Boolean) => Unit): (Long, Long) = {
    format match {
      case Formats.AVRO              => new AvroIngestJob(typeName).run(dsParams, typeName, inputs, statusCallback)
      case Formats.TSV | Formats.CSV => new DelimitedIngestJob(typeName, format).run(dsParams, typeName, inputs, statusCallback)
      // in case someone forgets to add a new type here
      case _ => throw new UnsupportedOperationException(s"Invalid input format $format")
    }
  }

}


