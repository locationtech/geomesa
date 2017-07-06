/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.File

import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.tools.ingest.ConverterIngest
import org.opengis.feature.simple.SimpleFeatureType

class ParquetConverterIngest(sft: SimpleFeatureType,
                             dsParams: Map[String, String],
                             converterConfig: Config,
                             inputs: Seq[String],
                             libjarsFile: String,
                             libjarsPaths: Iterator[() => Seq[File]],
                             numLocalThreads: Int,
                             dsPath: Path,
                             tempPath: Option[Path],
                             reducers: Int)
  extends ConverterIngest(sft, dsParams, converterConfig, inputs, libjarsFile, libjarsPaths, numLocalThreads) {

  override def runDistributedJob(statusCallback: (Float, Long, Long, Boolean) => Unit = (_, _, _, _) => Unit): (Long, Long) = {
    val job = new ParquetConverterJob(sft, converterConfig, dsPath, tempPath, reducers)
    job.run(dsParams, sft.getTypeName, inputs, libjarsFile, libjarsPaths, statusCallback)
  }

}
