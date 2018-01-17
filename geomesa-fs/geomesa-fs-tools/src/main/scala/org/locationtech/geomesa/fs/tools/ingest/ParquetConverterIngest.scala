/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.File

import com.beust.jcommander.ParameterException
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.ingest.AbstractIngest.StatusCallback
import org.locationtech.geomesa.tools.ingest.ConverterIngest
import org.opengis.feature.simple.SimpleFeatureType

class ParquetConverterIngest(sft: SimpleFeatureType,
                             dsParams: Map[String, String],
                             converterConfig: Config,
                             inputs: Seq[String],
                             mode: Option[RunMode],
                             libjarsFile: String,
                             libjarsPaths: Iterator[() => Seq[File]],
                             numLocalThreads: Int,
                             dsPath: Path,
                             tempPath: Option[Path],
                             reducers: Option[java.lang.Integer])
  extends ConverterIngest(sft, dsParams, converterConfig, inputs, mode, libjarsFile, libjarsPaths, numLocalThreads) {

  override def runDistributedJob(statusCallback: StatusCallback): (Long, Long) = {
    if (reducers.isEmpty) {
      throw new ParameterException("Must provide num-reducers argument for distributed ingest")
    }
    val job = new ParquetConverterJob(sft, converterConfig, dsPath, tempPath, reducers.get)
    job.run(dsParams, sft.getTypeName, inputs, libjarsFile, libjarsPaths, statusCallback)
  }

}
