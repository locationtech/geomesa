/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc.jobs

import org.apache.hadoop.mapreduce.Job
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemStorage

trait OrcStorageConfiguration extends StorageConfiguration {
  override def configureOutput(sft: SimpleFeatureType, job: Job): Unit = {
    job.setOutputFormatClass(classOf[OrcPartitionOutputFormat])
    StorageConfiguration.setSft(job.getConfiguration, sft)
    OrcSimpleFeatureOutputFormat.setDescription(job.getConfiguration,
      OrcFileSystemStorage.createTypeDescription(sft))
  }
}
