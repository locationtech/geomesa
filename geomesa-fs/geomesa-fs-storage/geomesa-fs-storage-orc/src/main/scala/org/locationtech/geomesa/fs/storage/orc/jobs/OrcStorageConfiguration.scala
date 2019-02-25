/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc.jobs

import org.apache.hadoop.mapreduce.Job
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemStorage
import org.opengis.feature.simple.SimpleFeatureType

trait OrcStorageConfiguration extends StorageConfiguration {
  override def configureOutput(sft: SimpleFeatureType, job: Job): Unit = {
    job.setOutputFormatClass(classOf[OrcPartitionOutputFormat])
    StorageConfiguration.setSft(job.getConfiguration, sft)
    OrcSimpleFeatureOutputFormat.setDescription(job.getConfiguration,
      OrcFileSystemStorage.createTypeDescription(sft))
  }
}
