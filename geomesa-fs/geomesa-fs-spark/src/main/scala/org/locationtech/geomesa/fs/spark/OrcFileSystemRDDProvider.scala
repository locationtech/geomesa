/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.spark

import java.io.Serializable

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputFormat
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemStorageFactory
import org.locationtech.geomesa.fs.storage.orc.jobs.OrcSimpleFeatureInputFormat
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class OrcFileSystemRDDProvider extends AbstractFileSystemRDDProvider with LazyLogging {

  override def canProcess(params: java.util.Map[String, Serializable]): Boolean =
    new OrcFileSystemStorageFactory().canProcess(params)

  override protected def inputFormat: Class[_ <: InputFormat[Void, SimpleFeature]] =
    classOf[OrcSimpleFeatureInputFormat]

  override protected def configure(conf: Configuration,
                                   sft: SimpleFeatureType,
                                   query: Query): Unit = {
    OrcSimpleFeatureInputFormat.configure(conf, sft, query.getFilter, query.getPropertyNames)
  }
}
