/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.spark

import java.io.Serializable
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.parquet.hadoop.ParquetInputFormat
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.parquet._
import org.locationtech.geomesa.parquet.jobs.{ParquetSimpleFeatureInputFormat, SimpleFeatureReadSupport}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class ParquetFileSystemRDDProvider extends AbstractFileSystemRDDProvider with LazyLogging {

  override def canProcess(params: util.Map[String, Serializable]): Boolean =
    new ParquetFileSystemStorageFactory().canProcess(params)

  override protected def inputFormat: Class[_ <: InputFormat[Void, SimpleFeature]] =
    classOf[ParquetSimpleFeatureInputFormat]

  override protected def configure(conf: Configuration,
                                   origSft: SimpleFeatureType,
                                   query: Query): Unit = {
    import org.locationtech.geomesa.index.conf.QueryHints._

    QueryPlanner.setQueryTransforms(query, origSft)
    val sft = query.getHints.getTransformSchema.getOrElse(origSft)

    StorageConfiguration.setSft(conf, sft)

    // Need this for query planning
    conf.set("parquet.filter.dictionary.enabled", "true")

    // push-down Parquet predicates and remaining gt-filter
    val (parquetFilter, modifiedGT) = new FilterConverter(origSft).convert(query.getFilter)
    parquetFilter.foreach(ParquetInputFormat.setFilterPredicate(conf, _))
    ParquetSimpleFeatureInputFormat.setGeoToolsFilter(conf, modifiedGT)

    // @see org.apache.parquet.hadoop.ParquetInputFormat.setReadSupportClass(org.apache.hadoop.mapred.JobConf, java.lang.Class<?>)
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[SimpleFeatureReadSupport].getName)
  }
}
