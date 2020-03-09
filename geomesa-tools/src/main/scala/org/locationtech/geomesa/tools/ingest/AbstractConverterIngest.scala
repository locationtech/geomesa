/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import com.beust.jcommander.ParameterException
import org.geotools.data.{DataStore, DataStoreFinder, DataUtilities}
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.utils.StatusCallback
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

abstract class AbstractConverterIngest(dsParams: Map[String, String], sft: SimpleFeatureType) extends Runnable {

  import scala.collection.JavaConverters._

  override def run(): Unit = {
    // create schema for the feature prior to ingest
    val ds = DataStoreFinder.getDataStore(dsParams.asJava)
    try {
      val existing = Try(ds.getSchema(sft.getTypeName)).getOrElse(null)
      if (existing == null) {
        Command.user.info(s"Creating schema '${sft.getTypeName}'")
        ds.createSchema(sft)
      } else {
        Command.user.info(s"Schema '${sft.getTypeName}' exists")
        if (DataUtilities.compare(sft, existing) != 0) {
          throw new ParameterException("Existing simple feature type does not match expected type" +
              s"\n  existing: '${SimpleFeatureTypes.encodeType(existing)}'" +
              s"\n  expected: '${SimpleFeatureTypes.encodeType(sft)}'")
        }
      }
      runIngest(ds, sft, createCallback())
    } finally {
      ds.dispose()
    }
  }

  protected def runIngest(ds: DataStore, sft: SimpleFeatureType, callback: StatusCallback): Unit

  private def createCallback(): StatusCallback = StatusCallback()
}
