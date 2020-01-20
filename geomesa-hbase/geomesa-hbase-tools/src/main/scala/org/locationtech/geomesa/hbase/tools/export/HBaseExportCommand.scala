/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.export

import com.beust.jcommander.Parameters
import org.apache.hadoop.mapreduce.Job
import org.geotools.data.Query
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.jobs.{GeoMesaHBaseInputFormat, HBaseJobUtils}
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.{HBaseParams, ToggleRemoteFilterParam}
import org.locationtech.geomesa.hbase.tools.export.HBaseExportCommand.HBaseExportParams
import org.locationtech.geomesa.tools.export.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.tools.{OptionalIndexParam, RequiredTypeNameParam}

// defined as a trait to allow us to mixin hbase/bigtable distributed classpaths
trait HBaseExportCommand extends ExportCommand[HBaseDataStore] {
  override val params = new HBaseExportParams

  override protected def configure(job: Job, ds: HBaseDataStore, query: Query): Unit =
    GeoMesaHBaseInputFormat.configure(job, HBaseJobUtils.getSingleScanPlan(ds, query))
}

object HBaseExportCommand {
  @Parameters(commandDescription = "Export features from a GeoMesa data store")
  class HBaseExportParams extends ExportParams with HBaseParams with RequiredTypeNameParam
      with OptionalIndexParam with ToggleRemoteFilterParam
}
