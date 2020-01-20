/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.export

import com.beust.jcommander.Parameters
import org.apache.hadoop.mapreduce.Job
import org.geotools.data.Query
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.AccumuloDataStoreCommand.AccumuloDistributedCommand
import org.locationtech.geomesa.accumulo.tools.AccumuloDataStoreParams
import org.locationtech.geomesa.accumulo.tools.export.AccumuloExportCommand.AccumuloExportParams
import org.locationtech.geomesa.jobs.accumulo.AccumuloJobUtils
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaAccumuloInputFormat
import org.locationtech.geomesa.tools.export.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.tools.{OptionalIndexParam, RequiredTypeNameParam}

class AccumuloExportCommand extends ExportCommand[AccumuloDataStore] with AccumuloDistributedCommand {

  import scala.collection.JavaConverters._

  override val params = new AccumuloExportParams

  override protected def configure(job: Job, ds: AccumuloDataStore, query: Query): Unit =
    GeoMesaAccumuloInputFormat.configure(job, connection.asJava, AccumuloJobUtils.getSingleQueryPlan(ds, query))
}

object AccumuloExportCommand {
  @Parameters(commandDescription = "Export features from a GeoMesa data store")
  class AccumuloExportParams extends ExportParams with AccumuloDataStoreParams
      with RequiredTypeNameParam with OptionalIndexParam
}
