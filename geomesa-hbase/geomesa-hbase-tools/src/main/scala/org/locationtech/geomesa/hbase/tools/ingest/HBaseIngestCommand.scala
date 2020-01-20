/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.ingest

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.{HBaseParams, ToggleRemoteFilterParam}
import org.locationtech.geomesa.hbase.tools.ingest.HBaseIngestCommand.HBaseIngestParams
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams

// defined as a trait to allow us to mixin hbase/bigtable distributed classpaths
trait HBaseIngestCommand extends IngestCommand[HBaseDataStore] {
  override val params = new HBaseIngestParams()
}

object HBaseIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class HBaseIngestParams extends IngestParams with HBaseParams with ToggleRemoteFilterParam
}
