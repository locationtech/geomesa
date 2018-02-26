/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.cassandra.data.CassandraDataStore
import org.locationtech.geomesa.cassandra.tools.{CassandraConnectionParams, CassandraDataStoreCommand}
import org.locationtech.geomesa.tools.export.{FileExportCommand, FileExportParams}
import org.locationtech.geomesa.tools.{CatalogParam, OptionalIndexParam, RequiredTypeNameParam}

class CassandraFileExportCommand extends FileExportCommand[CassandraDataStore] with CassandraDataStoreCommand {
  override val params = new CassandraFileExportParams
}

@Parameters(commandDescription = "Export features from a GeoMesa data store")
class CassandraFileExportParams extends FileExportParams with CassandraConnectionParams
    with CatalogParam with RequiredTypeNameParam with OptionalIndexParam
