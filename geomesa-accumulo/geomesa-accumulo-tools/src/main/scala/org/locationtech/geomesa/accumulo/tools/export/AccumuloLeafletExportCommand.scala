/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.export

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.export.{LeafletExportCommand, LeafletExportParams}
import org.locationtech.geomesa.tools.{OptionalIndexParam, RequiredTypeNameParam}

class AccumuloLeafletExportCommand extends LeafletExportCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new AccumuloLeafletExportParams
}

@Parameters(commandDescription = "Export features from a GeoMesa data store and render them in Leaflet")
class AccumuloLeafletExportParams extends LeafletExportParams with AccumuloDataStoreParams
    with RequiredTypeNameParam with OptionalIndexParam
