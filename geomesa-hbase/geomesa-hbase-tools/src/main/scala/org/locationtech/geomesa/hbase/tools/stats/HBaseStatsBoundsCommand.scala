/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.tools.stats.{StatsBoundsCommand, StatsBoundsParams}
import org.locationtech.geomesa.tools.{CatalogParam, RequiredTypeNameParam}

class HBaseStatsBoundsCommand extends StatsBoundsCommand[HBaseDataStore] with HBaseDataStoreCommand {
  override val params = new HBaseStatsBoundsParams
}

@Parameters(commandDescription = "View or calculate bounds on attributes in a GeoMesa feature type")
class HBaseStatsBoundsParams extends StatsBoundsParams with CatalogParam with RequiredTypeNameParam
