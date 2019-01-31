/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.data

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.tools.{CatalogParam, Command, DataStoreCommand, OptionalForceParam}

trait DeleteCatalogCommand[DS <: GeoMesaDataStore[DS]] extends DataStoreCommand[DS] {

  override val name = "delete-catalog"
  override def params: DeleteCatalogParams

  override def execute(): Unit = {
    if (params.force || Prompt.confirm(s"Delete catalog '${params.catalog}'? (yes/no): ")) {
      withDataStore(_.delete())
      Command.user.info(s"Deleted catalog '${params.catalog}'")
    } else {
      Command.user.info("Cancelled")
    }
  }
}

@Parameters(commandDescription = "Delete a GeoMesa catalog completely (and all features in it)")
class DeleteCatalogParams extends CatalogParam with OptionalForceParam
