/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.data

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand, OptionalForceParam}
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand, OptionalForceParam}
import org.locationtech.geomesa.tools.utils.Prompt

trait DeleteCatalogCommand[DS <: GeoMesaDataStore[_, _, _]] extends DataStoreCommand[DS] {

  override val name = "delete-catalog"
  override def params: DeleteCatalogParams

  override def execute() = {
    if (params.force || Prompt.confirm(s"Delete catalog '${params.catalog}'? (yes/no): ")) {
      withDataStore(_.delete())
      println(s"Deleted catalog '${params.catalog}'")
    } else {
      logger.info("Cancelled")
    }
  }
}

@Parameters(commandDescription = "Delete a GeoMesa catalog completely (and all features in it)")
class DeleteCatalogParams extends CatalogParam with OptionalForceParam
