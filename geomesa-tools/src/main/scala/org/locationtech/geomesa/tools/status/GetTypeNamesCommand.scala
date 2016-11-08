/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.status

import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.tools.DataStoreCommand

trait GetTypeNamesCommand[DS <: GeoMesaDataStore[_, _, _ ,_]] extends DataStoreCommand[DS] {

  override val name = "get-type-names"

  override def execute() = {
    println("Current features types:")
    withDataStore(_.getTypeNames.foreach(println))
  }
}
