/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.wfs

import org.geoserver.data.DataStoreFactoryInitializer
import org.geotools.data.DataAccessFactory

class AccumuloDataStoreFactoryInitializer(factoryClass: Class[DataAccessFactory])
    extends DataStoreFactoryInitializer[DataAccessFactory](factoryClass) {

  def initialize(factory: DataAccessFactory) {}
}
