/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.persistence

import org.locationtech.geomesa.utils.cache.FilePersistence
import org.vfny.geoserver.global.GeoserverDataDirectory

object PersistenceUtil extends
  FilePersistence(GeoserverDataDirectory.findCreateConfigDir("geomesa-config"), "geomesa-config.properties")
