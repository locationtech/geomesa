/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor

/**
 * Kept around for compatibility with existing tables configured with geomesa 2.4 or earlier
 */
@deprecated("Replaced with org.locationtech.geomesa.hbase.server.coprocessor.GeoMesaCoprocessor")
class GeoMesaCoprocessor extends org.locationtech.geomesa.hbase.server.coprocessor.GeoMesaCoprocessor
