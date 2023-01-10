/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2019-2024 The MITRE Corporation
=======
<<<<<<< HEAD
 * Copyright (c) 2019-2023 The MITRE Corporation
=======
 * Copyright (c) 2019-2022 The MITRE Corporation
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
 * This program and the accompanying materials are made available under
 * the Apache License, Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 * This software was produced for the U. S. Government under Basic
 * Contract No. W56KGU-18-D-0004, and is subject to the Rights in
 * Noncommercial Computer Software and Noncommercial Computer Software
 * Documentation Clause 252.227-7014 (FEB 2012)
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.spark.sql.functions.udf
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders
import org.locationtech.jts.geom.{Geometry, LineString}


/**
 * User-facing module imports
 */
package object sql extends DataFrameFunctions.Library with SpatialEncoders {

  def st_distanceSpheroid = udf((g1: Geometry, g2: Geometry) =>
    GeometricDistanceFunctions.ST_DistanceSpheroid(g1, g2))

  def st_lengthSpheroid = udf((l1: LineString) =>
    GeometricDistanceFunctions.ST_LengthSpheroid(l1))

}
