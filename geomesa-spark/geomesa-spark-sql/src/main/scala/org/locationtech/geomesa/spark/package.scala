/***********************************************************************
 * Copyright (c) 2019-2020 The MITRE Corporation
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
package object geotools extends DataFrameFunctions.Library with SpatialEncoders {

  def st_distanceSpheroid = udf((g1: Geometry, g2: Geometry) =>
    GeometricDistanceFunctions.ST_DistanceSpheroid(g1, g2))

  def st_lengthSpheroid = udf((l1: LineString) =>
    GeometricDistanceFunctions.ST_LengthSpheroid(l1))

}
