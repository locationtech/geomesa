/***********************************************************************
 * Copyright (c) 2019 The MITRE Corporation
 * This software was produced for the U. S. Government under Basic
 * Contract No. W56KGU-18-D-0004, and is subject to the Rights in
 * Noncommercial Computer Software and Noncommercial Computer
 * Software Documentation Clause 252.227-7014 (FEB 2012).
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
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
