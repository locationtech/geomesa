/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2019-2024 The MITRE Corporation
=======
<<<<<<< HEAD
 * Copyright (c) 2019-2023 The MITRE Corporation
=======
 * Copyright (c) 2019-2022 The MITRE Corporation
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
 * This program and the accompanying materials are made available under
 * the Apache License, Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 * This software was produced for the U. S. Government under Basic
 * Contract No. W56KGU-18-D-0004, and is subject to the Rights in
 * Noncommercial Computer Software and Noncommercial Computer Software
 * Documentation Clause 252.227-7014 (FEB 2012)
 ***********************************************************************/

package org.locationtech.geomesa.spark.sql

import org.apache.spark.sql.{Column, Encoder, Encoders, TypedColumn}
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper._
import org.locationtech.geomesa.spark.sql.GeometricDistanceFunctions.{ST_Transform, distanceNames}
import org.locationtech.jts.geom.Geometry

import java.lang


/**
 * DataFrame DSL functions for working with GeoTools
 */
object DataFrameFunctions extends SpatialEncoders {

  implicit def integerEncoder: Encoder[Integer] = Encoders.INT
  implicit def doubleEncoder: Encoder[Double] = Encoders.scalaDouble
  implicit def jDoubleEncoder: Encoder[lang.Double] = Encoders.DOUBLE

  /**
   * Group of DataFrame DSL functions associated with determining the relationship
   * between geometries using GeoTools.
   */
  trait SpatialRelations {

    import org.locationtech.geomesa.spark.jts.udf.SpatialRelationFunctions._

    def st_distanceSpheroid(left: Column, right: Column): TypedColumn[Any, lang.Double] =
      udfToColumn(ST_DistanceSphere, relationNames, left, right)

    def st_lengthSphere(line: Column): TypedColumn[Any, lang.Double] =
      udfToColumn(ST_LengthSphere, relationNames, line)

    def st_transform(geom: Column, fromCRS: Column, toCRS: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_Transform, distanceNames, geom, fromCRS, toCRS)
  }

  /** Stack of all DataFrame DSL functions. */
  trait Library extends SpatialRelations
}
