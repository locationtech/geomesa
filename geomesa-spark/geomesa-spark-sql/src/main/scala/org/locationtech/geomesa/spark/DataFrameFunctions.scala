/***********************************************************************
 * Copyright (c) 2019-2022 The MITRE Corporation
 * This program and the accompanying materials are made available under
 * the Apache License, Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 * This software was produced for the U. S. Government under Basic
 * Contract No. W56KGU-18-D-0004, and is subject to the Rights in
 * Noncommercial Computer Software and Noncommercial Computer Software
 * Documentation Clause 252.227-7014 (FEB 2012)
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.lang

import org.apache.spark.sql.{Column, Encoder, Encoders, TypedColumn}
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper._
import org.locationtech.jts.geom.Geometry
import org.locationtech.geomesa.spark.GeometricDistanceFunctions.{ST_Transform, distanceNames}


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
