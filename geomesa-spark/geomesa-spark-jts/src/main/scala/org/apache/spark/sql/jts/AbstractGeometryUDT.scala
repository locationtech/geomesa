/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql.jts

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.locationtech.geomesa.spark.jts.util.WKBUtils
import org.locationtech.jts.geom.Geometry

import scala.reflect._

/**
 * Base class for all JTS UDTs, which get encoded in Catalyst as WKB blobs.
 * @param simpleString short name, like "point"
 * @tparam T Concrete JTS type represented by this UDT
 */
abstract class AbstractGeometryUDT[T >: Null <: Geometry: ClassTag](override val simpleString: String)
  extends UserDefinedType[T] {

  private val delegate = AbstractGeometryUDT.impl()

  override def pyUDT: String = {
    val pyUDT = delegate.pyUDT
    if (pyUDT.isEmpty) {
      // Use our own python bindings
      s"geomesa_pyspark.types.${getClass.getSimpleName}"
    } else {
      // Use python bindings provided by sedona package
      pyUDT
    }
  }

  override def serialize(obj: T): Any = delegate.serialize(obj)

  override def sqlType: DataType = delegate.sqlType

  override def userClass: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  override def deserialize(datum: Any): T = delegate.deserialize(datum).asInstanceOf[T]
}

object AbstractGeometryUDT {

  import org.locationtech.geomesa.spark.jts.useSedonaSerialization

  private val GeoMesaGeometryUDT = new GeometryUDTImpl()

  private lazy val SedonaGeometryUDT =
    org.locationtech.geomesa.spark.jts.SedonaGeometryUDT.getOrElse(GeoMesaGeometryUDT)

  private def impl(): UserDefinedType[Geometry] =
    if (useSedonaSerialization) { SedonaGeometryUDT } else { GeoMesaGeometryUDT }

  private class GeometryUDTImpl extends UserDefinedType[Geometry] {
    override def pyUDT: String = "" // handled by specific implementing classes

    override def serialize(obj: Geometry): InternalRow =
      new GenericInternalRow(Array[Any](WKBUtils.write(obj)))

    override def sqlType: DataType = StructType(Seq(
      StructField("wkb", DataTypes.BinaryType)
    ))

    override def userClass: Class[Geometry] = classOf[Geometry]

    override def deserialize(datum: Any): Geometry =
      WKBUtils.read(datum.asInstanceOf[InternalRow].getBinary(0))
  }
}
