/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql.jts

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.jts.AbstractGeometryUDT.geometryUDTImpl
import org.apache.spark.sql.types._
import org.locationtech.geomesa.spark.isUsingSedona
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

  override def pyUDT: String = {
    val pyUDT = geometryUDTImpl.pyUDT
    if (pyUDT.isEmpty) {
      // Use our own python bindings
      s"geomesa_pyspark.types.${getClass.getSimpleName}"
    } else {
      // Use python bindings provided by sedona package
      pyUDT
    }
  }

  override def serialize(obj: T): Any = geometryUDTImpl.serialize(obj)

  override def sqlType: DataType = geometryUDTImpl.sqlType

  override def userClass: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  override def deserialize(datum: Any): T = geometryUDTImpl.deserialize(datum).asInstanceOf[T]
}

object AbstractGeometryUDT {
  lazy val geometryUDTImpl: UserDefinedType[Geometry] = {
    if (isUsingSedona) {
      Class.forName("org.apache.spark.sql.sedona_sql.UDT.GeometryUDT").newInstance().asInstanceOf[UserDefinedType[Geometry]]
    } else new AbstractGeometryUDTImpl()
  }

  class AbstractGeometryUDTImpl extends UserDefinedType[Geometry] {
    override def pyUDT: String = ""

    override def serialize(obj: Geometry): InternalRow = {
      new GenericInternalRow(Array[Any](WKBUtils.write(obj)))
    }

    override def sqlType: DataType = StructType(Seq(
      StructField("wkb", DataTypes.BinaryType)
    ))

    override def userClass: Class[Geometry] = classOf[Geometry]

    override def deserialize(datum: Any): Geometry = {
      val ir = datum.asInstanceOf[InternalRow]
      WKBUtils.read(ir.getBinary(0))
    }
  }
}
