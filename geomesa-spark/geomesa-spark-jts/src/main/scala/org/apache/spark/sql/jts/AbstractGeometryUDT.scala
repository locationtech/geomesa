/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql.jts

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.locationtech.geomesa.spark.jts.util.{GMWKBUtils, WKBUtils}

import scala.reflect._

/**
 * Base class for all JTS UDTs, which get encoded in Catalyst as WKB blobs.
 * @param simpleString short name, like "point"
 * @tparam T Concrete JTS type represented by this UDT
 */
abstract class AbstractGeometryUDT[T >: Null <: Geometry: ClassTag](override val simpleString: String)
  extends UserDefinedType[T] {

  override def serialize(obj: T): InternalRow = {
    new GenericInternalRow(Array[Any](GMWKBUtils.write(obj)))
  }

  override def sqlType: DataType = AbstractGeometryUDT.dataType

  override def userClass: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  override def deserialize(datum: Any): T = {
    val ir = datum.asInstanceOf[InternalRow]
    GMWKBUtils.read(ir.getBinary(0)).asInstanceOf[T]
  }
}

object AbstractGeometryUDT {
  val dataType = StructType(Seq(
    StructField("wkb", DataTypes.BinaryType)
  ))
}
