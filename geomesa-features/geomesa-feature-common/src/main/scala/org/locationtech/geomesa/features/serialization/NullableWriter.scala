/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.serialization

import java.util.{Date, UUID}

/** A collection of [[DatumWriter]]s for writing objects which may be null.
  *
  */
trait NullableWriter[Writer] extends PrimitiveWriter[Writer] with NullableCheck {

  /**
   * Write any value that may be null.
   */
  def writeNullable[T](rawWrite: DatumWriter[Writer, T]): DatumWriter[Writer, T]

  val writeNullableInt: DatumWriter[Writer, Int] = writeNullable(writeInt)
  val writeNullableLong: DatumWriter[Writer, Long] = writeNullable(writeLong)
  val writeNullableFloat: DatumWriter[Writer, Float] = writeNullable(writeFloat)
  val writeNullableDouble: DatumWriter[Writer, Double] = writeNullable(writeDouble)
  val writeNullableBoolean: DatumWriter[Writer, Boolean] = writeNullable(writeBoolean)
  val writeNullableDate: DatumWriter[Writer, Date] = writeNullable(writeDate)
}

trait NullableCheck {

  type isNullableFn = (Class[_]) => Boolean

  val notNullable: isNullableFn = (_) => false

  /** Standard definition of nullable types.  Only applies when the type is known, for example, for attribute values.
    * Does not apply when the type is not know, for example, in a user data map, because when the type is unknown then
    * the class of the object has to be serialized which serves as a null value flag.
    */
  val standardNullable: isNullableFn = {
    case c if classOf[java.lang.Integer].isAssignableFrom(c) => true
    case c if classOf[java.lang.Long].isAssignableFrom(c) => true
    case c if classOf[java.lang.Float].isAssignableFrom(c) => true
    case c if classOf[java.lang.Double].isAssignableFrom(c) => true
    case c if classOf[java.lang.Boolean].isAssignableFrom(c) => true
    case c if classOf[Date].isAssignableFrom(c) => true
    case c if classOf[UUID].isAssignableFrom(c) => true
    case _ => false
  }
}
