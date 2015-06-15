/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.serialization

import java.util.Date

/** A collection of [[DatumReader]]s for reading objects which may be null.
  *
  */
trait NullableReader[Reader] extends PrimitiveReader[Reader] with NullableCheck {

  /**
   * Read any value that may be null.
   */
  def readNullable[T](readRaw: DatumReader[Reader, T]): DatumReader[Reader, T]

  val readNullableInt: DatumReader[Reader, Int] = readNullable(readInt)
  val readNullableLong: DatumReader[Reader, Long] = readNullable(readLong)
  val readNullableFloat: DatumReader[Reader, Float] = readNullable(readFloat)
  val readNullableDouble: DatumReader[Reader, Double] = readNullable(readDouble)
  val readNullableBoolean: DatumReader[Reader, Boolean] = readNullable(readBoolean)
  val readNullableDate: DatumReader[Reader, Date] = readNullable(readDate)
}
