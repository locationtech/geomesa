/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.serialization

import java.util.Date

/** A collection of [[DatumReader]]s for reading primitive-like datums.
  *
  */
trait PrimitiveReader[Reader] {

  def readString: DatumReader[Reader, String]
  def readInt: DatumReader[Reader, Int]
  def readLong: DatumReader[Reader, Long]
  def readFloat: DatumReader[Reader, Float]
  def readDouble: DatumReader[Reader, Double]
  def readBoolean: DatumReader[Reader, Boolean]
  def readDate: DatumReader[Reader, Date]
  def readBytes: DatumReader[Reader, Array[Byte]]

  /** A [[DatumReader]] for reading an [[Int]] written with positive optimization. */
  def readPositiveInt: DatumReader[Reader, Int]
}
