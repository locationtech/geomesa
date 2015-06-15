/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.serialization

import java.util.Date

/** A collection of [[DatumWriter]]s for writing primitive-like datums.
  *
  */
trait PrimitiveWriter[Writer] {

  def writeString: DatumWriter[Writer, String]
  def writeInt: DatumWriter[Writer, Int]
  def writeLong: DatumWriter[Writer, Long]
  def writeFloat: DatumWriter[Writer, Float]
  def writeDouble: DatumWriter[Writer, Double]
  def writeBoolean: DatumWriter[Writer, Boolean]
  def writeDate: DatumWriter[Writer, Date]
  def writeBytes: DatumWriter[Writer, Array[Byte]]

  /** A [[DatumReader]] for writing an [[Int]] with positive optimization */
  def writePositiveInt: DatumWriter[Writer, Int]
}
