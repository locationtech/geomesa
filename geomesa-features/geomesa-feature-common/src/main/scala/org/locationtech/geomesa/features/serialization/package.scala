/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features

package object serialization {

  type Version = Int

  // Write a datum.
  type DatumWriter[Writer, -T] = (Writer, T) => Unit

  // Read a datum.
  type DatumReader[Reader, +T] = (Reader) => T
}

class SerializationException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause)
