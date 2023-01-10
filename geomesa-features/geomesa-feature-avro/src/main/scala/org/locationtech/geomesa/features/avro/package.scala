/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

package object avro {

  val AvroNamespace: String = "org.geomesa"

  object SerializationVersions {
    // Increment whenever encoding changes and handle in reader and writer
    // Version 2 changed the WKT geom to a binary geom
    // Version 3 adds byte array types to the schema...and is backwards compatible with V2
    // Version 4 adds a custom name encoder function for the avro schema
    //           v4 can read version 2 and 3 files but version 3 cannot read version 4
    // Version 5 changes user data to be a union type and fixes the coding to match the declared avro schema
    // Version 6 changes list and map types to be native avro collections instead of opaque binary
    val DefaultVersion         : Int = 5
    val NativeCollectionVersion: Int = 6
    val MaxVersion             : Int = 6
  }
}
