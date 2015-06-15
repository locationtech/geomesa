/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features

/**
 * Options to be applied when encoding.  The same options must be specified when decoding.
 */
object SerializationOption extends Enumeration {
  type SerializationOption = Value

  /**
   * If this [[SerializationOption]] is specified then all user data of the simple feature will be
   * serialized and deserialized.
   */
  val WithUserData = Value


  implicit class SerializationOptions(val options: Set[SerializationOption]) extends AnyVal {

    /**
     * @param value the value to search for
     * @return true iff ``this`` contains the given ``value``
     */
    def contains(value: SerializationOption.Value) = options.contains(value)

    /** @return true iff ``this`` contains ``EncodingOption.WITH_USER_DATA`` */
    def withUserData: Boolean = options.contains(SerializationOption.WithUserData)
  }

  object SerializationOptions {

    /**
     * An empty set of encoding options.
     */
    val none: SerializationOptions = Set.empty[SerializationOption]

    /**
     * @return a new [[SerializationOptions]] containing just the ``EncodingOption.WITH_USER_DATA`` option
     */
    def withUserData: SerializationOptions = Set(SerializationOption.WithUserData)
  }
}

