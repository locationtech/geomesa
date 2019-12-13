/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

/**
  * Creates row key values from simple features
  *
  * @tparam U row key binding
  */
trait WriteConverter[U] {
  def convert(feature: WritableFeature, lenient: Boolean = false): RowKeyValue[U]
}

object WriteConverter {

  private val EmptyArray = Array.empty[Byte]

  /**
    * Creates row key values from simple features
    *
    * @param keySpace keyspace
    * @tparam U key binding value
    */
  class WriteConverterImpl[U](keySpace: IndexKeySpace[_, U]) extends WriteConverter[U] {
    override def convert(feature: WritableFeature, lenient: Boolean = false): RowKeyValue[U] =
      keySpace.toIndexKey(feature, EmptyArray, feature.id, lenient)
  }

  /**
    * Creates row key values from simple features for a tiered index
    *
    * @param keySpace keyspace
    * @param tieredKeySpace tiered keyspace
    * @tparam U key binding value
    */
  class TieredWriteConverter[U](keySpace: IndexKeySpace[_, U], tieredKeySpace: IndexKeySpace[_, _])
      extends WriteConverter[U] {
    override def convert(feature: WritableFeature, lenient: Boolean = false): RowKeyValue[U] = {
      val tier = tieredKeySpace.toIndexKey(feature, EmptyArray, EmptyArray, lenient) match {
        case kv: SingleRowKeyValue[_] => kv.row
        case kv => throw new IllegalArgumentException(s"Expected single row key from tiered keyspace but got: $kv")
      }
      keySpace.toIndexKey(feature, tier, feature.id, lenient)
    }
  }
}
