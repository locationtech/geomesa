/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import org.apache.avro.generic.GenericRecord
import org.locationtech.geomesa.convert2.transforms.Expression.LiteralString
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{Expression, TransformerFunction, TransformerFunctionFactory}
import org.locationtech.geomesa.features.avro.serialization.AvroField.UuidBinaryField
import org.locationtech.geomesa.features.avro.serialization.CollectionSerialization

import java.nio.ByteBuffer

class AvroFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(avroPath, binaryList, binaryMap, binaryUuid)

  private val avroPath = new AvroPathFn(null)

  // parses a list encoded by the geomesa avro writer
  private val binaryList = TransformerFunction.pure("avroBinaryList") { args =>
    args(0) match {
<<<<<<< HEAD
      case bytes: Array[Byte] => CollectionSerialization.decodeList(ByteBuffer.wrap(bytes))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
      case bytes: Array[Byte] => AvroSimpleFeatureUtils.decodeList(ByteBuffer.wrap(bytes))
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
      case bytes: Array[Byte] => AvroSimpleFeatureUtils.decodeList(ByteBuffer.wrap(bytes))
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
      case null => null
      case arg => throw new IllegalArgumentException(s"Expected byte array but got: $arg")
    }
  }

  // parses a map encoded by the geomesa avro writer
  private val binaryMap = TransformerFunction.pure("avroBinaryMap") { args =>
    args(0) match {
<<<<<<< HEAD
      case bytes: Array[Byte] => CollectionSerialization.decodeMap(ByteBuffer.wrap(bytes))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
      case bytes: Array[Byte] => AvroSimpleFeatureUtils.decodeMap(ByteBuffer.wrap(bytes))
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
      case bytes: Array[Byte] => AvroSimpleFeatureUtils.decodeMap(ByteBuffer.wrap(bytes))
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
      case null => null
      case arg => throw new IllegalArgumentException(s"Expected byte array but got: $arg")
    }
  }

  // parses a uuid encoded by the geomesa avro writer
  private val binaryUuid = TransformerFunction.pure("avroBinaryUuid") { args =>
    args(0) match {
<<<<<<< HEAD
      case bytes: Array[Byte] => UuidBinaryField.decode(ByteBuffer.wrap(bytes))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
      case bytes: Array[Byte] => AvroSimpleFeatureUtils.decodeUUID(ByteBuffer.wrap(bytes))
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
      case bytes: Array[Byte] => AvroSimpleFeatureUtils.decodeUUID(ByteBuffer.wrap(bytes))
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
      case null => null
      case arg => throw new IllegalArgumentException(s"Expected byte array but got: $arg")
    }
  }

  class AvroPathFn(path: AvroPath) extends NamedTransformerFunction(Seq("avroPath"), pure = true) {

    override def getInstance(args: List[Expression]): AvroPathFn = {
      val path = args match {
        case _ :: LiteralString(s) :: _ => AvroPath(s)
        case _ => throw new IllegalArgumentException(s"Expected Avro path but got: ${args.headOption.orNull}")
      }
      new AvroPathFn(path)
    }

    override def apply(args: Array[AnyRef]): AnyRef =
      path.eval(args(0).asInstanceOf[GenericRecord]).orNull.asInstanceOf[AnyRef]
  }
}
