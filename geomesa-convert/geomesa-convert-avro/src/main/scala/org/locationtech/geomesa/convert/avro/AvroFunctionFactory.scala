/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import java.nio.ByteBuffer

import org.apache.avro.generic.GenericRecord
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.Expression.LiteralString
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{Expression, TransformerFunction, TransformerFunctionFactory}
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureUtils

class AvroFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(avroPath, binaryList, binaryMap, binaryUuid)

  private val avroPath = new AvroPathFn(null)

  // parses a list encoded by the geomesa avro writer
  private val binaryList = TransformerFunction.pure("avroBinaryList") { args =>
    args(0) match {
      case bytes: Array[Byte] => AvroSimpleFeatureUtils.decodeList(ByteBuffer.wrap(bytes))
      case null => null
      case arg => throw new IllegalArgumentException(s"Expected byte array but got: $arg")
    }
  }

  // parses a map encoded by the geomesa avro writer
  private val binaryMap = TransformerFunction.pure("avroBinaryMap") { args =>
    args(0) match {
      case bytes: Array[Byte] => AvroSimpleFeatureUtils.decodeMap(ByteBuffer.wrap(bytes))
      case null => null
      case arg => throw new IllegalArgumentException(s"Expected byte array but got: $arg")
    }
  }

  // parses a uuid encoded by the geomesa avro writer
  private val binaryUuid = TransformerFunction.pure("avroBinaryUuid") { args =>
    args(0) match {
      case bytes: Array[Byte] => AvroSimpleFeatureUtils.decodeUUID(ByteBuffer.wrap(bytes))
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
