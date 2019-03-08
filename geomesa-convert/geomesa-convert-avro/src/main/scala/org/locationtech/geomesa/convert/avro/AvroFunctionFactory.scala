/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import java.nio.ByteBuffer

import org.apache.avro.generic.GenericRecord
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{TransformerFunction, TransformerFunctionFactory}
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureUtils

class AvroFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(avroPath, binaryList, binaryMap, binaryUuid)

  private val avroPath = new AvroPathFn()

  // parses a list encoded by the geomesa avro writer
  private val binaryList = TransformerFunction.pure("avroBinaryList") { args =>
    AvroSimpleFeatureUtils.decodeList(ByteBuffer.wrap(args(0).asInstanceOf[Array[Byte]]))
  }

  // parses a map encoded by the geomesa avro writer
  private val binaryMap = TransformerFunction.pure("avroBinaryMap") { args =>
    AvroSimpleFeatureUtils.decodeMap(ByteBuffer.wrap(args(0).asInstanceOf[Array[Byte]]))
  }

  // parses a uuid encoded by the geomesa avro writer
  private val binaryUuid = TransformerFunction.pure("avroBinaryUuid") { args =>
    AvroSimpleFeatureUtils.decodeUUID(ByteBuffer.wrap(args(0).asInstanceOf[Array[Byte]]))
  }

  class AvroPathFn extends NamedTransformerFunction(Seq("avroPath"), pure = true) {
    private var path: AvroPath = _
    override def getInstance: AvroPathFn = new AvroPathFn()
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      if (path == null) {
        path = AvroPath(args(1).asInstanceOf[String])
      }
      path.eval(args(0).asInstanceOf[GenericRecord]).orNull
    }
  }
}
