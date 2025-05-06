/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import com.google.gson.{GsonBuilder, JsonArray, JsonElement, JsonNull, JsonObject, JsonPrimitive}
import org.apache.avro.generic.GenericRecord
import org.locationtech.geomesa.convert2.transforms.Expression.LiteralString
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{Expression, TransformerFunction, TransformerFunctionFactory}
import org.locationtech.geomesa.features.avro.serialization.AvroField.UuidBinaryField
import org.locationtech.geomesa.features.avro.serialization.CollectionSerialization

import java.nio.ByteBuffer
import java.util.UUID

class AvroFunctionFactory extends TransformerFunctionFactory {

  import scala.collection.JavaConverters._

  override def functions: Seq[TransformerFunction] = Seq(avroPath, binaryList, binaryMap, binaryUuid, avroToJson)

  private val gson = new GsonBuilder().disableHtmlEscaping().create()

  private val avroPath = new AvroPathFn(null)

  // parses a list encoded by the geomesa avro writer
  private val binaryList = TransformerFunction.pure("avroBinaryList") { args =>
    args(0) match {
      case bytes: Array[Byte] => CollectionSerialization.decodeList(ByteBuffer.wrap(bytes))
      case null => null
      case arg => throw new IllegalArgumentException(s"Expected byte array but got: $arg")
    }
  }

  // parses a map encoded by the geomesa avro writer
  private val binaryMap = TransformerFunction.pure("avroBinaryMap") { args =>
    args(0) match {
      case bytes: Array[Byte] => CollectionSerialization.decodeMap(ByteBuffer.wrap(bytes))
      case null => null
      case arg => throw new IllegalArgumentException(s"Expected byte array but got: $arg")
    }
  }

  // parses a uuid encoded by the geomesa avro writer
  private val binaryUuid = TransformerFunction.pure("avroBinaryUuid") { args =>
    args(0) match {
      case bytes: Array[Byte] => UuidBinaryField.decode(ByteBuffer.wrap(bytes))
      case uuid: UUID => uuid
      case null => null
      case arg => throw new IllegalArgumentException(s"Expected byte array but got: $arg")
    }
  }

  private val avroToJson = TransformerFunction.pure("avroToJson") { args =>
    args(0) match {
      case null => null
      case a => gson.toJson(toJson(a))
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

  private def toJson(arg: Any): JsonElement = {
    arg match {
      case s: String => new JsonPrimitive(s)
      case n: Number => new JsonPrimitive(n)
      case b: Boolean => new JsonPrimitive(b)
      case r: GenericRecord =>
        val obj = new JsonObject()
        var i = 0
        r.getSchema.getFields.asScala.foreach { field =>
          val value = toJson(r.get(i))
          if (!value.isJsonNull) {
            obj.add(field.name(), value)
          }
          i += 1
        }
        obj
      case list: java.util.List[_] =>
        val array = new JsonArray(list.size())
        list.asScala.foreach { elem =>
          val json = toJson(elem)
          if (!json.isJsonNull) {
            array.add(json)
          }
        }
        array
      case m: java.util.Map[_, _] =>
        val obj = new JsonObject()
        m.asScala.foreach { case (k, v) =>
          if (k != null) {
            val value = toJson(v)
            if (!value.isJsonNull) {
              obj.add(k.toString, value)
            }
          }
        }
        obj
      case null => JsonNull.INSTANCE
      case _ => new JsonPrimitive(arg.toString)
    }
  }

}
