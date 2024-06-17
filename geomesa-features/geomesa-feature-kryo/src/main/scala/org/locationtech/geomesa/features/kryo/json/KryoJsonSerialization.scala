/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.LazyLogging
import org.json4s.JsonAST._
import org.json4s.native.JsonMethods.{parse => _}
import org.locationtech.geomesa.features.kryo.json.JsonPathParser._

import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

/**
 * Serializes into bson (http://bsonspec.org/). Note this is a limited form of bson that only matches
 * the existing json types - does not cover the bson extensions like binary data, etc. Also note endianness,
 * etc might not match the spec 100%.
 *
 * The bson serialized value is preceded by a single byte, `\x00` to indicate null, or `\x01` to indicate non-null.
 * Additionally, non-document top-level values are supported, indicated with the prefix byte `\x02`. Top-level,
 * non-document values are encoded as a jsonb `element` with an empty name.
 *
 * Reduced BSON spec - only native JSON elements supported:
 *
 * byte    1 byte (8-bits)
 * int32   4 bytes (32-bit signed integer, two's complement)
 * int64   8 bytes (64-bit signed integer, two's complement)
 * double  8 bytes (64-bit IEEE 754-2008 binary floating point)
 *
 * document  ::= int32 e_list "\x00" BSON Document. int32 is the total number of bytes comprising the document.
 * e_list    ::= element e_list
 *           |	""
 * element   ::= "\x01" e_name double	64-bit binary floating point
 *           | "\x02" e_name string	UTF-8 string
 *           | "\x03" e_name document	Embedded document
 *           |	"\x04" e_name document	Array
 *           |	"\x08" e_name "\x00"	Boolean "false"
 *           |	"\x08" e_name "\x01"	Boolean "true"
 *           |	"\x09" e_name int64	UTC datetime
 *           |	"\x0A" e_name	Null value
 *           |	"\x10" e_name int32	32-bit integer
 *           |	"\x11" e_name int64	Timestamp
 *           |	"\x12" e_name int64	64-bit integer
 * e_name    ::= cstring	Key name
 * string    ::= int32 (byte*) "\x00"	String - The int32 is the number bytes in the (byte*) + 1 (for the trailing '\x00').
 *                                              The (byte*) is zero or more UTF-8 encoded characters.
 * cstring   ::= (byte*) "\x00"	Zero or more modified UTF-8 encoded characters followed by '\x00'. The (byte*)
 *                               MUST NOT contain '\x00', hence it is not full UTF-8.
 *
 * Note:
 *   Array - The document for an array is a normal BSON document with integer values for the keys,
 *   starting with 0 and continuing sequentially. For example, the array ['red', 'blue'] would be
 *   encoded as the document {'0': 'red', '1': 'blue'}. The keys must be in ascending numerical order.
 */
object KryoJsonSerialization extends LazyLogging {

  private[json] val TerminalByte :Byte = 0x00
  private[json] val DoubleByte   :Byte = 0x01
  private[json] val StringByte   :Byte = 0x02
  private[json] val DocByte      :Byte = 0x03
  private[json] val ArrayByte    :Byte = 0x04
  private[json] val BooleanByte  :Byte = 0x08
  private[json] val NullByte     :Byte = 0x0A
  private[json] val IntByte      :Byte = 0x10
  private[json] val LongByte     :Byte = 0x12

  private[json] val BooleanFalse :Byte = 0x00
  private[json] val BooleanTrue  :Byte = 0x01
  private[json] val NonDoc       :Byte = 0x02

  private val nameBuffers = new ThreadLocal[Array[Byte]] {
    override def initialValue(): Array[Byte] = Array.ofDim[Byte](32)
  }

  /**
    * Serialize a json object
    *
    * @param out output to write to
    * @param json json string to serialize - must be a json object
    */
  def serialize(out: Output, json: String): Unit = {
    import org.json4s._
    import org.json4s.native.JsonMethods._
    val obj = if (json == null) { null } else {
      try {
        parse(json)
      } catch {
        case NonFatal(e) =>
          logger.warn(s"Error parsing json:\n$json", e)
          null
      }
    }
    serialize(out, obj)
  }

  /**
    * Serialize a json object
    *
    * @param out output to write to
    * @param json object to serialize
    */
  def serialize(out: Output, json: JValue): Unit = {
    json match {
      case null | JNull  => out.write(BooleanFalse)
      case j: JObject    => out.write(BooleanTrue); writeDocument(out, j)
      case j             => out.write(NonDoc); writeValue(out, "", j)
    }
  }

  /**
    * Deserialize the given input. The input should be pointing to the start of
    * the bytes written by `serialize`. Upon completion, the input will be pointing
    * to the first byte after the bytes written by `serialize`.
    *
    * @param in input, pointing to the start of the json object
    * @return json as a string
    */
  def deserializeAndRender(in: Input): String = {
    import org.json4s.native.JsonMethods._
    val json = deserialize(in)
    if (json == null) {
      null
    } else {
      compact(render(json))
    }
  }

  /**
    * Deserialize the given input. The input should be pointing to the start of
    * the bytes written by `serialize`. Upon completion, the input will be pointing
    * to the first byte after the bytes written by `serialize`.
    *
    * @param in input, pointing to the start of the json object
    * @return parsed json object
    */
  def deserialize(in: Input): JValue = {
    try {
      in.readByte match {
        case BooleanFalse => null
        case BooleanTrue  => readDocument(in: Input)
        case NonDoc       => readValue(in)._2
      }
    } catch {
      case NonFatal(e) => logger.error("Error reading serialized kryo json", e); null
    }
  }

  /**
    * Deserialize the result of a json-path. The input should be pointing to the start of
    * the bytes written by `serialize`. There is no guarantee where the input will be
    * pointing after completion.
    *
    * If the path selects leaf elements, they will be returned as primitives. If the path
    * selects objects, they will be returned as json strings. If more than one item is
    * selected, they will be returned in a java.util.List. If nothing is selected, it will return null.
    *
    * @param in input, pointing to the start of the json object
    * @param path pre-parsed json path to evaluate
    * @return result of the path, if any
    */
  def deserialize(in: Input, path: JsonPath): Any = {
    if (path.isEmpty) {
      deserializeAndRender(in)
    } else {
      try { new KryoJsonPath(in).deserialize(path) } catch {
        case NonFatal(e) => logger.error("Error reading serialized kryo json", e); null
      }
    }
  }

  // primitive writing functions - in general will write a byte identifying the type, the key and then the value

  private def writeDocument(out: Output, name: String, value: JObject): Unit = {
    out.writeByte(DocByte)
    out.writeName(name)
    writeDocument(out, value)
  }

  // write a document without a name - used for the outer-most object which doesn't have a key
  private def writeDocument(out: Output, value: JObject): Unit = {
    val start = out.position()
    // write a placeholder that we will overwrite when we go back to write total length
    // note: don't just modify position, as that doesn't expand the buffer correctly
    out.writeInt(0)
    value.obj.foreach { case (name, elem) => writeValue(out, name, elem) }
    out.writeByte(TerminalByte) // marks the end of our object
    // go back and write the total length
    val end = out.position()
    out.setPosition(start)
    out.writeInt(end - start)
    out.setPosition(end)
  }

  private def writeValue(out: Output, name: String, value: JValue): Unit = {
    value match {
      case v: JString  => writeString(out, name, v)
      case v: JObject  => writeDocument(out, name, v)
      case v: JArray   => writeArray(out, name, v)
      case v: JDouble  => writeDouble(out, name, v)
      case v: JInt     => writeInt(out, name, v)
      case v: JLong    => writeLong(out, name, v)
      case JNull       => writeNull(out, name)
      case v: JBool    => writeBoolean(out, name, v)
      case v: JDecimal => writeDecimal(out, name, v)
    }
  }

  private def writeArray(out: Output, name: String, value: JArray): Unit = {
    out.writeByte(ArrayByte)
    out.writeName(name)
    // we store as an object where array index is the key
    var i = -1
    val withKeys = value.arr.map { element => i += 1; (i.toString, element) } // note: side-effect in map
    writeDocument(out, JObject(withKeys))
  }

  private def writeString(out: Output, name: String, value: JString): Unit = {
    out.writeByte(StringByte)
    out.writeName(name)
    val bytes = value.values.getBytes(StandardCharsets.UTF_8)
    out.writeInt(bytes.length)
    out.write(bytes)
    out.writeByte(TerminalByte)
  }

  private def writeDecimal(out: Output, name: String, value: JDecimal): Unit = {
    out.writeByte(DoubleByte)
    out.writeName(name)
    out.writeDouble(value.values.toDouble)
  }

  private def writeDouble(out: Output, name: String, value: JDouble): Unit = {
    out.writeByte(DoubleByte)
    out.writeName(name)
    out.writeDouble(value.values)
  }

  private def writeInt(out: Output, name: String, value: JInt): Unit = {
    if (value.values.isValidInt) {
      out.writeByte(IntByte)
      out.writeName(name)
      out.writeInt(value.values.intValue)
    } else if (value.values.isValidLong) {
      out.writeByte(LongByte)
      out.writeName(name)
      out.writeLong(value.values.longValue)
    } else {
      logger.warn(s"Skipping int value that does not fit in a long: $value")
    }
  }

  private def writeLong(out: Output, name: String, value: JLong): Unit = {
    out.writeByte(LongByte)
    out.writeName(name)
    out.writeLong(value.values)
  }

  private def writeBoolean(out: Output, name: String, v: JBool): Unit = {
    out.writeByte(BooleanByte)
    out.writeName(name)
    out.writeByte(if (v.values) BooleanTrue else BooleanFalse)
  }

  private def writeNull(out: Output, name: String): Unit = {
    out.writeByte(NullByte)
    out.writeName(name)
  }

  // primitive reading/skipping methods corresponding to the write methods above
  // assumes that the indicator byte and name have already been read

  private[json] def readDocument(in: Input): JObject = {
    val end = in.position() + in.readInt() - 1 // last byte is the terminal byte
    val elements = scala.collection.mutable.ArrayBuffer.empty[JField]
    while (in.position() < end) {
      elements.append(readValue(in))
    }
    in.skip(1) // skip over terminal byte
    JObject(elements.toList)
  }

  private[json] def readValue(in: Input): JField = {
    val switch = in.readByte()
    val name = in.readName()
    val value = switch match {
      case StringByte   => JString(readString(in))
      case DocByte      => readDocument(in)
      case ArrayByte    => readArray(in)
      case DoubleByte   => JDouble(in.readDouble())
      case IntByte      => JInt(in.readInt())
      case LongByte     => JLong(in.readLong())
      case NullByte     => JNull
      case BooleanByte  => JBool(readBoolean(in))
    }
    JField(name, value)
  }

  private[json] def readArray(in: Input): JArray = JArray(readDocument(in).obj.map(_._2))

  private[json] def skipDocument(in: Input): Unit = in.skip(in.readInt - 4) // length includes bytes storing length

  private[json] def readString(in: Input): String = {
    val bytes = Array.ofDim[Byte](in.readInt())
    in.read(bytes)
    in.skip(1) // skip TerminalByte
    new String(bytes, StandardCharsets.UTF_8)
  }

  private[json] def skipString(in: Input): Unit = in.skip(in.readInt() + 1) // skip TerminalByte

  private[json] def readBoolean(in: Input): Boolean = in.readByte == BooleanTrue

  private[json] def skipBoolean(in: Input): Unit = in.skip(1)

  private[json] implicit class RichOutput(val out: Output) extends AnyRef {
    def writeName(name: String): Unit = {
      // note: names are not allowed to contain the terminal byte (0x00) but we don't check for it
      out.write(name.getBytes(StandardCharsets.UTF_8))
      out.writeByte(TerminalByte)
    }
  }

  private[json] implicit class RichInput(val in: Input) extends AnyRef {
    def readName(): String = {
      var buffer = nameBuffers.get()
      var i = 0
      var byte: Byte = in.readByte()
      while (byte != TerminalByte) {
        if (i == buffer.length) {
          // expand our cached buffer to accommodate the name
          val copy = Array.ofDim[Byte](buffer.length * 2)
          System.arraycopy(buffer, 0, copy, 0, i)
          buffer = copy
          nameBuffers.set(buffer)
        }
        buffer(i) = byte
        i += 1
        byte = in.readByte()
      }
      new String(buffer, 0, i, StandardCharsets.UTF_8)
    }

    def skipName(): Unit = while (in.readByte() != TerminalByte) {}
  }
}
