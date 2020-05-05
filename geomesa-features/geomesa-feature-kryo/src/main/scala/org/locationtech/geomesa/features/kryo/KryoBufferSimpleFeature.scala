/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo

import com.esotericsoftware.kryo.io.{Input, Output}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature.{KryoBufferV3, _}
import org.locationtech.geomesa.features.kryo.impl.KryoFeatureDeserialization.KryoLongReader
import org.locationtech.geomesa.features.kryo.impl.{KryoFeatureDeserialization, KryoFeatureDeserializationV2}
import org.locationtech.geomesa.features.kryo.serialization.KryoUserDataSerialization
import org.locationtech.geomesa.utils.collection.IntBitSet
import org.locationtech.geomesa.utils.geotools.Transform.{PropertyTransform, RenameTransform, Transforms}
import org.locationtech.geomesa.utils.geotools.{ImmutableFeatureId, Transform}
import org.locationtech.geomesa.utils.kryo.NonMutatingInput
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.`type`.{AttributeDescriptor, Name}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.feature.{GeometryAttribute, Property}
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.BoundingBox

class KryoBufferSimpleFeature(serializer: KryoFeatureDeserialization) extends SimpleFeature {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private val input = new NonMutatingInput()

  private val idParser = if (serializer.withoutId) { new IdParser() } else { new WithIdParser() }
  private val delegateV3 = new KryoBufferV3(serializer, input)

  @volatile
  private var seenV2 = false
  private lazy val delegateV2 = {
    seenV2 = true
    val buf = new KryoBufferV2(serializer, input)
    if (transforms != null) {
      buf.setTransforms(transformSchema, transformDefinitions)
    }
    buf
  }

  private lazy val geomIndex = serializer.out.getGeomIndex

  private var delegate: KryoBufferDelegate = _

  private var transforms: String = _
  private var transformSchema: SimpleFeatureType = _
  private var transformDefinitions: Array[Transform] = _

  private var userData: java.util.Map[AnyRef, AnyRef] = _

  /**
    * Creates a new feature for later use - does not copy attribute bytes
    *
    * @return
    */
  def copy(): KryoBufferSimpleFeature = {
    val sf = new KryoBufferSimpleFeature(serializer)
    sf.setIdParser(idParser.parse)
    if (transforms != null) {
      sf.setTransforms(transforms, transformSchema)
    }
    sf
  }

  /**
    * Sets the parser for reading feature ids out of the id buffer
    *
    * @param parse parse method
    */
  def setIdParser(parse: (Array[Byte], Int, Int) => String): Unit = idParser.parse = parse

  /**
    * Sets the transform to be applied to this feature when calling `transform`
    *
    * @param transforms transform definition, per geotools format
    * @param transformSchema schema that results from applying the transform
    */
  def setTransforms(transforms: String, transformSchema: SimpleFeatureType): Unit = {
    this.transforms = transforms
    this.transformSchema = transformSchema
    this.transformDefinitions = Transforms(serializer.out, transforms).toArray
    delegateV3.setTransforms(transformSchema, transformDefinitions)
    if (seenV2) {
      delegateV2.setTransforms(transformSchema, transformDefinitions)
    }
  }

  /**
    * Gets any transforms applied to this feature
    *
    * @return
    */
  def getTransform: Option[(String, SimpleFeatureType)] =
    for { t <- Option(transforms); s <- Option(transformSchema) } yield { (t, s) }

  /**
    * Set the serialized bytes to use for reading attributes
    *
    * @param bytes serialized byte array
    */
  def setBuffer(bytes: Array[Byte]): Unit = setBuffer(bytes, 0, bytes.length)

  /**
    * Set the serialized bytes to use for reading attributes
    *
    * @param bytes serialized byte array
    * @param offset offset into the byte array of valid bytes
    * @param length number of valid bytes to read from the byte array
    */
  def setBuffer(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    input.setBuffer(bytes, offset, length)
    delegate = input.readByte() match {
      case KryoFeatureSerializer.Version3 => delegateV3
      case KryoFeatureSerializer.Version2 => delegateV2
      case b => throw new IllegalArgumentException(s"Can't process features serialized with version: $b")
    }
    delegate.reset()
    userData = null
  }

  /**
    * Sets the serialized bytes containing the feature ID (i.e. the row key)
    *
    * @param bytes bytes
    */
  def setIdBuffer(bytes: Array[Byte]): Unit = setIdBuffer(bytes, 0, bytes.length)

  /**
    * Sets the serialized bytes containing the feature ID (i.e. the row key)
    *
    * @param bytes bytes
    * @param offset offset into the byte array of valid bytes
    * @param length number of valid bytes to read from the byte array
    */
  def setIdBuffer(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    idParser.buffer = bytes
    idParser.offset = offset
    idParser.length = length
  }

  /**
    * Transform the feature into a serialized byte array
    *
    * @return
    */
  def transform(): Array[Byte] = delegate.transform(this)

  /**
    * Get a date attribute as a raw long
    *
    * @param index attribute index
    * @return
    */
  def getDateAsLong(index: Int): Long = delegate.getDateAsLong(index)

  /**
    * Get the underlying kryo input, positioned to read the attribute at the given index
    *
    * @param index attribute index
    * @return input, if the attribute is not null
    */
  def getInput(index: Int): Option[Input] = delegate.getInput(index)

  override def getAttribute(index: Int): AnyRef = delegate.getAttribute(index)

  override def getType: SimpleFeatureType = serializer.out
  override def getFeatureType: SimpleFeatureType = serializer.out
  override def getName: Name = serializer.out.getName

  override def getID: String = idParser.id()
  override def getIdentifier: FeatureId = new ImmutableFeatureId(idParser.id())

  override def getAttribute(name: Name): AnyRef = getAttribute(name.getLocalPart)
  override def getAttribute(name: String): Object = {
    val index = serializer.out.indexOf(name)
    if (index == -1) { null } else { getAttribute(index) }
  }

  override def getDefaultGeometry: AnyRef = if (geomIndex == -1) { null } else { getAttribute(geomIndex) }
  override def getAttributeCount: Int = serializer.out.getAttributeCount

  override def getBounds: BoundingBox = getDefaultGeometry match {
    case g: Geometry => new ReferencedEnvelope(g.getEnvelopeInternal, serializer.out.getCoordinateReferenceSystem)
    case _           => new ReferencedEnvelope(serializer.out.getCoordinateReferenceSystem)
  }

  override def getAttributes: java.util.List[AnyRef] = {
    val attributes = new java.util.ArrayList[AnyRef](serializer.out.getAttributeCount)
    var i = 0
    while (i < serializer.out.getAttributeCount) {
      attributes.add(getAttribute(i))
      i += 1
    }
    attributes
  }

  override def getUserData: java.util.Map[AnyRef, AnyRef] = {
    if (userData == null) {
      userData = if (serializer.withoutUserData) { new java.util.HashMap(1) } else { delegate.getUserData }
    }
    userData
  }

  override def getDefaultGeometryProperty: GeometryAttribute = throw new NotImplementedError
  override def getProperties: java.util.Collection[Property] = throw new NotImplementedError
  override def getProperties(name: Name): java.util.Collection[Property] = throw new NotImplementedError
  override def getProperties(name: String): java.util.Collection[Property] = throw new NotImplementedError
  override def getProperty(name: Name): Property = throw new NotImplementedError
  override def getProperty(name: String): Property = throw new NotImplementedError
  override def getValue: java.util.Collection[_ <: Property] = throw new NotImplementedError
  override def getDescriptor: AttributeDescriptor = throw new NotImplementedError

  override def setAttribute(name: Name, value: Object): Unit = throw new NotImplementedError
  override def setAttribute(name: String, value: Object): Unit = throw new NotImplementedError
  override def setAttribute(index: Int, value: Object): Unit = throw new NotImplementedError
  override def setAttributes(vals: java.util.List[Object]): Unit = throw new NotImplementedError
  override def setAttributes(vals: Array[Object]): Unit = throw new NotImplementedError
  override def setDefaultGeometry(geo: Object): Unit = throw new NotImplementedError
  override def setDefaultGeometryProperty(geoAttr: GeometryAttribute): Unit = throw new NotImplementedError
  override def setValue(newValue: Object): Unit = throw new NotImplementedError
  override def setValue(values: java.util.Collection[Property]): Unit = throw new NotImplementedError

  override def isNillable: Boolean = true
  override def validate(): Unit = throw new NotImplementedError

  override def toString: String = s"KryoBufferSimpleFeature:$getID"

  private class WithIdParser extends IdParser {
    override def id(): String = delegate.id()
  }
}

object KryoBufferSimpleFeature {

  private class IdParser {
    var parse: (Array[Byte], Int, Int) => String = (_, _, _) => null
    var buffer: Array[Byte] = _
    var offset: Int = 0
    var length: Int = 0

    def id(): String = parse(buffer, offset, length)
  }

  /**
    * Common interface for handling serialization versions
    */
  private sealed trait KryoBufferDelegate extends Transformer {

    /**
      * Invoked after the underlying kryo buffer has been updated with a new serialized feature
      */
    def reset(): Unit

    /**
      * Sets the transform to be applied to the feature
      *
      * @param schema schema that results from applying the transform
      * @param transforms transform definitions
      */
    def setTransforms(schema: SimpleFeatureType, transforms: Array[Transform]): Unit

    /**
      * Deserialize the feature ID (assuming options.withId)
      *
      * @return
      */
    def id(): String

    /**
      * Deserialize an attribute
      *
      * @param index attribute number
      * @return
      */
    def getAttribute(index: Int): AnyRef

    /**
      * Deserialize user data
      *
      * @return
      */
    def getUserData: java.util.Map[AnyRef, AnyRef]

    /**
      * Optimized method to get a date attribute as millis without creating a new Date object
      *
      * @param index attribute number
      * @return
      */
    def getDateAsLong(index: Int): Long

    /**
      * Gets the input, positioned to read the given attribute
      *
      * @param index attribute index
      * @return
      */
    def getInput(index: Int): Option[Input]
  }

  /**
    * Abstraction over transformer impls
    */
  private sealed trait Transformer {

    /**
      * Transform a feature, based on previously set transform schema
      *
      * @param original original feature being transformed
      * @return
      */
    def transform(original: SimpleFeature): Array[Byte]
  }

  /**
    * Serialization version 3 delegate
    *
    * @param serializer serializer
    * @param input input
    */
  private class KryoBufferV3(serializer: KryoFeatureDeserialization, input: Input) extends KryoBufferDelegate {

    private var metadata: Metadata = _
    private var transformer: Transformer = _

    override def reset(): Unit = metadata = Metadata(input) // reads count, size, nulls, etc

    override def setTransforms(schema: SimpleFeatureType, transforms: Array[Transform]): Unit = {
      val indices = transforms.map {
        case t: PropertyTransform => t.i
        case t: RenameTransform => t.i
        case _ => -1
      }
      if (indices.contains(-1)) {
        transformer = new ReserializeTransformer(schema, transforms, serializer.options)
      } else {
        transformer = new BinaryTransformer(indices)
      }
    }

    override def id(): String = {
      metadata.setIdPosition()
      input.readString()
    }

    override def getAttribute(index: Int): AnyRef = {
      if (index >= metadata.count || metadata.nulls.contains(index)) { null } else {
        metadata.setPosition(index)
        serializer.readers(index).apply(input)
      }
    }

    override def getUserData: java.util.Map[AnyRef, AnyRef] = {
      metadata.setUserDataPosition()
      KryoUserDataSerialization.deserialize(input)
    }

    override def getDateAsLong(index: Int): Long = {
      if (index >= metadata.count || metadata.nulls.contains(index)) { 0L } else {
        metadata.setPosition(index)
        KryoLongReader.apply(input)
      }
    }

    override def getInput(index: Int): Option[Input] = {
      if (index >= metadata.count || metadata.nulls.contains(index)) { None } else {
        metadata.setPosition(index)
        Some(input)
      }
    }

    override def transform(original: SimpleFeature): Array[Byte] = transformer.transform(original)

    // if we are just returning a subset of attributes, we can copy the bytes directly
    private class BinaryTransformer(indices: Array[Int]) extends Transformer {

      private val positionsAndLengths = Array.ofDim[(Int, Int)](indices.length)

      override def transform(original: SimpleFeature): Array[Byte] = {
        // +1 for version, +2 for count, +1 for size
        var length = (metadata.size * (indices.length + 1)) + (IntBitSet.size(indices.length) * 4) + 4
        val id = if (serializer.withoutId) { null } else {
          val pos = metadata.setIdPosition() + metadata.offset
          val id = input.readString()
          length += input.position() - pos
          id
        }

        val nulls = IntBitSet(indices.length)

        // track the write position for copying attributes in the transformed array
        var resultCursor = length

        var i = 0
        while (i < indices.length) {
          val index = indices(i) // the index of the attribute in the original feature
          if (index >= metadata.count || metadata.nulls.contains(index)) { nulls.add(i) } else {
            // read the offset and the subsequent offset to get the length
            val pos = metadata.setPosition(index)
            val len = metadata.setPosition(index + 1) - pos
            length += len
            positionsAndLengths(i) = (metadata.offset + pos, len)
          }
          i += 1
        }

        // note: the input buffer is the raw buffer. we need to ensure that we use the
        // offset into the raw buffer rather than the raw buffer directly
        val buf = input.getBuffer

        val result = Array.ofDim[Byte](length)
        val output = new Output(result, length)
        output.writeByte(KryoFeatureSerializer.Version3)
        output.writeShort(indices.length) // track the number of attributes
        output.write(metadata.size)
        i = 0
        while (i < positionsAndLengths.length) {
          // note: offset is always 4 here since we're using the full result array
          if (metadata.size == 2) {
            output.writeShort(resultCursor - 4)
          } else {
            output.writeInt(resultCursor - 4)
          }
          if (!nulls.contains(i)) {
            val (pos, len) = positionsAndLengths(i)
            System.arraycopy(buf, pos, result, resultCursor, len)
            resultCursor += len
          }
          i += 1
        }
        // user data offset
        if (metadata.size == 2) {
          output.writeShort(resultCursor)
        } else {
          output.writeInt(resultCursor)
        }

        // TODO user data?

        // write out nulls
        nulls.serialize(output)

        // we're already positioned to write out the feature id
        if (id != null) {
          output.writeString(id)
        }

        result
      }
    }
  }

  /**
    * Serialiation version 2 delegate
    *
    * @param serializer serializer
    * @param input input
    */
  private class KryoBufferV2(serializer: KryoFeatureDeserialization, input: Input) extends KryoBufferDelegate {

    private val offsets = Array.ofDim[Int](serializer.out.getAttributeCount)
    private var offset: Int = -1
    private var startOfOffsets: Int = -1
    private var missingAttributes: Boolean = false
    private var userDataOffset: Int = -1

    private var reserializeTransform: Transformer = _
    private var binaryTransform: Transformer = _

    override def reset(): Unit = {
      // reset our offsets
      offset = input.position() - 1 // we've already read the version byte
      startOfOffsets = offset + input.readInt()
      input.setPosition(startOfOffsets) // set to offsets start
      var i = 0
      while (i < offsets.length && input.position < input.limit) {
        offsets(i) = offset + input.readInt(true)
        i += 1
      }
      if (i < offsets.length) {
        // attributes have been added to the sft since this feature was serialized
        missingAttributes = true
        do { offsets(i) = -1; i += 1 } while (i < offsets.length)
      } else {
        missingAttributes = false
      }
      userDataOffset = input.position()
    }

    override def setTransforms(schema: SimpleFeatureType, transforms: Array[Transform]): Unit = {
      val indices = transforms.map {
        case t: PropertyTransform => t.i
        case t: RenameTransform => t.i
        case _ => -1
      }

      // transforms by evaluating the transform expressions and then serializing the resulting feature
      // we use this for transform expressions and for data that was written using an old schema
      reserializeTransform = new ReserializeTransformer(schema, transforms, serializer.options)
      // if we are just returning a subset of attributes, we can copy the bytes directly
      // and avoid creating new objects, reserializing, etc
      binaryTransform =
          if (indices.contains(-1)) { reserializeTransform } else { new BinaryTransformerV2(input, indices, offsets) }
    }

    override def id(): String = {
      input.setPosition(5)
      input.readString()
    }

    override def getAttribute(index: Int): AnyRef = {
      val offset = offsets(index)
      if (offset == -1) { null } else {
        input.setPosition(offset)
        serializer.readersV2(index)(input)
      }
    }

    override def getUserData: java.util.Map[AnyRef, AnyRef] = {
      input.setPosition(userDataOffset)
      KryoUserDataSerialization.deserialize(input)
    }

    override def getDateAsLong(index: Int): Long = {
      val offset = offsets(index)
      if (offset == -1) { 0L } else {
        input.setPosition(offset)
        KryoFeatureDeserializationV2.LongReader.apply(input)
      }
    }

    override def getInput(index: Int): Option[Input] = {
      val offset = offsets(index)
      if (offset == -1) { None } else {
        input.setPosition(offset)
        Some(input)
      }
    }

    override def transform(original: SimpleFeature): Array[Byte] = {
      // if attributes have been added to the sft, we have to reserialize to get the null serialized values
      val transformer = if (missingAttributes) { reserializeTransform } else { binaryTransform }
      transformer.transform(original)
    }

    /**
      * Serialization version 2 binary transformer. Copies serialized attribute bytes directly without
      * deserializing them
      *
      * @param input input
      * @param indices transform indices
      * @param offsets attribute offsets
      */
    private class BinaryTransformerV2(input: Input, indices: Array[Int], offsets: Array[Int]) extends Transformer {

      private val mutableOffsetsAndLength = Array.ofDim[(Int,Int)](indices.length)

      override def transform(original: SimpleFeature): Array[Byte] = {
        // NOTE: the input buffer is the raw buffer. we need to ensure that we use the
        // offset into the raw buffer rather than the raw buffer directly
        val buf = input.getBuffer
        var length = offsets(0) - offset // space for version, offset block and ID
        var idx = 0
        while (idx < mutableOffsetsAndLength.length) {
          val i = indices(idx)
          val el = (if (i < offsets.length - 1) { offsets(i + 1) } else { startOfOffsets }) - offsets(i)
          length += el
          mutableOffsetsAndLength(idx) = (offsets(i), el)
          idx += 1
        }

        val dst = Array.ofDim[Byte](length)
        // copy the version, offset block and id
        var dstPos = offsets(0) - offset
        System.arraycopy(buf, offset, dst, 0, dstPos)
        mutableOffsetsAndLength.foreach { case (o, l) =>
          System.arraycopy(buf, o, dst, dstPos, l)
          dstPos += l
        }
        // note that the offset block is incorrect - we couldn't use this in another lazy feature
        // but the normal serializer doesn't care
        dst
      }
    }
  }

  /**
    * For non-attribute expressions, we have evaluate them, then serialize the resulting feature
    *
    * @param schema transform schema
    * @param transforms transform definitions
    * @param options serialization options
    */
  private class ReserializeTransformer(
      schema: SimpleFeatureType,
      transforms: Array[Transform],
      options: Set[SerializationOption]
    ) extends Transformer {

    private val serializer = KryoFeatureSerializer(schema, options)
    private val sf = new ScalaSimpleFeature(schema, "")

    override def transform(original: SimpleFeature): Array[Byte] = {
      sf.setId(original.getID)
      var i = 0
      while (i < transforms.length) {
        sf.setAttribute(i, transforms(i).evaluate(original))
        i += 1
      }
      serializer.serialize(sf)
    }
  }
}
