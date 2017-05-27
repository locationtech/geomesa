package org.locationtech.geomesa.parquet

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{MessageType, OriginalType, Type, Types}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Created by afox on 5/25/17.
  */
object SFTSchemaConverter {


  def apply(sft: SimpleFeatureType): MessageType = {
    import scala.collection.JavaConversions._
    new MessageType(sft.getTypeName, sft.getAttributeDescriptors.map(convertField))
  }

  def convertField(ad: AttributeDescriptor): Type = {
    import PrimitiveTypeName._
    import Type.Repetition

    val binding = ad.getType.getBinding
    val (objectType, _) = ObjectType.selectType(binding, ad.getUserData)
    objectType match {
      case ObjectType.GEOMETRY =>
        // TODO: currently only dealing with Points packed into a 16 byte fixed array
        Types.primitive(FIXED_LEN_BYTE_ARRAY, Repetition.REQUIRED)
          .length(16)
          .named(ad.getLocalName)

      case ObjectType.DATE =>
        Types.primitive(INT64, Repetition.REQUIRED)
          .named(ad.getLocalName)

      case ObjectType.STRING =>
        Types.primitive(BINARY, Repetition.REQUIRED)
          .as(OriginalType.UTF8)
          .named(ad.getLocalName)

      case ObjectType.INT =>
        Types.primitive(INT32, Repetition.REQUIRED)
          .named(ad.getLocalName)

      case ObjectType.DOUBLE =>
        Types.primitive(DOUBLE, Repetition.REQUIRED)
          .named(ad.getLocalName)

      case ObjectType.LONG =>
        Types.primitive(INT64, Repetition.REQUIRED)
          .named(ad.getLocalName)

      case ObjectType.FLOAT =>
        Types.primitive(FLOAT, Repetition.REQUIRED)
          .named(ad.getLocalName)

      case ObjectType.BOOLEAN =>
        Types.primitive(BOOLEAN, Repetition.REQUIRED)
          .named(ad.getLocalName)

      case ObjectType.BYTES =>
        // TODO:

      case ObjectType.LIST =>
        // TODO:

      case ObjectType.MAP =>
        // TODO:

      case ObjectType.UUID =>
        // TODO:
    }
  }
}
