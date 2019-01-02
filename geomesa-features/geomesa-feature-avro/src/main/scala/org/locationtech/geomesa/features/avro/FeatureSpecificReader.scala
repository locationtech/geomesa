/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.apache.avro.Schema
import org.apache.avro.io.{Decoder, _}
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureUtils._
import org.locationtech.geomesa.features.avro.FeatureSpecificReader.AttributeReader
import org.locationtech.geomesa.features.avro.serde.{ASFDeserializer, Version1Deserializer, Version2Deserializer}
import org.locationtech.geomesa.features.avro.serialization.AvroUserDataSerialization
import org.locationtech.geomesa.features.serialization.ObjectType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Creates an unitialized feature reader. One of `setType` or `setTypes` must be called before reading
  *
  * @param opts serialization options
  */
class FeatureSpecificReader(opts: SerializationOptions) extends DatumReader[SimpleFeature] {

  private val includeUserData = opts.withUserData

  // DataFileStreams or DataFileReaders may set this after construction of the object
  private var attributeReader: AttributeReader = _

  /**
    * Set the simple feature type being read. One of `setType` or `setTypes` must be called before reading
    *
    * @param sft simple feature type
    */
  def setType(sft: SimpleFeatureType): Unit = setTypes(sft, sft)

  /**
    * Sets the simple feature type being read, and the projected return value. One of `setType` or `setTypes`
    * must be called before reading
    *
    * @param originalType simple feature type of the underlying data
    * @param projectedType projected type to read out
    */
  def setTypes(originalType: SimpleFeatureType, projectedType: SimpleFeatureType): Unit =
    attributeReader = new AttributeReader(originalType, projectedType, !opts.withoutId)

  override def setSchema(schema: Schema): Unit = {}

  override def read(reuse: SimpleFeature, in: Decoder): SimpleFeature = {
    val version = in.readInt()

    if (version > VERSION) {
      throw new IllegalArgumentException(s"AvroSimpleFeature version $version  is unsupported. " +
          "You may need to upgrade to a new version")
    }

    val feature = attributeReader.read(version, in)

    if (includeUserData) {
      feature.getUserData.putAll(AvroUserDataSerialization.deserialize(in))
    }

    feature
  }
}

object FeatureSpecificReader {

  /**
    * Creates a feature specific reader, initialized with the feature type
    *
    * @param sft simple feature type
    * @param opts serialization options
    * @return
    */
  def apply(sft: SimpleFeatureType, opts: SerializationOptions = SerializationOptions.none): FeatureSpecificReader = {
    val reader = new FeatureSpecificReader(opts)
    reader.setType(sft)
    reader
  }

  /**
    * Creates a projecting feature specific reader, initialized with the feature type
    *
    * @param originalType simple feature type of the data being read
    * @param projectedType simple feature type to project to during read
    * @return
    */
  def apply(originalType: SimpleFeatureType, projectedType: SimpleFeatureType): FeatureSpecificReader =
    apply(originalType, projectedType, SerializationOptions.none)

  /**
    * Creates a projecting feature specific reader, initialized with the feature type
    *
    * @param originalType simple feature type of the data being read
    * @param projectedType simple feature type to project to during read
    * @param opts serialization options
    * @return
    */
  def apply(originalType: SimpleFeatureType,
            projectedType: SimpleFeatureType,
            opts: SerializationOptions): FeatureSpecificReader = {
    val reader = new FeatureSpecificReader(opts)
    reader.setTypes(originalType, projectedType)
    reader
  }

  private class AttributeReader(originalType: SimpleFeatureType,
                                projectedType: SimpleFeatureType,
                                includeFid: Boolean) {

    import scala.collection.JavaConverters._

    private val requiredFields = DataUtilities.attributeNames(projectedType).toSet
    private val nillableFields = originalType.getAttributeDescriptors.asScala.collect {
      case ad if ad.isNillable => ad.getLocalName
    }.toSet

    private val v2Readers = buildFieldReaders(Version2Deserializer)
    private lazy val v1Readers = buildFieldReaders(Version1Deserializer)

    def read(version: Int, decoder: Decoder): ScalaSimpleFeature = {
      val feature = ScalaSimpleFeature.create(projectedType, if (includeFid) { decoder.readString() } else { "" })
      if (version > 1) {
        v2Readers.foreach(_.apply(feature, decoder))
      } else {
        v1Readers.foreach(_.apply(feature, decoder))
      }
      feature
    }

    private def buildFieldReaders(d: ASFDeserializer): Seq[(ScalaSimpleFeature, Decoder) => Unit] = {
      originalType.getAttributeDescriptors.asScala.map { ad =>
        val name = ad.getLocalName
        val i = projectedType.indexOf(name)
        val skip = !requiredFields.contains(name)

        val f: (ScalaSimpleFeature, Decoder) => Unit = ObjectType.selectType(ad).head match {
          case ObjectType.STRING   => if (skip) { (_, in) => d.consumeString(in)   } else { d.setString(_, i, _)   }
          case ObjectType.INT      => if (skip) { (_, in) => d.consumeInt(in)      } else { d.setInt(_, i, _)      }
          case ObjectType.LONG     => if (skip) { (_, in) => d.consumeLong(in)     } else { d.setLong(_, i, _)     }
          case ObjectType.FLOAT    => if (skip) { (_, in) => d.consumeFloat(in)    } else { d.setFloat(_, i, _)    }
          case ObjectType.DOUBLE   => if (skip) { (_, in) => d.consumeDouble(in)   } else { d.setDouble(_, i, _)   }
          case ObjectType.BOOLEAN  => if (skip) { (_, in) => d.consumeBool(in)     } else { d.setBool(_, i, _)     }
          case ObjectType.DATE     => if (skip) { (_, in) => d.consumeDate(in)     } else { d.setDate(_, i, _)     }
          case ObjectType.UUID     => if (skip) { (_, in) => d.consumeUUID(in)     } else { d.setUUID(_, i, _)     }
          case ObjectType.GEOMETRY => if (skip) { (_, in) => d.consumeGeometry(in) } else { d.setGeometry(_, i, _) }
          case ObjectType.LIST     => if (skip) { (_, in) => d.consumeList(in)     } else { d.setList(_, i, _)     }
          case ObjectType.MAP      => if (skip) { (_, in) => d.consumeMap(in)      } else { d.setMap(_, i, _)      }
          case ObjectType.BYTES    => if (skip) { (_, in) => d.consumeBytes(in)    } else { d.setBytes(_, i, _)    }
          case ObjectType.JSON     => if (skip) { (_, in) => d.consumeString(in)   } else { d.setString(_, i, _)   }
          case b => throw new IllegalArgumentException(s"Unexpected attribute binding: $b")
        }

        if (nillableFields.contains(name)) {
          (sf: ScalaSimpleFeature, in: Decoder) => if (in.readIndex() == 1) { in.readNull() } else { f(sf, in) }
        } else {
          f
        }
      }
    }
  }
}
