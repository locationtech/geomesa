/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.fs.storage.orc

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.orc.TypeDescription
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.{FileSystemPathReader, MetadataObservingFileSystemWriter, WriterCallback}
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
  * Orc implementation of FileSystemStorage
  *
  * @param metadata metadata
  */
class OrcFileSystemStorage(context: FileSystemContext, metadata: StorageMetadata)
    extends AbstractFileSystemStorage(context, metadata, OrcFileSystemStorage.FileExtension) with LazyLogging {

  override protected def createWriter(file: Path, cb: WriterCallback): FileSystemWriter =
    new OrcFileSystemWriter(metadata.sft, context.conf, file) with MetadataObservingFileSystemWriter {
      override def callback: WriterCallback = cb
    }

  override protected def createReader(
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)]): FileSystemPathReader = {
    val optimized = filter.map(FastFilterFactory.optimize(metadata.sft, _))
    new OrcFileSystemReader(metadata.sft, context.conf, optimized, transform)
  }
}

object OrcFileSystemStorage {

  val Encoding      = "orc"
  val FileExtension = "orc"

  def geometryXField(attribute: String): String = s"${attribute}_x"
  def geometryYField(attribute: String): String = s"${attribute}_y"

  /**
    * Create the Orc type description corresponding to the SimpleFeatureType. SimpleFeatureType is
    * modeled as an Orc Struct, with nested fields for each attribute.
    *
    * @param sft simple feature type
    * @return
    */
  def createTypeDescription(sft: SimpleFeatureType, fid: Boolean = true): TypeDescription = {
    val container = TypeDescription.createStruct()
    var i = 0
    while (i < sft.getAttributeCount) {
      addTypeDescription(container, sft.getDescriptor(i))
      i += 1
    }
    if (fid) {
      container.addField("id", TypeDescription.createString())
    }
    container
  }

  /**
    * Gets a count of the Orc fields created for a simple feature type.
    *
    * Geometry attributes create 2 columns, other attributes create 1, feature id creates 1
    *
    * @param sft simple feature type
    * @param fid include feature ids or not
    * @return
    */
  def fieldCount(sft: SimpleFeatureType, fid: Boolean = true): Int = {
    import scala.collection.JavaConversions._
    sft.getAttributeDescriptors.count(d => classOf[Geometry].isAssignableFrom(d.getType.getBinding)) +
        sft.getAttributeCount + (if (fid) { 1 } else { 0 })
  }

  /**
    * Add a type description for an attribute
    *
    * @param container top-level Orc struct corresponding to the SimpleFeatureType
    * @param descriptor descriptor
    */
  private def addTypeDescription(container: TypeDescription, descriptor: AttributeDescriptor): Unit = {
    val name = descriptor.getLocalName
    val bindings = ObjectType.selectType(descriptor)
    bindings.head match {
      case ObjectType.GEOMETRY => addGeometryDescription(container, name, bindings(1))
      case ObjectType.LIST     => container.addField(name, TypeDescription.createList(simple(bindings(1))))
      case ObjectType.MAP      => container.addField(name, TypeDescription.createMap(simple(bindings(1)), simple(bindings(2))))
      case binding             => container.addField(name, simple(binding))
    }
  }

  /**
    * We create two separate fields, one for x-values and one for y-values. Orc doesn't support
    * predicate push-down on nested fields, so we flatten them out. Note that Orc also doesn't
    * support predicate push-down for complex fields, so we actually only benefit for Points.
    */
  private def addGeometryDescription(container: TypeDescription, name: String, binding: ObjectType): Unit = {
    import TypeDescription.{createDouble, createList}

    val x = geometryXField(name)
    val y = geometryYField(name)

    binding match {
      case ObjectType.POINT =>
        // point (x, y) pair
        container.addField(x, createDouble())
        container.addField(y, createDouble())

      case ObjectType.LINESTRING =>
        // list of points
        container.addField(x, createList(createDouble()))
        container.addField(y, createList(createDouble()))

      case ObjectType.MULTIPOINT =>
        // list of points
        container.addField(x, createList(createDouble()))
        container.addField(y, createList(createDouble()))

      case ObjectType.POLYGON =>
        // list of lines (exterior ring + any holes)
        container.addField(x, createList(createList(createDouble())))
        container.addField(y, createList(createList(createDouble())))

      case ObjectType.MULTILINESTRING =>
        // list of lines
        container.addField(x, createList(createList(createDouble())))
        container.addField(y, createList(createList(createDouble())))

      case ObjectType.MULTIPOLYGON =>
        // list of polygons
        container.addField(x, createList(createList(createList(createDouble()))))
        container.addField(y, createList(createList(createList(createDouble()))))

      case _ => throw new IllegalArgumentException(s"Unexpected geometry type $binding")
    }
  }

  /**
    * Type description for simple types, e.g. int, float, date, etc
    *
    * @param binding type binding
    * @return
    */
  private def simple(binding: ObjectType): TypeDescription = {
    binding match {
      case ObjectType.DATE    => TypeDescription.createTimestamp()
      case ObjectType.STRING  => TypeDescription.createString()
      case ObjectType.INT     => TypeDescription.createInt()
      case ObjectType.LONG    => TypeDescription.createLong()
      case ObjectType.FLOAT   => TypeDescription.createFloat()
      case ObjectType.DOUBLE  => TypeDescription.createDouble()
      case ObjectType.BOOLEAN => TypeDescription.createBoolean()
      case ObjectType.BYTES   => TypeDescription.createBinary()
      case ObjectType.JSON    => TypeDescription.createString()
      case ObjectType.UUID    => TypeDescription.createString()
      case _ => throw new IllegalArgumentException(s"Unexpected simple object type $binding")
    }
  }
}
