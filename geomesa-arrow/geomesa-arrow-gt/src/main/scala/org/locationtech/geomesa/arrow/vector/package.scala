/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.{ListVector, StructVector}
import org.apache.arrow.vector.types.Types.MinorType
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.{FieldVector, ZeroVector}

package object vector {

  import scala.collection.JavaConverters._

  /**
   * Trait for creating new vectors
   */
  sealed trait VectorFactory {

    /**
     * Create a vector based on a minor type
     *
     * @param name vector name
     * @param minorType vector type
     * @param metadata field metadata
     * @tparam T vector type
     * @return
     */
    def apply[T <: FieldVector](name: String, minorType: MinorType, metadata: Map[String, String]): T =
      apply(name, new FieldType(true, minorType.getType, null, if (metadata.isEmpty) { null } else { metadata.asJava }))

    /**
     * Create a vector based on a field
     *
     * @param name vector name
     * @param field field
     * @tparam T vector type
     * @return
     */
    def apply[T <: FieldVector](name: String, field: FieldType): T
  }

  object VectorFactory {
    def apply(allocator: BufferAllocator): VectorFactory = FromAllocator(allocator)
    def apply(parent: StructVector): VectorFactory = FromStruct(parent)
  }

  /**
   * Factory for creating vectors from an allocator
   *
   * @param allocator allocator
   */
  case class FromAllocator(allocator: BufferAllocator) extends VectorFactory {
    override def apply[T <: FieldVector](name: String, field: FieldType): T = {
      val vector = field.createNewSingleVector(name, allocator, null)
      vector.allocateNew()
      vector.asInstanceOf[T]
    }
  }

  /**
   * Factory for creating vectors from a parent struct
   *
   * @param parent struct vector
   */
  case class FromStruct(parent: StructVector) extends VectorFactory {
    override def apply[T <: FieldVector](name: String, field: FieldType): T = {
      var vector = parent.getChild(name)
      if (vector == null) {
        vector = parent.addOrGet(name, field, classOf[FieldVector])
        vector.allocateNew()
      }
      vector.asInstanceOf[T]
    }
  }

  /**
   * Factory for creating vectors from a parent list
   *
   * Note: a list vector can only have a single child, and the name is not used
   *
   * @param parent list vector
   */
  case class FromList(parent: ListVector) extends VectorFactory {
    override def apply[T <: FieldVector](name: String, field: FieldType): T = {
      var vector = parent.getDataVector
      if (vector == ZeroVector.INSTANCE) {
        vector = parent.addOrGetVector[T](field).getVector
        vector.allocateNew()
      }
      vector.asInstanceOf[T]
    }
  }
}
