/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.writer

import org.locationtech.geomesa.accumulo.index.JoinIndex
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.conf.ColumnGroups
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}

import java.nio.charset.StandardCharsets

/**
 * Maps columns families from the default index implementation to the accumulo-specific values
 * that were used in legacy indices
 */
trait ColumnFamilyMapper extends (Array[Byte] => Array[Byte])

object ColumnFamilyMapper {

  /**
   * Create a mapper
   *
   * @param index feature index being used
   * @return
   */
  def apply(index: GeoMesaFeatureIndex[_, _]): ColumnFamilyMapper = {
    // last version before col families start matching up with index-api
    val flip = index.name match {
      case Z3Index.name  => 5
      case Z2Index.name  => 4
      case XZ3Index.name => 1
      case XZ2Index.name => 1
      case IdIndex.name  => 3
      case AttributeIndex.name | JoinIndex.name => 7
      case _ => 0
    }

    if (index.version > flip) {
      IdentityMapper
    } else if (index.version < 2 && index.name == IdIndex.name) {
      IdSftMapper
    } else if (index.version < 3 && (index.name == AttributeIndex.name || index.name == JoinIndex.name)) {
      EmptyMapper
    } else if (index.name == JoinIndex.name) {
      JoinMapper
    } else {
      FaMapper
    }
  }

  private case object IdentityMapper extends ColumnFamilyMapper {
    override def apply(cf: Array[Byte]): Array[Byte] = cf
  }

  private case object IdSftMapper extends FixedMapper("SFT")

  private case object EmptyMapper extends FixedMapper("")

  private case object JoinMapper extends FixedMapper("I")

  private case object FaMapper extends ColumnFamilyMapper {
    private val f = "F".getBytes(StandardCharsets.UTF_8)
    private val a = "A".getBytes(StandardCharsets.UTF_8)
    override def apply(cf: Array[Byte]): Array[Byte] = {
      if (java.util.Arrays.equals(cf, ColumnGroups.Default)) {
        f
      } else if (java.util.Arrays.equals(cf, ColumnGroups.Attributes)) {
        a
      } else {
        cf
      }
    }
  }

  private class FixedMapper(cf: String) extends ColumnFamilyMapper {
    private val bytes = cf.getBytes(StandardCharsets.UTF_8)
    override def apply(cf: Array[Byte]): Array[Byte] = bytes
  }
}
