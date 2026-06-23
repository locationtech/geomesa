/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.utils.text.StringSerialization
import org.locationtech.jts.geom.Geometry

import java.util.regex.Pattern
import scala.reflect.ClassTag

trait SpatialScheme extends PartitionScheme

object SpatialScheme {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  abstract class SpatialPartitionSchemeFactory[T <: Geometry : ClassTag](val name: String) extends PartitionSchemeFactory {

    private val namePattern: Pattern = Pattern.compile(s"$name-([0-9]+)bits?:?")

    override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] = {
      val opts = SchemeOpts(scheme)
      lazy val matcher = namePattern.matcher(scheme)

      def build(resolution: Short): Option[PartitionScheme] = {
        val geom = opts.getSingle("attribute").orElse(Option(sft.getGeomField)).orNull
        require(geom != null, s"Spatial schemes requires an attribute to be specified with 'attribute=<attribute>'")
        val index = attributeIndex(sft, geom, Some(implicitly[ClassTag[T]].runtimeClass))
        Some(buildPartitionScheme(resolution, geom, index))
      }

      if (opts.name == this.name) {
        val bits = opts.getSingle("bits").map(_.toShort).getOrElse {
          throw new IllegalArgumentException(s"Spatial schemes requires a resolution to be specified with 'bits=<resolution>'")
        }
        build(bits)
      } else if (matcher.matches()) {
        build(matcher.group(1).toShort)
      } else {
        None
      }
    }

    def buildPartitionScheme(bits: Int, geom: String, geomIndex: Int): PartitionScheme
  }


  /**
   * Holder for a z-value field, along with a reference back to the original geometry field
   *
   * @param geometry name of the original geometry field being covered
   * @param zValue name of the z-value field
   */
  case class ZValueField(geometry: String, zValue: String)

  object ZValueField {

    val ZValueFieldPrefix = "__"
    val Z2ValueFieldSuffix = "_z2__"
    val XZ2ValueFieldSuffix = "_xz2__"

    def z2(geometry: String, encoded: Boolean = false): ZValueField = {
      val geom = if (encoded) { geometry } else { StringSerialization.alphaNumericSafeString(geometry) }
      val zValue = s"$ZValueFieldPrefix$geom$Z2ValueFieldSuffix"
      ZValueField(geom, zValue)
    }

    def xz2(geometry: String, encoded: Boolean = false): ZValueField = {
      val geom = if (encoded) { geometry } else { StringSerialization.alphaNumericSafeString(geometry) }
      val zValue = s"$ZValueFieldPrefix$geom$XZ2ValueFieldSuffix"
      ZValueField(geom, zValue)
    }

    /**
     * Creates a field name based on a z-value field
     *
     * @param field name of a potential z-value field
     * @return
     */
    def fromFieldName(field: String): Option[ZValueField] = {
      if (field.startsWith(ZValueFieldPrefix)) {
        Seq(Z2ValueFieldSuffix, XZ2ValueFieldSuffix).collectFirst {
          case suffix if field.endsWith(suffix) =>
            ZValueField(field.substring(ZValueFieldPrefix.length, field.length - suffix.length), field)
        }
      } else {
        None
      }
    }
  }
}
