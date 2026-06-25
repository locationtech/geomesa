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
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionScheme.EnumeratedScheme
import org.locationtech.jts.geom.Geometry

import java.util.regex.Pattern
import scala.reflect.ClassTag

trait SpatialScheme extends PartitionScheme with EnumeratedScheme {

  protected def digits: Int

  override def partitions: Seq[PartitionKey] = {
    val keys = (1 to digits).foldLeft(Seq("")) { (accumulated, _) =>
      for {
        str <- accumulated
        char <- SpatialScheme.HexChars
      } yield {
        str + char
      }
    }
    keys.map(PartitionKey(name, _))
  }
}

object SpatialScheme {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val HexChars: Seq[Char] = Range(0, 10).map(_.toString.charAt(0)) ++ Seq.tabulate(6)(i => ('a' + i).toChar)

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
}
