/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.locationtech.geomesa.accumulo.index.legacy._
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, GeoMesaFeatureIndexFactory}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.utils.conf.IndexId
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Feature index factory that provides attribute join indices
  */
class AccumuloFeatureIndexFactory extends GeoMesaFeatureIndexFactory {

  override def indices(sft: SimpleFeatureType, hint: Option[String]): Seq[IndexId] = {
    def defaults: Seq[IndexId] = JoinIndex.defaults(sft).map(IndexId(JoinIndex.name, JoinIndex.version, _))
    hint match {
      case None => defaults
      case Some(h) =>
        lazy val Array(name, attributes @ _*) = h.split(":")
        if (h.equalsIgnoreCase(JoinIndex.name) || h.equalsIgnoreCase(AttributeIndex.name)) {
          defaults
        } else if (name.equalsIgnoreCase(JoinIndex.name) && JoinIndex.supports(sft, attributes)) {
          Seq(IndexId(JoinIndex.name, JoinIndex.version, attributes))
        } else {
          Seq.empty
        }
    }
  }

  override def available(sft: SimpleFeatureType): Seq[(String, Int)] = {
    if (JoinIndex.defaults(sft).exists(JoinIndex.supports(sft, _))) {
      Seq((JoinIndex.name, JoinIndex.version))
    } else {
      Seq.empty
    }
  }

  override def create[T, U](ds: GeoMesaDataStore[_],
                            sft: SimpleFeatureType,
                            index: IndexId): Option[GeoMesaFeatureIndex[T, U]] = {
    val idx = if (index.name == JoinIndex.name) {
      val Seq(attribute, secondary @ _*) = index.attributes
      index.version match {
        case 8  => Some(new JoinIndex(ds, sft, attribute, secondary, index.mode))
        case 7  => Some(new JoinIndexV7(ds, sft, attribute, secondary, index.mode))
        case 6  => Some(new JoinIndexV6(ds, sft, attribute, secondary, index.mode))
        case 5  => Some(new JoinIndexV5(ds, sft, attribute, secondary, index.mode))
        case 4  => Some(new JoinIndexV4(ds, sft, attribute, secondary, index.mode))
        case 3  => Some(new JoinIndexV3(ds, sft, attribute, secondary.headOption, index.mode))
        case 2  => Some(new JoinIndexV2(ds, sft, attribute, secondary.headOption, index.mode))
        case _ => None
      }
    } else {
      None
    }
    idx.asInstanceOf[Option[GeoMesaFeatureIndex[T, U]]]
  }
}
