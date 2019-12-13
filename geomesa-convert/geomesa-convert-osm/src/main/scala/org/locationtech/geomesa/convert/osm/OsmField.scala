/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.osm

import java.util.Date

import com.typesafe.config.Config
import org.locationtech.jts.geom.Geometry
import de.topobyte.osm4j.core.model.iface._
import de.topobyte.osm4j.core.model.util.OsmModelUtil
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert.osm.OsmField.OsmAttribute.OsmAttribute
import org.locationtech.geomesa.convert.{Field, _}

case class OsmField(name: String, attribute: OsmAttribute, transform: Expr) extends Field {

  private val mutableArray = Array.ofDim[Any](1)
  private val fromArray = OsmField.fromArray(attribute)

  override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = {
    mutableArray(0) = fromArray(args)
    if (transform == null) {
      mutableArray(0)
    } else {
      super.eval(mutableArray)
    }
  }
}

object OsmField {

  private def gf = JTSFactoryFinder.getGeometryFactory
  private val tagsFromMetadata =
    Seq(OsmAttribute.user, OsmAttribute.uid, OsmAttribute.version, OsmAttribute.changeset, OsmAttribute.timestamp)

  def build(field: Config): Field = {
    val name = field.getString("name")
    val transform = if (field.hasPath("transform")) {
      Transformers.parseTransform(field.getString("transform"))
    } else {
      null
    }
    if (field.hasPath("attribute")) {
      OsmField(name, OsmAttribute.withName(field.getString("attribute")), transform)
    } else {
      SimpleField(name, transform)
    }
  }

  def requiresMetadata(fields: Seq[Field]): Boolean =
    fields.exists { case OsmField(_, tag, _) => tagsFromMetadata.contains(tag); case _ => false }

  def fromArray(attribute: OsmAttribute): (Array[Any]) => Any = {
    attribute match {
      case OsmAttribute.id        => (a) => a(0)
      case OsmAttribute.geometry  => (a) => a(1)
      case OsmAttribute.tags      => (a) => a(2)
      case OsmAttribute.timestamp => (a) => a(3)
      case OsmAttribute.user      => (a) => a(4)
      case OsmAttribute.uid       => (a) => a(5)
      case OsmAttribute.version   => (a) => a(6)
      case OsmAttribute.changeset => (a) => a(7)
    }
  }

  def toArrayWithMetadata(entity: OsmEntity, geometry: Geometry): Array[Any] = {
    val tags = OsmModelUtil.getTagsAsMap(entity)
    val metadata = Option(entity.getMetadata)
    val timestamp = metadata.map(m => new Date(m.getTimestamp)).orNull
    val user = metadata.map(_.getUser).orNull
    val uid = metadata.map(m => Long.box(m.getUid)).orNull
    val version = metadata.map(m => Int.box(m.getVersion)).orNull
    val changeset = metadata.map(m => Long.box(m.getChangeset)).orNull
    Array(entity.getId, geometry, tags, timestamp, user, uid, version, changeset)
  }

  def toArrayNoMetadata(entity: OsmEntity, geometry: Geometry): Array[Any] =
    Array(entity.getId, geometry, OsmModelUtil.getTagsAsMap(entity))

  object OsmAttribute extends Enumeration {
    type OsmAttribute = Value
    val id, geometry, tags, timestamp, user, uid, version, changeset = Value
  }
}
