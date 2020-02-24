/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.osm

import java.io.InputStream

import com.typesafe.config.Config
import de.topobyte.osm4j.core.model.iface._
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert.osm.OsmFormat.OsmFormat
import org.locationtech.geomesa.convert.osm.OsmNodesConverter.OsmNodesConfig
import org.locationtech.geomesa.convert2.AbstractConverter.BasicOptions
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.jts.geom.Coordinate
import org.opengis.feature.simple.SimpleFeatureType

class OsmNodesConverter(sft: SimpleFeatureType, config: OsmNodesConfig, fields: Seq[OsmField], options: BasicOptions)
    extends AbstractConverter[OsmNode, OsmNodesConfig, OsmField, BasicOptions](sft, config, fields, options) {

  private val gf = JTSFactoryFinder.getGeometryFactory
  private val fetchMetadata = requiresMetadata(fields)
  private val toArray = if (fetchMetadata) { toArrayWithMetadata _ } else { toArrayNoMetadata _ }

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[OsmNode] = {
    // nodes are first in the file, so we can stop when we hit another element type
    osmIterator(is, config.format, fetchMetadata)
        .takeWhile(_.getType == EntityType.Node)
        .map(_.getEntity.asInstanceOf[OsmNode])
  }

  override protected def values(
      parsed: CloseableIterator[OsmNode],
      ec: EvaluationContext): CloseableIterator[Array[Any]] =
    parsed.map(n => toArray(n, gf.createPoint(new Coordinate(n.getLongitude, n.getLatitude))))
}

object OsmNodesConverter {

  case class OsmNodesConfig(
      `type`: String,
      format: OsmFormat,
      idField: Option[Expression],
      caches: Map[String, Config],
      userData: Map[String, Expression]
    ) extends ConverterConfig
}
