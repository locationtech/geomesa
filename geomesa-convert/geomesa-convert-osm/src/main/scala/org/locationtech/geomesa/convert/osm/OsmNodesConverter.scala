/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.osm

import java.io.InputStream

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Coordinate
import de.topobyte.osm4j.core.model.iface._
import de.topobyte.osm4j.pbf.seq.PbfIterator
import de.topobyte.osm4j.xml.dynsax.OsmXmlIterator
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.immutable.IndexedSeq

class OsmNodesConverter(val targetSFT: SimpleFeatureType,
                        val idBuilder: Expr,
                        val inputFields: IndexedSeq[Field],
                        val userDataBuilder: Map[String, Expr],
                        val caches: Map[String, EnrichmentCache],
                        val parseOpts: ConvertParseOpts,
                        val pbf: Boolean,
                        val needsMetadata: Boolean) extends ToSimpleFeatureConverter[OsmNode] with LazyLogging {

  private def gf = JTSFactoryFinder.getGeometryFactory
  private val toArray = if (needsMetadata) { OsmField.toArrayWithMetadata _ } else { OsmField.toArrayNoMetadata _ }

  override def fromInputType(i: OsmNode, ec: EvaluationContext): Iterator[Array[Any]] =
    Iterator.single(toArray(i, gf.createPoint(new Coordinate(i.getLongitude, i.getLatitude))))

  override def process(is: InputStream, ec: EvaluationContext = createEvaluationContext()): Iterator[SimpleFeature] = {
    val iterator = if (pbf) new PbfIterator(is, needsMetadata) else new OsmXmlIterator(is, needsMetadata)
    val entities = new Iterator[OsmNode] {
      private var element = if (iterator.hasNext) { iterator.next } else { null }
      // nodes are first in the file, so we can stop when we hit another element type
      override def hasNext: Boolean = element != null && element.getType == EntityType.Node
      override def next(): OsmNode = {
        val ret = element.getEntity.asInstanceOf[OsmNode]
        element = if (iterator.hasNext) iterator.next else null
        ret
      }
    }
    processInput(entities, ec)
  }
}

class OsmNodesConverterFactory extends AbstractSimpleFeatureConverterFactory[OsmNode] {

  override protected val typeToProcess = "osm-nodes"

  override protected def buildConverter(sft: SimpleFeatureType,
                                        conf: Config,
                                        idBuilder: Expr,
                                        fields: IndexedSeq[Field],
                                        userDataBuilder: Map[String, Expr],
                                        cacheServices: Map[String, EnrichmentCache],
                                        parseOpts: ConvertParseOpts): SimpleFeatureConverter[OsmNode] = {
    val pbf = if (conf.hasPath("format")) conf.getString("format").toLowerCase.trim.equals("pbf") else false
    val needsMetadata = OsmField.requiresMetadata(fields)
    new OsmNodesConverter(sft, idBuilder, fields, userDataBuilder, cacheServices, parseOpts, pbf, needsMetadata)
  }

  override protected def buildField(field: Config): Field = OsmField.build(field)
}
