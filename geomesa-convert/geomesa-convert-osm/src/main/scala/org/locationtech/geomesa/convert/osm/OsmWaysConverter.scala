/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.osm

import java.io.InputStream
import java.nio.file.Files
import java.sql.DriverManager

import com.typesafe.config.Config
import de.topobyte.osm4j.core.model.iface._
import de.topobyte.osm4j.core.model.impl.Node
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert.osm.OsmFormat.OsmFormat
import org.locationtech.geomesa.convert.osm.OsmWaysConverter.OsmWaysConfig
import org.locationtech.geomesa.convert2.AbstractConverter.BasicOptions
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseQuietly, PathUtils, WithClose}
import org.locationtech.jts.geom.Coordinate
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class OsmWaysConverter(sft: SimpleFeatureType, config: OsmWaysConfig, fields: Seq[OsmField], options: BasicOptions)
    extends AbstractConverter[OsmWay, OsmWaysConfig, OsmField, BasicOptions](sft, config, fields, options) {

  private val gf = JTSFactoryFinder.getGeometryFactory
  private val fetchMetadata = requiresMetadata(fields)
  private val toArray = if (fetchMetadata) { toArrayWithMetadata _ } else { toArrayNoMetadata _ }

  private val (connection, h2Dir) = config.jdbc match {
    case Some(url) => (DriverManager.getConnection(url), None)
    case None =>
      // create a temporary h2 database to store our nodes
      // the ways only refer to nodes by reference, but we can't keep the whole file in memory
      val h2Dir = Files.createTempDirectory("geomesa-convert-h2").toFile
      Class.forName("org.h2.Driver")
      (DriverManager.getConnection(s"jdbc:h2:split:${h2Dir.getAbsolutePath}/osm"), Some(h2Dir))
  }

  createNodesTable()

  private val insertStatement = connection.prepareStatement("INSERT INTO nodes(id, lon, lat) VALUES (?, ?, ?);")

  override def close(): Unit = {
    val exceptions = ArrayBuffer.empty[Throwable]
    Try(dropNodesTable()).failed.foreach(exceptions += _)
    CloseQuietly(insertStatement, connection).foreach(exceptions += _)
    Try(h2Dir.foreach(d => PathUtils.deleteRecursively(d.toPath))).failed.foreach(exceptions += _)
    if (exceptions.nonEmpty) {
      val first = exceptions.head
      exceptions.tail.foreach(first.addSuppressed)
      throw first
    }
  }

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[OsmWay] = {
    val elements = osmIterator(is, config.format, fetchMetadata)
    // first read in all the nodes and store them for later lookup
    val ways = elements.flatMap {
      case e if e.getType == EntityType.Node => insertNode(e.getEntity.asInstanceOf[OsmNode]); CloseableIterator.empty
      case e => CloseableIterator.single(e)
    }
    // types are ordered in the file, so we can stop when we hit a non-way type
    ways.takeWhile(_.getType == EntityType.Way).map(_.getEntity.asInstanceOf[OsmWay])
  }

  override protected def values(
      parsed: CloseableIterator[OsmWay],
      ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    parsed.flatMap { way =>
      // TODO some ways are marked as 'area' and maybe should be polygons?
      // note: nodes may occur more than once in the way
      val nodeIds = Seq.range(0, way.getNumberOfNodes).map(way.getNodeId)
      val selected = selectNodes(nodeIds)
      val nodes = nodeIds.flatMap(selected.get)
      if (nodes.size != nodeIds.length) {
        logger.warn(s"Dropping references to non-existing nodes in way '${way.getId}': " +
            s"${nodeIds.filterNot(nodes.contains).mkString(", ")}")
      }
      if (nodes.size < 2) {
        logger.warn(s"Dropping way '${way.getId}' because it does not have enough valid nodes to form a linestring")
        CloseableIterator.empty
      } else {
        val coords = nodes.map(n => new Coordinate(n.getLongitude, n.getLatitude))
        CloseableIterator.single(toArray(way, gf.createLineString(coords.toArray)))
      }
    }
  }

  private def createNodesTable(): Unit = {
    val sql = "create table nodes(id BIGINT NOT NULL PRIMARY KEY, lon DOUBLE, lat DOUBLE);"
    WithClose(connection.prepareStatement(sql))(_.execute())
  }

  private def dropNodesTable(): Unit =
    WithClose(connection.prepareStatement("drop table nodes;"))(_.execute())

  private def insertNode(node: OsmNode): Unit = {
    insertStatement.setLong(1, node.getId)
    insertStatement.setDouble(2, node.getLongitude)
    insertStatement.setDouble(3, node.getLatitude)
    insertStatement.executeUpdate()
  }

  private def selectNodes(ids: Seq[Long]): Map[Long, OsmNode] = {
    val map = scala.collection.mutable.Map.empty[Long, OsmNode]
    // group so that we don't overwhelm the select clause
    ids.grouped(99).foreach { group =>
      val statement = connection.prepareCall(s"SELECT * FROM nodes WHERE id IN(${group.mkString(",")});")
      val results = statement.executeQuery()
      while (results.next()) {
        val node = new Node(results.getLong(1), results.getDouble(2), results.getDouble(3))
        map.put(node.getId, node)
      }
      results.close()
      statement.close()
    }
    map.toMap
  }
}

object OsmWaysConverter {

  case class OsmWaysConfig(
      `type`: String,
      format: OsmFormat,
      jdbc: Option[String],
      idField: Option[Expression],
      caches: Map[String, Config],
      userData: Map[String, Expression]
    ) extends ConverterConfig
}

