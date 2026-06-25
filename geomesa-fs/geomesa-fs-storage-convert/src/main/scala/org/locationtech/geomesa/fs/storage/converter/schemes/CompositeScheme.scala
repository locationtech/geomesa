/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.schemes

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter

import scala.util.control.NonFatal

case class CompositeScheme(schemes: Seq[PartitionScheme]) extends PartitionScheme {

  require(schemes.lengthCompare(1) > 0, "Must provide at least 2 schemes for a composite scheme")

  override val depth: Int = schemes.map(_.depth).sum

  override def pattern: String = schemes.map(_.pattern).mkString("/")

  override def getIntersectingPartitions(filter: Filter): Option[Seq[String]] = {
    val head = schemes.head.getIntersectingPartitions(filter)
    schemes.tail.foldLeft(head) { case (result, scheme) =>
      for { res <- result; partitions <- scheme.getIntersectingPartitions(filter) } yield {
        res.flatMap(r => partitions.map(p => s"$r/$p"))
      }
    }
  }
}

object CompositeScheme {

  val SchemeSeparator = ","

  object CompositePartitionSchemeFactory extends PartitionSchemeFactory with LazyLogging {
    override def load(sft: SimpleFeatureType, config: NamedOptions): Option[PartitionScheme] = {
      if (!config.name.contains(SchemeSeparator)) { None } else {
        val configs = config.name.split(SchemeSeparator).map(n => config.copy(name = n))
        val composites = configs.map { c =>
          try { PartitionSchemeFactory.load(sft, c) } catch {
            case NonFatal(e) =>
              throw new IllegalArgumentException(s"Error creating composite scheme '${c.name}' (from '${config.name}'):", e)
          }
        }
        Some(CompositeScheme(composites))
      }
    }
  }
}
