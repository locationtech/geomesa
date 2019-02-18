/***********************************************************************
 * Copyright (c) 2017-2019 IBM
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core._
import org.geotools.data.Query
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.CassandraDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, MetadataStringSerializer}
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.stats.MetadataBackedStats.RunnableStats
import org.locationtech.geomesa.index.utils.{Explainer, LocalLocking}
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.simple.SimpleFeatureType

class CassandraDataStore(val session: Session, config: CassandraDataStoreConfig)
    extends GeoMesaDataStore[CassandraDataStore](config) with LocalLocking {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  override val metadata: GeoMesaMetadata[String] =
    new CassandraBackedMetadata(session, config.catalog, MetadataStringSerializer)

  override val adapter: CassandraIndexAdapter = new CassandraIndexAdapter(this)

  override val stats: GeoMesaStats = new RunnableStats(this)

  override def getQueryPlan(query: Query, index: Option[String], explainer: Explainer): Seq[CassandraQueryPlan] =
    super.getQueryPlan(query, index, explainer).asInstanceOf[Seq[CassandraQueryPlan]]

  override def delete(): Unit = {
    val tables = getTypeNames.flatMap(getAllIndexTableNames)
    (tables.distinct :+ config.catalog).par.foreach { table =>
      session.execute(s"drop table $table")
    }
  }

  override protected def transitionIndices(sft: SimpleFeatureType): Unit = {
    val dtg = sft.getDtgField.toSeq
    val geom = Option(sft.getGeomField).toSeq

    val indices = Seq.newBuilder[IndexId]
    val tableNameKeys = Seq.newBuilder[(String, String)]

    sft.getIndices.foreach {
      case id if id.name == IdIndex.name =>
        require(id.version == 1, s"Expected index version of 1 but got: $id")
        indices += id.copy(version = 3)
        tableNameKeys += ((s"table.${IdIndex.name}.v1", s"table.${IdIndex.name}.v3"))

      case id if id.name == Z3Index.name =>
        require(id.version <= 2, s"Expected index version of 1 or 2 but got: $id")
        indices += id.copy(attributes = geom ++ dtg, version = id.version + 3)
        tableNameKeys += ((s"table.${Z3Index.name}.v${id.version}", s"table.${Z3Index.name}.v${id.version + 3}"))

      case id if id.name == XZ3Index.name =>
        require(id.version == 1, s"Expected index version of 1 but got: $id")
        indices += id.copy(attributes = geom ++ dtg)

      case id if id.name == Z2Index.name =>
        require(id.version <= 2, s"Expected index version of 1 or 2 but got: $id")
        indices += id.copy(attributes = geom, version = id.version + 2)
        tableNameKeys += ((s"table.${Z2Index.name}.v${id.version}", s"table.${Z2Index.name}.v${id.version + 2}"))

      case id if id.name == XZ2Index.name =>
        require(id.version == 1, s"Expected index version of 1 but got: $id")
        indices += id.copy(attributes = geom)

      case id if id.name == AttributeIndex.name =>
        require(id.version <= 2, s"Expected index version of 1 or 2 but got: $id")
        val fields = geom ++ dtg
        sft.getAttributeDescriptors.asScala.foreach { d =>
          val index = d.getUserData.remove(AttributeOptions.OPT_INDEX).asInstanceOf[String]
          if (index == null || index.equalsIgnoreCase("false") || index.equalsIgnoreCase(IndexCoverage.NONE.toString)) {
            // no-op
          } else if (java.lang.Boolean.valueOf(index) || index.equalsIgnoreCase(IndexCoverage.FULL.toString) ||
              index.equalsIgnoreCase(IndexCoverage.JOIN.toString)) {
            indices += id.copy(attributes = Seq(d.getLocalName) ++ fields, version = id.version + 4)
          } else {
            throw new IllegalStateException(s"Expected an index coverage or boolean but got: $index")
          }
        }
        tableNameKeys ++=
            Seq(s"table.${AttributeIndex.name}.v${id.version}", "tables.idx.attr.name")
                .map((_, s"table.${AttributeIndex.name}.v${id.version + 4}"))
    }

    sft.setIndices(indices.result)

    // update metadata keys for tables
    tableNameKeys.result.foreach { case (from, to) =>
      metadata.scan(sft.getTypeName, from, cache = false).foreach { case (key, name) =>
        metadata.insert(sft.getTypeName, to + key.substring(from.length), name)
        metadata.remove(sft.getTypeName, key)
      }
    }
  }
}
