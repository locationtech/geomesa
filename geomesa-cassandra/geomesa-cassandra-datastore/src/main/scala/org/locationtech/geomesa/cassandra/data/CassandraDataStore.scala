/***********************************************************************
 * Copyright (c) 2017 IBM
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core._
import org.locationtech.geomesa.cassandra._
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.CassandraDataStoreConfig
import org.locationtech.geomesa.cassandra.index.CassandraFeatureIndex
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, MetadataStringSerializer}
import org.locationtech.geomesa.index.stats.{GeoMesaStats, UnoptimizedRunnableStats}
import org.locationtech.geomesa.index.utils.LocalLocking
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class CassandraDataStore(val session: Session, config: CassandraDataStoreConfig)
    extends CassandraDataStoreType(config) with LocalLocking {

  override val metadata: GeoMesaMetadata[String] =
    new CassandraBackedMetadata(session, config.catalog, MetadataStringSerializer)

  override def manager: CassandraIndexManagerType = CassandraFeatureIndex

  override def stats: GeoMesaStats = new UnoptimizedRunnableStats(this)

  override def createFeatureWriterAppend(sft: SimpleFeatureType,
                                         indices: Option[Seq[CassandraFeatureIndexType]]): CassandraFeatureWriterType =
    new CassandraAppendFeatureWriter(sft, this, indices)

  override def createFeatureWriterModify(sft: SimpleFeatureType,
                                         indices: Option[Seq[CassandraFeatureIndexType]],
                                         filter: Filter): CassandraFeatureWriterType =
    new CassandraModifyFeatureWriter(sft, this, indices, filter)

  override def createSchema(sft: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.setTableSharing(false)
    super.createSchema(sft)
  }

  override def delete(): Unit = {
    val tables = getTypeNames.map(getSchema).flatMap { sft =>
      manager.indices(sft, IndexMode.Any).map(_.getTableName(sft.getTypeName, this))
    }

    (tables.distinct :+ config.catalog).par.foreach { table =>
      session.execute(s"drop table $table")
    }
  }

  override def dispose(): Unit = {
    super.dispose()
  }
}
