/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geotools.tools.data

import com.beust.jcommander.Parameters
import org.geotools.data.Transaction
import org.geotools.data.postgis.PostGISPSDialect
import org.geotools.jdbc.{JDBCDataStore, JDBCDataStoreFactory}
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand
import org.locationtech.geomesa.geotools.tools.GeoToolsDataStoreCommand.GeoToolsDataStoreParams
import org.locationtech.geomesa.geotools.tools.data.PostgisUpgradeSchemaCommand.PostgisUpgradeSchemaParams
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.io.WithClose

import scala.annotation.tailrec

class PostgisUpgradeSchemaCommand extends GeoToolsDataStoreCommand {

  override val params = new PostgisUpgradeSchemaParams()

  override val name: String = "partition-upgrade"

  override def execute(): Unit = withDataStore { case ds: JDBCDataStore =>
    Command.user.info(s"Running upgrade on schema: ${params.featureName}")
    val sft = ds.getSchema(params.featureName)
    WithClose(ds.getConnection(Transaction.AUTO_COMMIT)) { cx =>
      val dialect = ds.dialect match {
        case p: PartitionedPostgisDialect => p
        case p: PostGISPSDialect =>
          @tailrec
          def unwrap(c: Class[_]): Class[_] =
            if (c == classOf[PostGISPSDialect]) { c } else { unwrap(c.getSuperclass) }
          val m = unwrap(p.getClass).getDeclaredMethod("getDelegate")
          m.setAccessible(true)
          m.invoke(p).asInstanceOf[PartitionedPostgisDialect]
      }
      val schema = connection.getOrElse(JDBCDataStoreFactory.SCHEMA.key, "public")
      dialect.upgrade(schema, sft, cx)
    }
    Command.user.info("Upgrade complete")
  }
}

object PostgisUpgradeSchemaCommand {
  @Parameters(commandDescription = "Update the GeoMesa partitioning functions to the latest version")
  class PostgisUpgradeSchemaParams extends GeoToolsDataStoreParams with RequiredTypeNameParam
}

