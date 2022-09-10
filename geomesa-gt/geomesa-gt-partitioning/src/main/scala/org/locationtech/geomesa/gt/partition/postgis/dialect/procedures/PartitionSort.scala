/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package procedures

/**
 * Re-sorts a partition table. Can be used for maintenance if time-latent data comes in and degrades performance
 * of the BRIN index
 */
@deprecated("replaced with compact_partition")
object PartitionSort extends SqlProcedure {

  override def name(info: TypeInfo): FunctionName = FunctionName(s"${info.typeName}_partition_sort")

  override protected def createStatements(info: TypeInfo): Seq[String] = Seq(proc(info))

  private def proc(info: TypeInfo): String = {
    s"""CREATE OR REPLACE PROCEDURE ${name(info).quoted}(for_date timestamp without time zone) LANGUAGE plpgsql AS
       |  $$BODY$$
       |    BEGIN
       |      RAISE INFO '% Calling deprecated procedure %', timeofday()::timestamp, ${name(info).asLiteral};
       |      CALL ${CompactPartitions.name(info).quoted}(for_date);
       |    END;
       |  $$BODY$$;
       |""".stripMargin
  }
}
