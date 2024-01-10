/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package functions

/**
 * Truncates a timestamp to the closest partition boundary, based on the number of hours in each partition
 */
object TruncateToPartition extends TruncateToPartition with AdvisoryLock {
  override protected val lockId: Long = 1616433564832724520L
}

class TruncateToPartition extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    Seq(
      """CREATE OR REPLACE FUNCTION truncate_to_partition(dtg timestamp without time zone, hours int)
        |RETURNS timestamp without time zone AS
        |  $BODY$
        |    SELECT date_trunc('day', dtg) +
        |      (hours * INTERVAL '1 HOUR' * floor(date_part('hour', dtg) / hours));
        |  $BODY$
        |LANGUAGE sql;""".stripMargin
    )
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] = Seq.empty // function is shared between types
}
