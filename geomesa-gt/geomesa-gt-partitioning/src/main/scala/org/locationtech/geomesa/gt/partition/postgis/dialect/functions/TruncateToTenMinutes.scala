/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package functions

/**
 * Truncates a timestamp to the nearest ten-minute boundary. For example, 05:45:23 -> 05:40:00
 */
object TruncateToTenMinutes extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    Seq(
      """CREATE OR REPLACE FUNCTION truncate_to_ten_minutes(dtg timestamp without time zone)
        |RETURNS timestamp without time zone AS
        |  $BODY$
        |    SELECT date_trunc('hour', dtg) + INTERVAL '10 MINUTES' * floor(date_part('minute', dtg) / 10);
        |  $BODY$
        |LANGUAGE sql;""".stripMargin
    )
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] = Seq.empty // function is shared between types
}
