/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase

import org.joda.time.{DateTime, Seconds, Weeks}
import org.locationtech.geomesa.curve.{BinnedTime, TimePeriod, Z3SFC}

package object data {
  val SFC = Z3SFC(TimePeriod.Week)
  val dateToIndex = BinnedTime.dateToBinnedTime(TimePeriod.Week)
}
