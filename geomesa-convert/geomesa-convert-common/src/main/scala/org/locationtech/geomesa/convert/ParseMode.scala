/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

object ParseMode extends Enumeration {
  type ParseMode = Value
  val Incremental: ParseMode = Value("incremental")
  val Batch      : ParseMode = Value("batch")
  val Default    : ParseMode = Incremental
}
