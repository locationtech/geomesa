/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import com.beust.jcommander.converters.IParameterSplitter

import java.util.Collections

class NoopParameterSplitter extends IParameterSplitter {
  override def split(s : String): java.util.List[String] = new java.util.ArrayList(Collections.singletonList(s))
}
