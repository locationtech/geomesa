/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import java.util.Collections

import com.beust.jcommander.converters.IParameterSplitter

class NoopParameterSplitter extends IParameterSplitter {
  override def split(s : String): java.util.List[String] = new java.util.ArrayList(Collections.singletonList(s))
}
