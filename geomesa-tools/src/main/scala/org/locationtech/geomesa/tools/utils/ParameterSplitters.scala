/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import com.beust.jcommander.converters.IParameterSplitter

import scala.collection.JavaConversions._

// Overrides JCommander's split on comma to allow single keywords containing commas
class KeywordParamSplitter extends IParameterSplitter {
  override def split(s : String): java.util.List[String] = List(s)
}

class SemiColonParamSplitter extends IParameterSplitter {
  override def split(s : String): java.util.List[String] = List(s)
}