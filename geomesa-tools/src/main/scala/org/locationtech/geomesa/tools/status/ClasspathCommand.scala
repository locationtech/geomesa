/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.tools.Command

class ClasspathCommand extends Command {

  override val name = "classpath"
  override val params = new ClasspathParameters
  override def execute(): Unit = {}
}

@Parameters(commandDescription = "Display the GeoMesa classpath")
class ClasspathParameters {}
