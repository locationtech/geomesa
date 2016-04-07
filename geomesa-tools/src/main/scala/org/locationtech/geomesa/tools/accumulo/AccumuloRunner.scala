/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.tools.accumulo

import org.locationtech.geomesa.tools.accumulo.commands._
import org.locationtech.geomesa.tools.common.Runner
import org.locationtech.geomesa.tools.common.commands.{Command, HelpCommand, VersionCommand}

object AccumuloRunner extends Runner {
  override val scriptName: String = "geomesa-accumulo"
  override val commands: List[Command] = List(
    new CreateCommand(jc),
    new DeleteCatalogCommand(jc),
    new DeleteRasterCommand(jc),
    new DescribeCommand(jc),
    new EnvironmentCommand(jc),
    new ExplainCommand(jc),
    new ExportCommand(jc),
    new HelpCommand(jc),
    new IngestCommand(jc),
    new IngestRasterCommand(jc),
    new ListCommand(jc),
    new RemoveSchemaCommand(jc),
    new TableConfCommand(jc),
    new VersionCommand(jc),
    new QueryStatsCommand(jc),
    new GetSftCommand(jc)
  )
}
