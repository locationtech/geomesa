/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.tools.status.AccumuloDescribeSchemaCommand.DescribeParameters
import org.locationtech.geomesa.tools.RequiredTypeNameParam
import org.locationtech.geomesa.tools.status.DescribeSchemaCommand

class AccumuloDescribeSchemaCommand extends DescribeSchemaCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new DescribeParameters
}

object AccumuloDescribeSchemaCommand {
  @Parameters(commandDescription = "Describe the attributes of a given GeoMesa feature type")
  class DescribeParameters extends AccumuloDataStoreParams with RequiredTypeNameParam
}

