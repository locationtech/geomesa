/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.tools.status

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.arrow.data.ArrowDataStore
import org.locationtech.geomesa.arrow.tools.ArrowDataStoreCommand
import org.locationtech.geomesa.arrow.tools.ArrowDataStoreCommand.UrlParam
import org.locationtech.geomesa.arrow.tools.status.ArrowGetTypeNamesCommand.ArrowGetTypeNamesParams
import org.locationtech.geomesa.tools.status.GetTypeNamesCommand

class ArrowGetTypeNamesCommand extends GetTypeNamesCommand[ArrowDataStore] with ArrowDataStoreCommand {
  override val params = new ArrowGetTypeNamesParams()
}

object ArrowGetTypeNamesCommand {
  @Parameters(commandDescription = "List the feature type for a given Arrow resource")
  class ArrowGetTypeNamesParams extends UrlParam
}
