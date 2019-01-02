/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools.status

import com.beust.jcommander._
import org.locationtech.geomesa.kudu.data.KuduDataStore
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.KuduParams
import org.locationtech.geomesa.kudu.tools.status.KuduKeywordsCommand.KuduKeywordsParams
import org.locationtech.geomesa.tools.status.{KeywordsCommand, KeywordsParams}

class KuduKeywordsCommand extends KeywordsCommand[KuduDataStore] with KuduDataStoreCommand {
  override val params = new KuduKeywordsParams
}

object KuduKeywordsCommand {
  @Parameters(commandDescription = "Add/Remove/List keywords on an existing schema")
  class KuduKeywordsParams extends KeywordsParams with KuduParams
}
