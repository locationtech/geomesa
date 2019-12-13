/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.status

import com.beust.jcommander._
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand
import org.locationtech.geomesa.hbase.tools.HBaseDataStoreCommand.{HBaseParams, RemoteFilterNotUsedParam}
import org.locationtech.geomesa.hbase.tools.status.HBaseKeywordsCommand.HBaseKeywordsParams
import org.locationtech.geomesa.tools.status.{KeywordsCommand, KeywordsParams}

class HBaseKeywordsCommand extends KeywordsCommand[HBaseDataStore] with HBaseDataStoreCommand {
  override val params = new HBaseKeywordsParams
}

object HBaseKeywordsCommand {
  @Parameters(commandDescription = "Add/Remove/List keywords on an existing schema")
  class HBaseKeywordsParams extends KeywordsParams with HBaseParams with RemoteFilterNotUsedParam
}
