/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.tools.ingest

import java.io.File

import com.beust.jcommander.Parameters
import org.apache.kudu.client.KuduClient
import org.locationtech.geomesa.kudu.data.KuduDataStore
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand
import org.locationtech.geomesa.kudu.tools.KuduDataStoreCommand.KuduParams
import org.locationtech.geomesa.kudu.tools.ingest.KuduIngestCommand.KuduIngestParams
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

class KuduIngestCommand extends IngestCommand[KuduDataStore] with KuduDataStoreCommand {

  override val params = new KuduIngestParams()

  override val libjarsFile: String = "org/locationtech/geomesa/kudu/tools/ingest-libjars.list"

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_KUDU_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[KuduDataStore]),
    () => ClassPathUtils.getJarsFromClasspath(classOf[KuduClient])
  )
}

object KuduIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class KuduIngestParams extends IngestParams with KuduParams
}
