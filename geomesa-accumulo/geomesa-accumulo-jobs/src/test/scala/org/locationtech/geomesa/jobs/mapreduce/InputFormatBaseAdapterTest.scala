/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.mapreduce.Job
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InputFormatBaseAdapterTest extends Specification {
  val job = new Job

  "InputFormatBaseAdapter" should {
    "set values properly" in {
      InputFormatBaseAdapter.setScanAuthorizations(job, new Authorizations())
      InputFormatBaseAdapter.setZooKeeperInstance(job, "instanceId", "localhost")
      InputFormatBaseAdapter.setConnectorInfo(job, "user", new PasswordToken("password"))
      true mustEqual(true)
    }
  }
}
