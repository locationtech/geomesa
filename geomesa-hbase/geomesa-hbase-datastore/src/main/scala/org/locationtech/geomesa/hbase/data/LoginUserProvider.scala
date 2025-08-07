/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.apache.hadoop.hbase.security.User.SecureHadoopUser
import org.apache.hadoop.hbase.security.UserProvider
import org.apache.hadoop.security.UserGroupInformation

class LoginUserProvider extends UserProvider {
  override def getCurrent = {
    new SecureHadoopUser(UserGroupInformation.getLoginUser)
  }
}