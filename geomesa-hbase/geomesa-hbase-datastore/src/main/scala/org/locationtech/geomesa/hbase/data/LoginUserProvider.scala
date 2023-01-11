/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
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