/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.slf4j.impl;

/*
  org.apache.hbase:hbase-http:(,2.2.7] has a hard-coded dependency on Log4jLoggerAdapter from
  org.slf4j:slf4j-log4j12:(,1.7.33]. Replacing slf4j-log4j12 with slf4j-reload4j in GeoMesa
  resulted in a NoClassDefFoundError during unit tests with the HBase mini cluster. The mini
  cluster doesn't do anything meaningful with the class, so this empty placeholder resolves
  those errors.
 */
public class Log4jLoggerAdapter {
}
