/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.audit

import org.locationtech.geomesa.utils.audit.{AuditedEvent, DeletableEvent}

case class QueryEvent(storeType: String,
                      typeName: String,
                      date:     Long,
                      user:     String,
                      filter:   String,
                      hints:    String,
                      planTime: Long,
                      scanTime: Long,
                      hits:     Long,
                      deleted:  Boolean = false) extends AuditedEvent with DeletableEvent
