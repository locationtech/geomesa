/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.observer

import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter

/**
 * Marker trait for writer hooks
 */
trait FileSystemObserver extends FileSystemWriter
