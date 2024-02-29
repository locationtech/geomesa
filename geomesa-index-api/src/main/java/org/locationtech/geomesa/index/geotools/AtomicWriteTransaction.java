/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools;

import org.geotools.api.data.Transaction;

import java.io.IOException;
import java.util.Set;

/**
 * Transaction object that enforces atomic writes - this ensures that a feature is not modified between
 * when it's read and when it's updated. Does not support normal transaction operations, such
 * as commit or rollback, and instead operates like auto-commit.
 */
public class AtomicWriteTransaction
      implements Transaction {

    public static final AtomicWriteTransaction INSTANCE = new AtomicWriteTransaction();

    private AtomicWriteTransaction() {

    }

    @Override
    public void putState(Object key, State state) {
        throw new UnsupportedOperationException("PutState is not supported for GeoMesa stores");
    }

    @Override
    public State getState(Object key) {
        return null;
    }

    @Override
    public void removeState(Object key) {
        throw new UnsupportedOperationException("RemoveState is not supported for GeoMesa stores");
    }

    @Override
    public void putProperty(Object key, Object value) {
        throw new UnsupportedOperationException("PutProperty is not supported for GeoMesa stores");
    }

    @Override
    public Object getProperty(Object key) {
        return null;
    }

    @Override
    public void addAuthorization(String authID) {
        throw new UnsupportedOperationException("AddAuthorization is not supported for GeoMesa stores");
    }

    @Override
    public Set<String> getAuthorizations() {
        throw new UnsupportedOperationException("GetAuthorizations is not supported for GeoMesa stores");
    }

    @Override
    public void commit()
          throws IOException {

    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException("Rollback is not supported for GeoMesa stores");
    }



    @Override
    public void close()
          throws IOException {

    }

    @Override
    public String toString() {
        return "AtomicWritesTransaction";
    }
}

