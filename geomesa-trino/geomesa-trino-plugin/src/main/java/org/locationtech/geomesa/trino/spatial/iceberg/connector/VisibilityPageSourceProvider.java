/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergTableCredentials;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.Domain;
import org.locationtech.geomesa.trino.security.GeoMesaSecurityFunctions;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Enforces absorbed predicates using exact filtering or complete statistics/footer proofs. */
final class VisibilityPageSourceProvider implements ConnectorPageSourceProvider {
    private final ConnectorPageSourceProvider delegate;
    private final VisibilityFooterReader footerReader;

    VisibilityPageSourceProvider(ConnectorPageSourceProvider delegate) { this(delegate, null); }

    VisibilityPageSourceProvider(ConnectorPageSourceProvider delegate, VisibilityFooterReader footerReader) {
        this.delegate = delegate;
        this.footerReader = footerReader;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle table, Optional<ConnectorTableCredentials> credentials,
            List<ColumnHandle> columns, DynamicFilter dynamicFilter, MemoryContext memoryContext) {
        if (!(table instanceof VisibilityTableHandle visibility)) {
            return delegate.createPageSource(transaction, session, split, table, credentials, columns, dynamicFilter, memoryContext);
        }
        List<Slice> auths = visibility.auths().stream().map(Slices::utf8Slice).toList();
        Domain domain = split instanceof IcebergSplit iceberg
            ? iceberg.fileStatisticsDomain().getDomains().map(d -> d.get(visibility.visibilityColumn())).orElse(null)
            : null;
        boolean bypass = allVisible(domain, auths);
        if (!bypass && footerReader != null && split instanceof IcebergSplit iceberg
                && iceberg.fileFormat() == IcebergFileFormat.PARQUET && iceberg.parquetFileDecryptionData().isEmpty()
                && footerReader.shouldProbe(session, iceberg)
                && credentials.orElse(null) instanceof IcebergTableCredentials icebergCredentials) {
            try (var ignored = footerReader.scope()) {
                var inspection = footerReader.inspect(session, iceberg, icebergCredentials,
                    visibility.visibilityColumn().getId(), auths, memoryContext);
                var footerResult = inspection.result();
                if (footerResult == VisibilityFooterReader.Result.NONE_VISIBLE) {
                    return new VisibilityFilteringPageSource(new EmptyPageSource(), columns.size(), -1,
                        auths, false, footerResult, inspection.bytes(), inspection.nanos());
                }
                return createFiltered(transaction, session, split, visibility, credentials, columns, dynamicFilter,
                    memoryContext, auths, footerResult == VisibilityFooterReader.Result.ALL_VISIBLE, footerResult,
                    inspection.bytes(), inspection.nanos());
            }
        }
        return createFiltered(transaction, session, split, visibility, credentials, columns, dynamicFilter,
            memoryContext, auths, bypass, null, 0, 0);
    }

    private ConnectorPageSource createFiltered(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorSplit split, VisibilityTableHandle visibility, Optional<ConnectorTableCredentials> credentials,
            List<ColumnHandle> columns, DynamicFilter dynamicFilter, MemoryContext memoryContext,
            List<Slice> auths, boolean bypass, VisibilityFooterReader.Result footerResult,
            long footerBytes, long footerNanos) {
        List<ColumnHandle> physical = new ArrayList<>(columns);
        int channel = physical.indexOf(visibility.visibilityColumn());
        if (!bypass && channel < 0) {
            channel = physical.size();
            physical.add(visibility.visibilityColumn());
        }
        ConnectorPageSource source = delegate.createPageSource(transaction, session, split, visibility.delegate(),
            credentials, physical, dynamicFilter, memoryContext);
        return new VisibilityFilteringPageSource(source, columns.size(), channel, auths, bypass, footerResult,
            footerBytes, footerNanos);
    }

    static boolean allVisible(Domain domain, List<Slice> auths) {
        if (domain == null || domain.isNone()) return false;
        if (domain.isOnlyNull()) return true;
        if (!domain.getValues().isSingleValue() || !(domain.getValues().getSingleValue() instanceof Slice expression)) {
            return false;
        }
        // Equal bounds prove all non-null values equal this expression. Null is unrestricted.
        return visible(expression, auths);
    }

    static boolean visible(Slice expression, List<Slice> auths) {
        for (Slice auth : auths) {
            if (!GeoMesaSecurityFunctions.isVisible(expression, auth)) return false;
        }
        return true;
    }
}
