/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;

import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ViewExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 * Connector-level access control that injects a per-row visibility filter on
 * tables carrying a visibility column, enforcing entitlements for ALL Trino
 * consumers of the {@code spatial_iceberg} catalog (direct SQL / JDBC / BI),
 * complementing the datastore-layer enforcement used by GeoTools clients.
 *
 * <p><strong>Allow-all baseline.</strong> Every {@link ConnectorAccessControl}
 * method DENIES by default, so a connector that installs one is expected to own
 * authorization for the catalog. This feature is purely additive — it must not
 * restrict anything beyond row visibility — so every check is overridden to
 * allow and every {@code filter*} to identity, reproducing the permissive
 * baseline of Iceberg's (absent) connector access control. Only
 * {@link #getRowFilters} adds behavior. {@code VisibilityAccessControlAllowAllTest}
 * is a tripwire: if a Trino upgrade adds a new (deny-default) SPI method, that
 * test fails so the new method is consciously allowed rather than silently
 * locking the catalog.
 *
 * <p>The visibility column is detected at analysis time by
 * {@code SpatialConnectorMetadata.getColumnHandles} and read here from the shared
 * {@link GeoMesaColumnCatalog}. Metadata tables (information_schema, Iceberg
 * {@code $}-metadata) are skipped. For any other (data) table that has not been
 * observed, the filter fails closed (hides all rows) rather than risk leaking a
 * vis-bearing table. Boundary: this protects only {@code spatial_iceberg}; the
 * plain {@code iceberg} catalog is not wrapped and must not be exposed to
 * untrusted users.
 */
public final class VisibilityAccessControl implements ConnectorAccessControl {

    private static final Logger LOG = Logger.getLogger(VisibilityAccessControl.class.getName());

    private final String catalog;
    private final GeoMesaColumnCatalog geomCatalog;
    private final AuthorizationResolver resolver;

    /**
     * Builds the access control for a single catalog.
     *
     * @param catalog name of the catalog this access control guards
     * @param geomCatalog shared column catalog used to detect visibility columns
     * @param resolver resolves a session identity to its authorization tokens
     */
    public VisibilityAccessControl(String catalog, GeoMesaColumnCatalog geomCatalog,
                                   AuthorizationResolver resolver) {
        this.catalog = catalog;
        this.geomCatalog = geomCatalog;
        this.resolver = resolver;
    }

    /**
     * The one method that adds behavior: a row filter for tables with a
     * visibility column, evaluated by the global {@code is_visible} UDF.
     *
     * @param context the connector security context (carries the session identity)
     * @param table the table being accessed
     * @return row filters to apply; empty list if the table needs none
     */
    @Override
    public List<ViewExpression> getRowFilters(ConnectorSecurityContext context, SchemaTableName table) {
        // Metadata/system tables (information_schema, Iceberg "$"-metadata) are
        // never visibility-controlled and reach here unobserved; skip them
        // explicitly so the fail-closed branch below doesn't empty SHOW TABLES /
        // information_schema.
        if (isMetadataTable(table)) {
            return List.of();
        }
        // visibilityColumn: null = not observed, empty = observed-no-vis-col,
        // present = observed-with-vis-col.
        Optional<String> visColumn = geomCatalog.visibilityColumn(table);
        if (visColumn == null) {
            // A real data table reached row-filter analysis without first being
            // observed via getColumnHandles (e.g. an unexpected planner path or
            // cold worker). Fail closed — hide all rows rather than risk leaking
            // a vis-bearing table. Normal scans observe columns first, so this
            // should not fire in practice.
            LOG.warning("Visibility column not observed for " + catalog + "." + table
                + " before row-filter analysis; hiding all rows (fail-closed)");
            return List.of(viewExpression(context, table, "false"));
        }
        if (visColumn.isEmpty()) {
            return List.of();  // observed: table has no visibility column
        }
        // Sort for a deterministic, cache-friendly auth literal (order is
        // irrelevant to the is_visible UDF).
        List<String> auths =
            new ArrayList<>(new TreeSet<>(resolver.authorizationsFor(context.getIdentity())));
        return List.of(viewExpression(context, table,
            VisibilityRowFilter.conjunct(visColumn.get(), auths)));
    }

    /** information_schema and Iceberg {@code $}-suffixed metadata tables carry no
     *  visibility column and must never be row-filtered. */
    private static boolean isMetadataTable(SchemaTableName table) {
        return "information_schema".equals(table.getSchemaName())
            || table.getTableName().contains("$");
    }

    private ViewExpression viewExpression(ConnectorSecurityContext context,
                                          SchemaTableName table, String expression) {
        return ViewExpression.builder()
            .identity(context.getIdentity().getUser())
            .catalog(catalog)
            .schema(table.getSchemaName())
            .expression(expression)
            .build();
    }

    // ── Allow-all overrides (generated from the Trino 481 ConnectorAccessControl
    //    SPI). Checks are no-ops; filter* return their input unchanged. ──
    /** Allow-all override. @param context the security context @param schemaName the schema name @param properties the schema properties */
    @Override public void checkCanCreateSchema(io.trino.spi.connector.ConnectorSecurityContext context, java.lang.String schemaName, java.util.Map<java.lang.String, java.lang.Object> properties) { }
    /** Allow-all override. @param context the security context @param schemaName the schema name */
    @Override public void checkCanDropSchema(io.trino.spi.connector.ConnectorSecurityContext context, java.lang.String schemaName) { }
    /** Allow-all override. @param context the security context @param schemaName the schema name @param newSchemaName the new schema name */
    @Override public void checkCanRenameSchema(io.trino.spi.connector.ConnectorSecurityContext context, java.lang.String schemaName, java.lang.String newSchemaName) { }
    /** Allow-all override. @param context the security context @param schemaName the schema name @param principal the new owner principal */
    @Override public void checkCanSetSchemaAuthorization(io.trino.spi.connector.ConnectorSecurityContext context, java.lang.String schemaName, io.trino.spi.security.TrinoPrincipal principal) { }
    /** Allow-all override. @param context the security context */
    @Override public void checkCanShowSchemas(io.trino.spi.connector.ConnectorSecurityContext context) { }
    /** Identity filter. @param context the security context @param schemaNames the schema names @return the schema names unchanged */
    @Override public java.util.Set<java.lang.String> filterSchemas(io.trino.spi.connector.ConnectorSecurityContext context, java.util.Set<java.lang.String> schemaNames) { return schemaNames; }
    /** Allow-all override. @param context the security context @param schemaName the schema name */
    @Override public void checkCanShowCreateSchema(io.trino.spi.connector.ConnectorSecurityContext context, java.lang.String schemaName) { }
    /** Allow-all override. @param context the security context @param table the table name */
    @Override public void checkCanShowCreateTable(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table) { }
    /** Allow-all override. @param context the security context @param table the table name @param properties the table properties */
    @Override public void checkCanCreateTable(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.util.Map<java.lang.String, java.lang.Object> properties) { }
    /** Allow-all override. @param context the security context @param table the table name */
    @Override public void checkCanDropTable(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table) { }
    /** Allow-all override. @param context the security context @param table the table name @param newTable the new table name */
    @Override public void checkCanRenameTable(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, io.trino.spi.connector.SchemaTableName newTable) { }
    /** Allow-all override. @param context the security context @param table the table name @param properties the table properties */
    @Override public void checkCanSetTableProperties(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.util.Map<java.lang.String, java.util.Optional<java.lang.Object>> properties) { }
    /** Allow-all override. @param context the security context @param table the table name */
    @Override public void checkCanSetTableComment(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table) { }
    /** Allow-all override. @param context the security context @param view the view name */
    @Override public void checkCanSetViewComment(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName view) { }
    /** Allow-all override. @param context the security context @param table the table name */
    @Override public void checkCanSetColumnComment(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table) { }
    /** Allow-all override. @param context the security context @param schemaName the schema name */
    @Override public void checkCanShowTables(io.trino.spi.connector.ConnectorSecurityContext context, java.lang.String schemaName) { }
    /** Identity filter. @param context the security context @param tableNames the table names @return the table names unchanged */
    @Override public java.util.Set<io.trino.spi.connector.SchemaTableName> filterTables(io.trino.spi.connector.ConnectorSecurityContext context, java.util.Set<io.trino.spi.connector.SchemaTableName> tableNames) { return tableNames; }
    /** Allow-all override. @param context the security context @param table the table name */
    @Override public void checkCanShowColumns(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table) { }
    /** Identity filter. @param context the security context @param tableColumns the columns by table @return the columns unchanged */
    @Override public java.util.Map<io.trino.spi.connector.SchemaTableName, java.util.Set<java.lang.String>> filterColumns(io.trino.spi.connector.ConnectorSecurityContext context, java.util.Map<io.trino.spi.connector.SchemaTableName, java.util.Set<java.lang.String>> tableColumns) { return tableColumns; }
    /** Allow-all override. @param context the security context @param table the table name */
    @Override public void checkCanAddColumn(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table) { }
    /** Allow-all override. @param context the security context @param table the table name */
    @Override public void checkCanAlterColumn(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table) { }
    /** Allow-all override. @param context the security context @param table the table name */
    @Override public void checkCanDropColumn(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table) { }
    /** Allow-all override. @param context the security context @param table the table name @param principal the new owner principal */
    @Override public void checkCanSetTableAuthorization(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, io.trino.spi.security.TrinoPrincipal principal) { }
    /** Allow-all override. @param context the security context @param table the table name */
    @Override public void checkCanRenameColumn(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table) { }
    /** Allow-all override. @param context the security context @param table the table name @param branch the branch name @param columns the column names */
    @Override public void checkCanSelectFromColumns(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.util.Optional<java.lang.String> branch, java.util.Set<java.lang.String> columns) { }
    /** Allow-all override. @param context the security context @param table the table name @param columns the column names */
    @Override public void checkCanSelectFromColumns(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.util.Set<java.lang.String> columns) { }
    /** Allow-all override. @param context the security context @param table the table name @param branch the branch name */
    @Override public void checkCanInsertIntoTable(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.util.Optional<java.lang.String> branch) { }
    /** Allow-all override. @param context the security context @param table the table name */
    @Override public void checkCanInsertIntoTable(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table) { }
    /** Allow-all override. @param context the security context @param table the table name @param branch the branch name */
    @Override public void checkCanDeleteFromTable(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.util.Optional<java.lang.String> branch) { }
    /** Allow-all override. @param context the security context @param table the table name */
    @Override public void checkCanDeleteFromTable(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table) { }
    /** Allow-all override. @param context the security context @param table the table name */
    @Override public void checkCanTruncateTable(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table) { }
    /** Allow-all override. @param context the security context @param table the table name @param branch the branch name @param columns the updated column names */
    @Override public void checkCanUpdateTableColumns(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.util.Optional<java.lang.String> branch, java.util.Set<java.lang.String> columns) { }
    /** Allow-all override. @param context the security context @param table the table name @param columns the updated column names */
    @Override public void checkCanUpdateTableColumns(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.util.Set<java.lang.String> columns) { }
    /** Allow-all override. @param context the security context @param view the view name */
    @Override public void checkCanCreateView(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName view) { }
    /** Allow-all override. @param context the security context @param view the view name @param newView the new view name */
    @Override public void checkCanRenameView(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName view, io.trino.spi.connector.SchemaTableName newView) { }
    /** Allow-all override. @param context the security context @param view the view name @param principal the new owner principal */
    @Override public void checkCanSetViewAuthorization(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName view, io.trino.spi.security.TrinoPrincipal principal) { }
    /** Allow-all override. @param context the security context @param view the materialized view name @param principal the new owner principal */
    @Override public void checkCanSetMaterializedViewAuthorization(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName view, io.trino.spi.security.TrinoPrincipal principal) { }
    /** Allow-all override. @param context the security context @param view the view name */
    @Override public void checkCanDropView(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName view) { }
    /** Allow-all override. @param context the security context @param table the table name @param branch the branch name @param columns the column names */
    @Override public void checkCanCreateViewWithSelectFromColumns(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.util.Optional<java.lang.String> branch, java.util.Set<java.lang.String> columns) { }
    /** Allow-all override. @param context the security context @param table the table name @param columns the column names */
    @Override public void checkCanCreateViewWithSelectFromColumns(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.util.Set<java.lang.String> columns) { }
    /** Allow-all override. @param context the security context @param view the materialized view name @param properties the view properties */
    @Override public void checkCanCreateMaterializedView(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName view, java.util.Map<java.lang.String, java.lang.Object> properties) { }
    /** Allow-all override. @param context the security context @param view the materialized view name */
    @Override public void checkCanRefreshMaterializedView(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName view) { }
    /** Allow-all override. @param context the security context @param view the view name */
    @Override public void checkCanRefreshView(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName view) { }
    /** Allow-all override. @param context the security context @param view the materialized view name @param properties the view properties */
    @Override public void checkCanSetMaterializedViewProperties(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName view, java.util.Map<java.lang.String, java.util.Optional<java.lang.Object>> properties) { }
    /** Allow-all override. @param context the security context @param view the materialized view name */
    @Override public void checkCanDropMaterializedView(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName view) { }
    /** Allow-all override. @param context the security context @param view the materialized view name @param newView the new materialized view name */
    @Override public void checkCanRenameMaterializedView(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName view, io.trino.spi.connector.SchemaTableName newView) { }
    /** Allow-all override. @param context the security context @param propertyName the session property name */
    @Override public void checkCanSetCatalogSessionProperty(io.trino.spi.connector.ConnectorSecurityContext context, java.lang.String propertyName) { }
    /** Allow-all override. @param context the security context @param privilege the privilege @param schemaName the schema name @param grantee the grantee principal @param grantOption whether grant option is included */
    @Override public void checkCanGrantSchemaPrivilege(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.security.Privilege privilege, java.lang.String schemaName, io.trino.spi.security.TrinoPrincipal grantee, boolean grantOption) { }
    /** Allow-all override. @param context the security context @param privilege the privilege @param schemaName the schema name @param grantee the grantee principal */
    @Override public void checkCanDenySchemaPrivilege(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.security.Privilege privilege, java.lang.String schemaName, io.trino.spi.security.TrinoPrincipal grantee) { }
    /** Allow-all override. @param context the security context @param privilege the privilege @param schemaName the schema name @param grantee the grantee principal @param grantOption whether grant option is included */
    @Override public void checkCanRevokeSchemaPrivilege(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.security.Privilege privilege, java.lang.String schemaName, io.trino.spi.security.TrinoPrincipal grantee, boolean grantOption) { }
    /** Allow-all override. @param context the security context @param privilege the privilege @param table the table name @param grantee the grantee principal @param grantOption whether grant option is included */
    @Override public void checkCanGrantTablePrivilege(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.security.Privilege privilege, io.trino.spi.connector.SchemaTableName table, io.trino.spi.security.TrinoPrincipal grantee, boolean grantOption) { }
    /** Allow-all override. @param context the security context @param privilege the privilege @param table the table name @param grantee the grantee principal */
    @Override public void checkCanDenyTablePrivilege(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.security.Privilege privilege, io.trino.spi.connector.SchemaTableName table, io.trino.spi.security.TrinoPrincipal grantee) { }
    /** Allow-all override. @param context the security context @param privilege the privilege @param table the table name @param grantee the grantee principal @param grantOption whether grant option is included */
    @Override public void checkCanRevokeTablePrivilege(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.security.Privilege privilege, io.trino.spi.connector.SchemaTableName table, io.trino.spi.security.TrinoPrincipal grantee, boolean grantOption) { }
    /** Allow-all override. @param context the security context @param privilege the privilege @param table the table name @param branch the branch name @param grantee the grantee principal @param grantOption whether grant option is included */
    @Override public void checkCanGrantTableBranchPrivilege(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.security.Privilege privilege, io.trino.spi.connector.SchemaTableName table, java.lang.String branch, io.trino.spi.security.TrinoPrincipal grantee, boolean grantOption) { }
    /** Allow-all override. @param context the security context @param privilege the privilege @param table the table name @param branch the branch name @param grantee the grantee principal */
    @Override public void checkCanDenyTableBranchPrivilege(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.security.Privilege privilege, io.trino.spi.connector.SchemaTableName table, java.lang.String branch, io.trino.spi.security.TrinoPrincipal grantee) { }
    /** Allow-all override. @param context the security context @param privilege the privilege @param table the table name @param branch the branch name @param grantee the grantee principal @param grantOption whether grant option is included */
    @Override public void checkCanRevokeTableBranchPrivilege(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.security.Privilege privilege, io.trino.spi.connector.SchemaTableName table, java.lang.String branch, io.trino.spi.security.TrinoPrincipal grantee, boolean grantOption) { }
    /** Allow-all override. @param context the security context @param role the role name @param grantor the grantor principal */
    @Override public void checkCanCreateRole(io.trino.spi.connector.ConnectorSecurityContext context, java.lang.String role, java.util.Optional<io.trino.spi.security.TrinoPrincipal> grantor) { }
    /** Allow-all override. @param context the security context @param role the role name */
    @Override public void checkCanDropRole(io.trino.spi.connector.ConnectorSecurityContext context, java.lang.String role) { }
    /** Allow-all override. @param context the security context @param roles the role names @param grantees the grantee principals @param adminOption whether admin option is included @param grantor the grantor principal */
    @Override public void checkCanGrantRoles(io.trino.spi.connector.ConnectorSecurityContext context, java.util.Set<java.lang.String> roles, java.util.Set<io.trino.spi.security.TrinoPrincipal> grantees, boolean adminOption, java.util.Optional<io.trino.spi.security.TrinoPrincipal> grantor) { }
    /** Allow-all override. @param context the security context @param roles the role names @param grantees the grantee principals @param adminOption whether admin option is included @param grantor the grantor principal */
    @Override public void checkCanRevokeRoles(io.trino.spi.connector.ConnectorSecurityContext context, java.util.Set<java.lang.String> roles, java.util.Set<io.trino.spi.security.TrinoPrincipal> grantees, boolean adminOption, java.util.Optional<io.trino.spi.security.TrinoPrincipal> grantor) { }
    /** Allow-all override. @param context the security context @param role the role name */
    @Override public void checkCanSetRole(io.trino.spi.connector.ConnectorSecurityContext context, java.lang.String role) { }
    /** Allow-all override. @param context the security context */
    @Override public void checkCanShowRoles(io.trino.spi.connector.ConnectorSecurityContext context) { }
    /** Allow-all override. @param context the security context */
    @Override public void checkCanShowCurrentRoles(io.trino.spi.connector.ConnectorSecurityContext context) { }
    /** Allow-all override. @param context the security context */
    @Override public void checkCanShowRoleGrants(io.trino.spi.connector.ConnectorSecurityContext context) { }
    /** Allow-all override. @param context the security context @param procedure the procedure name */
    @Override public void checkCanExecuteProcedure(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaRoutineName procedure) { }
    /** Allow-all override. @param context the security context @param table the table name @param procedure the procedure name */
    @Override public void checkCanExecuteTableProcedure(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.lang.String procedure) { }
    /** Allow-all override. @param context the security context @param function the function name @return true (always allowed) */
    @Override public boolean canExecuteFunction(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaRoutineName function) { return true; }
    /** Allow-all override. @param context the security context @param function the function name @return true (always allowed) */
    @Override public boolean canCreateViewWithExecuteFunction(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaRoutineName function) { return true; }
    /** Allow-all override. @param context the security context @param schemaName the schema name */
    @Override public void checkCanShowFunctions(io.trino.spi.connector.ConnectorSecurityContext context, java.lang.String schemaName) { }
    /** Identity filter. @param context the security context @param functions the function names @return the function names unchanged */
    @Override public java.util.Set<io.trino.spi.function.SchemaFunctionName> filterFunctions(io.trino.spi.connector.ConnectorSecurityContext context, java.util.Set<io.trino.spi.function.SchemaFunctionName> functions) { return functions; }
    /** Allow-all override. @param context the security context @param function the function name */
    @Override public void checkCanCreateFunction(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaRoutineName function) { }
    /** Allow-all override. @param context the security context @param function the function name */
    @Override public void checkCanDropFunction(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaRoutineName function) { }
    /** Allow-all override. @param context the security context @param function the function name */
    @Override public void checkCanShowCreateFunction(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaRoutineName function) { }
    /** Allow-all override. @param context the security context @param table the table name */
    @Override public void checkCanShowBranches(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table) { }
    /** Allow-all override. @param context the security context @param table the table name @param branch the branch name */
    @Override public void checkCanCreateBranch(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.lang.String branch) { }
    /** Allow-all override. @param context the security context @param table the table name @param branch the branch name */
    @Override public void checkCanDropBranch(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.lang.String branch) { }
    /** Allow-all override. @param context the security context @param table the table name @param sourceBranch the source branch name @param targetBranch the target branch name */
    @Override public void checkCanFastForwardBranch(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.lang.String sourceBranch, java.lang.String targetBranch) { }
    /** No column masks. @param context the security context @param table the table name @param columns the columns @return an empty map (no masks applied) */
    @Override public java.util.Map<io.trino.spi.connector.ColumnSchema, io.trino.spi.security.ViewExpression> getColumnMasks(io.trino.spi.connector.ConnectorSecurityContext context, io.trino.spi.connector.SchemaTableName table, java.util.List<io.trino.spi.connector.ColumnSchema> columns) { return java.util.Map.of(); }
}
