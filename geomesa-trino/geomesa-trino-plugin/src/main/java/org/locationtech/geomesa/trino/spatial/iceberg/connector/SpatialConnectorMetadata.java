/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.geospatial.serde.JtsGeometrySerde;
import io.trino.spi.connector.*;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import io.trino.spi.predicate.*;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.VarcharType;
import org.locationtech.geomesa.curve.interop.SpaceFillingCurves.HexRange;
import org.locationtech.geomesa.trino.spatial.iceberg.transforms.SpatialIndexRanges;
import org.locationtech.geomesa.trino.spatial.iceberg.BboxHandles;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;
import org.locationtech.geomesa.trino.spatial.GeometryColumn;
import org.locationtech.geomesa.trino.spatial.iceberg.SpatialPartitionHandle;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConnectorMetadata wrapper that injects spatial range constraints into
 * applyFilter for intersection-implying spatial predicates (ST_Intersects,
 * ST_Within, ST_Contains, ST_Crosses, ST_Touches, ST_Overlaps, ST_Equals)
 * and the equivalent __X_bbox__ struct comparisons. For each such predicate the
 * code extracts the query envelope and injects per-geom:
 *   (1) four REAL-typed domains on the {xmin, ymin, xmax, ymax} sub-fields of
 *       the __X_bbox__ struct column (per-file bbox-stat pruning), and
 *   (2) when the table has a __X_z2__ or __X_xz2__ partition column, a
 *       VARCHAR domain covering the spatial-index values that overlap the
 *       envelope (Z2 emits contiguous ranges; XZ2 emits per-cell equality
 *       predicates because the encoding is lossy across levels).
 * Multi-geom support: a single query may filter on multiple geometry columns;
 * findAllSpatialMatches walks ALL spatial calls in the expression tree so
 * each geom column gets independent pruning via its own companions.
 * Disjunctions are handled when every OR branch spatially constrains the same
 * geom column (e.g. {@code ST_Intersects(g, p1) OR ST_Intersects(g, p2)}):
 * the pushed cover is the union of the branch envelopes.
 *
 * <p>{@code ST_Disjoint} is deliberately NOT pushed down: its result set is the
 * rows that do NOT overlap the envelope, so the overlap-only bbox/Z2 domains
 * this connector injects would prune away exactly the files holding the answer.
 * Disjoint predicates fall through to the delegate unchanged and are evaluated
 * row-by-row.
 */
public class SpatialConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LoggerFactory.getLogger(SpatialConnectorMetadata.class);

    /** Ordered sub-field names of a {@code __<X>_bbox__} struct column. */
    private static final List<String> BBOX_FIELDS = List.of("xmin", "ymin", "xmax", "ymax");

    // Cached reflective accessors for foreign-classloader JTS Geometry/Envelope values
    // (see envelopeOf). Keyed by concrete Class so each lookup runs at most once.
    private static final Map<Class<?>, Method> GEOM_ENVELOPE_METHOD = new ConcurrentHashMap<>();
    private static final Map<Class<?>, Method[]> ENVELOPE_BOUND_METHODS = new ConcurrentHashMap<>();

    private final ConnectorMetadata delegate;
    private final GeoMesaColumnCatalog geomCatalog;

    /** Result of locating a spatial constraint in a constraint expression: the
     *  query envelope(s), the spatial function's name (lowercased ASCII; {@code
     *  "$or"} for a disjunction of spatial predicates), and the name of the
     *  geometry column the constraint is filtering on. A single predicate
     *  yields one envelope; an OR of predicates on the same geom yields one
     *  envelope per branch — matching rows lie within their union. */
    record SpatialMatch(List<Envelope> envelopes, String functionName, String geomName) {
        SpatialMatch(Envelope envelope, String functionName, String geomName) {
            this(List.of(envelope), functionName, geomName);
        }
    }

    /** Result of the bbox-pattern reconstruction path: the reconstructed envelope
     *  and the geom-column name parsed from the bbox struct parent variable. */
    record BboxPatternMatch(Envelope envelope, String geomName) {}

    /**
     * Wraps a delegate metadata with spatial-predicate pushdown.
     *
     * @param delegate the underlying iceberg metadata
     * @param geomCatalog the shared geometry-column catalog
     */
    public SpatialConnectorMetadata(ConnectorMetadata delegate,
                                    GeoMesaColumnCatalog geomCatalog) {
        this.delegate = delegate;
        this.geomCatalog = geomCatalog;
    }

    /** Resolve the per-geom-column descriptor map for the given table handle.
     *  Lazy-populates the shared catalog using this metadata's delegate. */
    private Map<String, GeometryColumn> geomsFor(ConnectorSession session,
                                                 ConnectorTableHandle handle) {
        SchemaTableName tn = delegate.getTableName(session, handle);
        return geomCatalog.resolve(tn, session, handle, delegate);
    }

    /**
     * Injects spatial range constraints into the filter for spatial predicates.
     *
     * @param session the connector session
     * @param handle the table handle
     * @param constraint the filter constraint
     * @return the constraint application result with injected spatial domains, or empty
     */
    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {

        // Walk the filter expression for ALL ST_* spatial calls (not just the first).
        // Per-geom routing: each ST_* on column X uses X's bbox + partition companions.
        List<SpatialMatch> matches = findAllSpatialMatches(constraint.getExpression());

        // If no ST_*, try the bbox-pattern reconstruction at the top level — this fires
        // when the planner has already lowered a spatial call to its 4-predicate bbox
        // struct comparisons (re-entry on a second planning iteration). The reconstructed
        // envelope is anchored to whichever geom's bbox struct the comparisons reference.
        if (matches.isEmpty()) {
            List<BboxPatternMatch> bboxes = tryExtractBboxPatternMatches(constraint.getExpression());
            if (bboxes.isEmpty()) return delegate.applyFilter(session, handle, constraint);
            matches = bboxes.stream()
                .map(bp -> new SpatialMatch(bp.envelope(), "bbox_pattern", bp.geomName()))
                .toList();
        }

        Map<String, GeometryColumn> geoms = geomsFor(session, handle);
        if (geoms.isEmpty()) return delegate.applyFilter(session, handle, constraint);

        Map<ColumnHandle, Domain> domains = new HashMap<>();
        List<BboxHandles> injectedBboxes = new ArrayList<>();
        List<SpatialPartitionHandle> injectedPartitions = new ArrayList<>();

        for (SpatialMatch match : matches) {
            GeometryColumn g = geoms.get(match.geomName());
            if (g == null) continue;  // ST_* on a non-geom column → leave in residual

            // The four bbox sub-field domains are ANDed by the engine, so a
            // multi-envelope (OR) match can only push their combined bounds.
            // Partition ranges live in a single Domain and are pushed per
            // envelope, preserving the union's selectivity.
            Envelope env = new Envelope(match.envelopes().get(0));
            match.envelopes().forEach(env::expandToInclude);

            // Bbox sub-field domains for per-file Parquet-stat pruning.
            g.bbox().ifPresent(bbox -> {
                // Skip re-injection if the planner round-tripped this already.
                if (constraint.getSummary().getDomains().map(d -> d.containsKey(bbox.xmax())).orElse(false)) {
                    return;
                }
                domains.merge(bbox.xmax(), realGreaterThanOrEqual(pruneSafeLowerBound(env.getMinX())), Domain::intersect);
                domains.merge(bbox.xmin(), realLessThanOrEqual(pruneSafeUpperBound(env.getMaxX())),    Domain::intersect);
                domains.merge(bbox.ymax(), realGreaterThanOrEqual(pruneSafeLowerBound(env.getMinY())), Domain::intersect);
                domains.merge(bbox.ymin(), realLessThanOrEqual(pruneSafeUpperBound(env.getMaxY())),    Domain::intersect);
                injectedBboxes.add(bbox);
            });

            // Spatial-partition pushdown for manifest-list pruning.
            g.partition().ifPresent(sp -> {
                List<Range> ranges = buildPartitionRanges(sp, match.envelopes());
                if (!ranges.isEmpty()) {
                    domains.merge(sp.column(),
                        Domain.create(SortedRangeSet.copyOf(VarcharType.VARCHAR, ranges), false),
                        Domain::intersect);
                    injectedPartitions.add(sp);
                }
            });
        }

        if (domains.isEmpty()) {
            return delegate.applyFilter(session, handle, constraint);
        }

        TupleDomain<ColumnHandle> augmentedSummary =
            constraint.getSummary().intersect(TupleDomain.withColumnDomains(domains));
        Constraint augmented = new Constraint(augmentedSummary, constraint.getExpression(),
            constraint.getAssignments());
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> delegateResult =
            delegate.applyFilter(session, handle, augmented);

        // Empty = steady state: a prior round absorbed the injected domains into
        // the handle. The log is the only trace if a delegate ever rejects them.
        if (delegateResult.isEmpty()) {
            LOG.atDebug()
                .setMessage("applyFilter: delegate returned empty with {} injected spatial domain(s) on {}")
                .addArgument(domains.size())
                .addArgument(() -> delegate.getTableName(session, handle))
                .log();
            return Optional.empty();
        }
        ConstraintApplicationResult<ConnectorTableHandle> dr = delegateResult.get();

        TupleDomain<ColumnHandle> cleanedRemaining =
            stripInjectedDomains(dr.getRemainingFilter(), injectedBboxes, injectedPartitions);

        // orElse replicates the SPI default ("no claim" keeps the original); the
        // ST_* predicate must stay row-level — the injected domains only prune.
        return Optional.of(new ConstraintApplicationResult<>(
            dr.getHandle(),
            cleanedRemaining,
            dr.getRemainingExpression().orElse(constraint.getExpression()),
            dr.isPrecalculateStatistics()));
    }

    /**
     * Builds the {@code SortedRangeSet} entries for a spatial-partition column over
     * the query envelope(s) — one cover per envelope, unioned by the range set
     * (overlaps are merged by {@code SortedRangeSet.copyOf}). Z2 emits contiguous
     * closed ranges; XZ2 emits a hybrid of wide ranges (level-0 partition-grid
     * cells) and point-equality singletons (higher-level cells). The singleton must
     * be {@code Range.equal} rather than a zero-width {@code Range.range}: when a
     * zero-width range is AND-combined with all four bbox sub-field predicates,
     * Iceberg's metadata evaluator over-prunes files whose per-column stats each
     * pass individually, silently dropping matching rows (observed on Trino 476 +
     * Iceberg 1.9.1; any 3-of-4 bbox combination is fine — only the full
     * conjunction triggers it).
     */
    private static List<Range> buildPartitionRanges(SpatialPartitionHandle sp, List<Envelope> envelopes) {
        List<Range> ranges = new ArrayList<>();
        for (Envelope env : envelopes) {
            List<HexRange> hexRanges = switch (sp.kind()) {
                case Z2  -> SpatialIndexRanges.z2Ranges(env);
                case XZ2 -> SpatialIndexRanges.xz2Ranges(env);
            };
            for (HexRange r : hexRanges) {
                if (r.lower().equals(r.upper())) {
                    ranges.add(Range.equal(VarcharType.VARCHAR, Slices.utf8Slice(r.lower())));
                } else {
                    ranges.add(Range.range(VarcharType.VARCHAR,
                        Slices.utf8Slice(r.lower()), true, Slices.utf8Slice(r.upper()), true));
                }
            }
        }
        return ranges;
    }

    /**
     * Strips the bbox sub-field and spatial-partition domains we injected from the
     * delegate's {@code remainingFilter}. They aren't projected into the scan output,
     * so leaving them in would cause {@code PushPredicateIntoTableScan} to fail with a
     * null column-mapping error. Row-level correctness is preserved by the original
     * spatial predicate, which remains in {@code remainingExpression}.
     */
    private static TupleDomain<ColumnHandle> stripInjectedDomains(
            TupleDomain<ColumnHandle> remainingFilter,
            List<BboxHandles> injectedBboxes,
            List<SpatialPartitionHandle> injectedPartitions) {
        return remainingFilter.getDomains()
            .map(remaining -> {
                Map<ColumnHandle, Domain> filtered = new HashMap<>(remaining);
                for (BboxHandles bbox : injectedBboxes) {
                    filtered.remove(bbox.xmin()); filtered.remove(bbox.ymin());
                    filtered.remove(bbox.xmax()); filtered.remove(bbox.ymax());
                }
                for (SpatialPartitionHandle sp : injectedPartitions) {
                    filtered.remove(sp.column());
                }
                return filtered.isEmpty()
                    ? TupleDomain.<ColumnHandle>all()
                    : TupleDomain.withColumnDomains(filtered);
            })
            .orElse(remainingFilter);
    }

    /** Narrow to REAL rounding toward -∞ — round-to-nearest can land inside the
     *  query envelope and prune files containing matching rows. */
    static float pruneSafeLowerBound(double value) {
        float f = (float) value;
        return f > value ? Math.nextDown(f) : f;
    }

    /** Narrow to REAL rounding toward +∞; see {@link #pruneSafeLowerBound}. */
    static float pruneSafeUpperBound(double value) {
        float f = (float) value;
        return f < value ? Math.nextUp(f) : f;
    }

    /** Builds a REAL-typed Domain for "column >= value". */
    private static Domain realGreaterThanOrEqual(float value) {
        long bits = (long) Float.floatToIntBits(value);
        return Domain.create(SortedRangeSet.copyOf(RealType.REAL,
            List.of(Range.greaterThanOrEqual(RealType.REAL, bits))), false);
    }

    /** Builds a REAL-typed Domain for "column <= value". */
    private static Domain realLessThanOrEqual(float value) {
        long bits = (long) Float.floatToIntBits(value);
        return Domain.create(SortedRangeSet.copyOf(RealType.REAL,
            List.of(Range.lessThanOrEqual(RealType.REAL, bits))), false);
    }

    /**
     * Function names whose row-side argument is a geometry and whose query-side
     * argument is a geometry literal — the shape this connector can extract an
     * envelope from for Z2/bbox pushdown. Every listed predicate implies the two
     * geometries intersect (in either argument order), so overlap-only pruning is
     * sound: a matching row's bbox must overlap the query envelope. {@code
     * ST_Disjoint} is excluded on purpose — see the class javadoc. {@code
     * ST_Distance}-based predicates carry no extractable envelope; the datastore
     * lowers DWITHIN to bbox comparisons the pattern-reconstruction path picks up.
     */
    private static boolean isSpatialPredicateName(String fn) {
        return switch (fn) {
            case "st_intersects", "st_within", "st_contains",
                 "st_crosses", "st_touches", "st_overlaps", "st_equals" -> true;
            default -> false;
        };
    }

    /**
     * Extracts the envelope of the geometry-constant argument of a spatial
     * function call (see {@link #isSpatialPredicateName}; the caller has
     * already verified the function name). Used by collectSpatialMatches.
     */
    Optional<Envelope> tryExtractEnvelope(Call call) {
        // Look for a geometry argument. Trino constant-folds ST_GeometryFromText('WKT')
        // to a Geometry constant before PushPredicateIntoTableScan fires, so Form 1 is
        // the normal path. Form 2 handles the un-folded call defensively.
        // Name-based type check (StandardTypes.GEOMETRY) avoids classloader identity
        // mismatch between our bundled GeometryType and the one in Trino's registry.
        for (ConnectorExpression arg : call.getArguments()) {
            // Form 1: Geometry constant (already folded by the optimizer).
            if (arg instanceof Constant constant
                    && io.trino.spi.type.StandardTypes.GEOMETRY.equals(constant.getType().getBaseName())) {
                return envelopeOf(constant.getValue());
            }
            // Form 2: ST_GeometryFromText(varchar_literal) not yet folded.
            if (arg instanceof Call wktCall) {
                String wktFn = wktCall.getFunctionName().getName().toLowerCase(Locale.ROOT);
                if ((wktFn.equals("st_geometryfromtext") || wktFn.equals("st_geomfromtext"))
                        && !wktCall.getArguments().isEmpty()
                        && wktCall.getArguments().get(0) instanceof Constant wktConst) {
                    try {
                        Slice wktSlice = (Slice) wktConst.getValue();
                        Geometry geom = new WKTReader().read(wktSlice.toStringUtf8());
                        return Optional.of(geom.getEnvelopeInternal());
                    } catch (Exception e) {
                        LOG.debug("Could not parse WKT literal into an envelope: " + e.getMessage());
                        return Optional.empty();
                    }
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Extract the bounding-box {@link Envelope} of a folded Geometry-constant value.
     *
     * <p>The value arrives in one of two shapes. Older optimizers handed us the type's
     * serialized {@link Slice}; Trino's current optimizer constant-folds
     * {@code ST_GeometryFromText(...)} into an in-memory JTS {@code Geometry} object. That
     * object's {@code Class} is loaded by the geospatial plugin's {@code PluginClassLoader},
     * and Trino's per-plugin classloader isolation makes it NOT identity-equal to our own
     * bundled JTS {@code Geometry} (same bytecode, different loader) — while {@code Geometry}/
     * JTS is not part of the connector SPI. So neither a cast to our {@link Geometry} nor to
     * {@link Slice} works, and there is no shared, typed handle on the object. Reflection over
     * the stable JTS API is the only classloader-agnostic option; the {@link Method} handles
     * are cached per concrete {@code Class}, so resolution happens once (and only at planning
     * time). Doubles returned by the bound accessors cross classloaders cleanly.
     */
    private static Optional<Envelope> envelopeOf(Object value) {
        if (value == null) return Optional.empty();
        try {
            // Legacy path: value is the type's serialized Slice — our bundled serde reads it.
            if (value instanceof Slice slice) {
                return Optional.of(JtsGeometrySerde.deserialize(slice).getEnvelopeInternal());
            }
            // Foreign-classloader JTS Geometry: reflect getEnvelopeInternal() then the four
            // bound accessors and rebuild the envelope from primitives.
            Method envelopeMethod =
                GEOM_ENVELOPE_METHOD.computeIfAbsent(value.getClass(), c -> lookupMethod(c, "getEnvelopeInternal"));
            Object envelope = envelopeMethod.invoke(value);
            Method[] bounds = ENVELOPE_BOUND_METHODS.computeIfAbsent(envelope.getClass(), c -> new Method[]{
                lookupMethod(c, "getMinX"), lookupMethod(c, "getMaxX"),
                lookupMethod(c, "getMinY"), lookupMethod(c, "getMaxY")});
            return Optional.of(new Envelope(
                (double) bounds[0].invoke(envelope), (double) bounds[1].invoke(envelope),
                (double) bounds[2].invoke(envelope), (double) bounds[3].invoke(envelope)));
        } catch (Throwable e) {
            // Unexpected geometry-constant shape: skip pushdown, fall back to a correct
            // (if unpruned) scan. A systematic failure here (e.g. a Trino upgrade changing the
            // folded-geometry representation) silently disables ALL spatial pruning, so warn
            // loudly every time — it fires at most once per planned query, and a single
            // warning is too easy to lose in a long-lived server's logs.
            LOG.warn("Could not extract envelope from a Geometry constant; spatial pruning "
                + "will be skipped for this predicate: " + e);
            return Optional.empty();
        }
    }

    /** {@link Class#getMethod} with the checked exception rewrapped so it works inside a
     *  {@code computeIfAbsent} mapping function; failures surface to envelopeOf's catch. */
    private static Method lookupMethod(Class<?> c, String name) {
        try {
            return c.getMethod(name);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("No method " + name + "() on " + c.getName(), e);
        }
    }

    /** Like {@link #tryExtractEnvelope} but also returns the function name and the
     *  geom-column name extracted from the spatial-arg position. Convenience entry
     *  point retained for test fixtures and as the seam for a future Trino-aware
     *  row-level shortcut; the production path uses {@link #findAllSpatialMatches}. */
    Optional<SpatialMatch> findSpatialMatch(ConnectorExpression expr) {
        return findAllSpatialMatches(expr).stream().findFirst();
    }

    /** Collects ALL spatial matches in an expression tree (recursing through AND
     *  nodes), not just the first one. Lets multi-geom predicate queries push
     *  pruning on every geom column independently. */
    List<SpatialMatch> findAllSpatialMatches(ConnectorExpression expr) {
        List<SpatialMatch> acc = new ArrayList<>();
        collectSpatialMatches(expr, acc);
        return acc;
    }

    private void collectSpatialMatches(ConnectorExpression expr, List<SpatialMatch> acc) {
        if (!(expr instanceof Call call)) return;
        String fn = call.getFunctionName().getName().toLowerCase(Locale.ROOT);
        if (isSpatialPredicateName(fn)) {
            Optional<Envelope> envOpt = tryExtractEnvelope(call);
            Optional<String> geomNameOpt = extractGeomColumnName(call);
            if (envOpt.isPresent() && geomNameOpt.isPresent()) {
                acc.add(new SpatialMatch(envOpt.get(), fn, geomNameOpt.get()));
            }
            return;  // don't recurse into nested ST_*
        }
        // Disjunctions prune only when EVERY branch constrains the same geometry
        // column; the sound cover is then the union of the branch envelopes.
        if (OR_FUNCTION_NAME.equals(call.getFunctionName())) {
            acc.addAll(orSpatialMatches(call));
            return;
        }
        // Otherwise descend through CONJUNCTIONS only. A spatial predicate under
        // $not (or any other combinator) is not a top-level constraint on the
        // result set: injecting its envelope would prune rows that satisfy the
        // query through another path (e.g. NOT(WITHIN(t)) — matching rows live
        // outside t). Found by the datastore filter-parity suite.
        if (!AND_FUNCTION_NAME.equals(call.getFunctionName())) return;
        for (ConnectorExpression arg : call.getArguments()) collectSpatialMatches(arg, acc);
    }

    /**
     * Matches for a disjunction: rows satisfying {@code b1 OR b2 OR ...} lie in
     * the union of the branch envelopes, but ONLY for a geometry column that is
     * spatially constrained in <em>every</em> branch — a row can satisfy the OR
     * through a branch that says nothing about a given geom (e.g.
     * {@code DISJOINT(a) OR CROSSES(b)} — rows matching only the disjoint side
     * live OUTSIDE b's envelope; found by the datastore filter-parity suite).
     * Nested ANDs/ORs are handled by recursion through
     * {@link #findAllSpatialMatches}.
     */
    private List<SpatialMatch> orSpatialMatches(Call or) {
        // Per-branch, per-geom envelopes; a branch with no spatial match on any
        // geom vetoes the whole disjunction (its rows are unconstrained).
        Map<String, List<Envelope>> unionByGeom = null;
        for (ConnectorExpression branch : or.getArguments()) {
            Map<String, List<Envelope>> branchByGeom = new LinkedHashMap<>();
            for (SpatialMatch m : findAllSpatialMatches(branch)) {
                branchByGeom.computeIfAbsent(m.geomName(), k -> new ArrayList<>())
                    .addAll(m.envelopes());
            }
            if (branchByGeom.isEmpty()) return List.of();
            if (unionByGeom == null) {
                unionByGeom = branchByGeom;
            } else {
                // Keep only geoms constrained in every branch seen so far.
                unionByGeom.keySet().retainAll(branchByGeom.keySet());
                for (Map.Entry<String, List<Envelope>> e : branchByGeom.entrySet()) {
                    List<Envelope> existing = unionByGeom.get(e.getKey());
                    if (existing != null) existing.addAll(e.getValue());
                }
            }
            if (unionByGeom.isEmpty()) return List.of();
        }
        List<SpatialMatch> out = new ArrayList<>();
        for (Map.Entry<String, List<Envelope>> e : unionByGeom.entrySet()) {
            out.add(new SpatialMatch(List.copyOf(e.getValue()), "$or", e.getKey()));
        }
        return out;
    }

    /** Extracts the geom-column name from a spatial-function call. The expected
     *  shape is {@code ST_*(ST_GeomFromBinary(<var>), <literal>)} — unwrap the
     *  {@code st_geomfrombinary} call to find the row-side {@link Variable}.
     *  Falls back to looking for a direct Variable arg (handles tests that build
     *  the expression without the wrap, and any future planner shape changes). */
    static Optional<String> extractGeomColumnName(Call call) {
        for (ConnectorExpression arg : call.getArguments()) {
            // Direct variable arg.
            if (arg instanceof Variable v) {
                return Optional.of(v.getName());
            }
            // Wrapped: ST_GeomFromBinary(var).
            if (arg instanceof Call inner
                    && "st_geomfrombinary".equals(inner.getFunctionName().getName().toLowerCase(Locale.ROOT))
                    && !inner.getArguments().isEmpty()
                    && inner.getArguments().get(0) instanceof Variable v) {
                return Optional.of(v.getName());
            }
        }
        return Optional.empty();
    }

    // ── Bbox-pattern reconstruction ───────────────────────────────────────────

    /** Single-match convenience over {@link #tryExtractBboxPatternMatches}; retained
     *  for test fixtures. */
    Optional<BboxPatternMatch> tryExtractBboxPatternMatch(ConnectorExpression expr) {
        return tryExtractBboxPatternMatches(expr).stream().findFirst();
    }

    /** Reconstructs one envelope per geometry column whose bbox struct has all four
     *  bound comparisons present as top-level conjuncts. Per-geom (not first-geom-
     *  wins): multi-geom queries lower each spatial predicate to its own bbox
     *  comparisons, and each deserves independent pruning. */
    List<BboxPatternMatch> tryExtractBboxPatternMatches(ConnectorExpression expr) {
        Map<String, double[]> bounds = new java.util.LinkedHashMap<>();
        collectBboxBounds(expr, bounds);
        List<BboxPatternMatch> out = new ArrayList<>();
        for (Map.Entry<String, double[]> e : bounds.entrySet()) {
            double[] b = e.getValue();
            if (!Double.isNaN(b[0]) && !Double.isNaN(b[1])
                    && !Double.isNaN(b[2]) && !Double.isNaN(b[3])) {
                out.add(new BboxPatternMatch(new Envelope(b[0], b[1], b[2], b[3]), e.getKey()));
            }
        }
        return out;
    }

    /** Recursively walks the expression tree filling in per-geom bbox bounds, keyed
     *  by the parent column name of the bbox struct (e.g. {@code __center_bbox__} →
     *  {@code center}). Bounds order: {xmax≥, xmin≤, ymax≥, ymin≤} → {minX, maxX,
     *  minY, maxY} of the reconstructed envelope. */
    private void collectBboxBounds(ConnectorExpression expr, Map<String, double[]> bounds) {
        if (!(expr instanceof Call call)) return;
        boolean gte = GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName());
        boolean lte = LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName());
        if (!gte && !lte) {
            // Descend through conjunctions only: bbox comparisons under $or/$not (or
            // inside a CASE shortcut) are not top-level constraints — reconstructing
            // an envelope from them would over-prune the other branches. Found by the
            // datastore filter-parity suite (DISJOINT(a) OR CROSSES(b) lost rows).
            if (!AND_FUNCTION_NAME.equals(call.getFunctionName())) return;
            for (ConnectorExpression arg : call.getArguments()) collectBboxBounds(arg, bounds);
            return;
        }
        if (call.getArguments().size() != 2) return;
        ConnectorExpression left  = call.getArguments().get(0);
        ConnectorExpression right = call.getArguments().get(1);
        FieldRef ref  = bboxFieldRef(left);
        Double    val = constantDouble(right);
        if (ref == null || val == null) {
            ref = bboxFieldRef(right);
            val = constantDouble(left);
            if (ref != null && val != null) { boolean t = gte; gte = lte; lte = t; }
        }
        if (ref == null || val == null) return;

        double[] b = bounds.computeIfAbsent(ref.geomName(),
            k -> new double[]{Double.NaN, Double.NaN, Double.NaN, Double.NaN});
        switch (ref.fieldName()) {
            case "xmax" -> { if (gte) b[0] = val; }
            case "xmin" -> { if (lte) b[1] = val; }
            case "ymax" -> { if (gte) b[2] = val; }
            case "ymin" -> { if (lte) b[3] = val; }
        }
    }

    private record FieldRef(String geomName, String fieldName) {}

    /** If expr is a FieldDereference (or CAST-wrapped FieldDereference) on a bbox struct
     *  column, returns the geom-column name (parsed from the struct column's name like
     *  {@code __<X>_bbox__}) AND the sub-field name. Returns null otherwise. */
    private FieldRef bboxFieldRef(ConnectorExpression expr) {
        if (expr instanceof Call c
                && CAST_FUNCTION_NAME.equals(c.getFunctionName())
                && c.getArguments().size() == 1) {
            expr = c.getArguments().get(0);
        }
        if (!(expr instanceof FieldDereference fd)) return null;
        if (!(fd.getTarget().getType() instanceof RowType rowType)) return null;
        List<String> fieldNames = rowType.getFields().stream()
            .map(f -> f.getName().orElse(""))
            .toList();
        if (!fieldNames.equals(BBOX_FIELDS)) return null;
        int idx = fd.getField();
        if (idx < 0 || idx >= fieldNames.size()) return null;

        // Recover the geom column name from the struct's parent Variable. Trino's plan
        // symbols strip leading/trailing underscores ("__center_bbox__" → "center_bbox"),
        // so we strip the "_bbox" suffix to recover the geom name.
        String parentName = parentVariableName(fd.getTarget());
        if (parentName == null) return null;
        String stripped = parentName.startsWith("__") && parentName.endsWith("__")
            ? parentName.substring(2, parentName.length() - 2)
            : parentName;
        if (!stripped.endsWith("_bbox")) return null;
        String geomName = stripped.substring(0, stripped.length() - "_bbox".length());
        return new FieldRef(geomName, fieldNames.get(idx));
    }

    private static String parentVariableName(ConnectorExpression target) {
        if (target instanceof Variable v) return v.getName();
        return null;
    }

    /** Extracts a double value from a Constant (handles DOUBLE and REAL bit-encoding). */
    private Double constantDouble(ConnectorExpression expr) {
        // Unwrap CAST on constant: e.g., CAST(-80.0 AS REAL)
        if (expr instanceof Call c
                && CAST_FUNCTION_NAME.equals(c.getFunctionName())
                && c.getArguments().size() == 1) {
            expr = c.getArguments().get(0);
        }
        if (!(expr instanceof Constant c)) return null;
        Object val = c.getValue();
        if (val == null) return null;
        if (val instanceof Double d) return d;
        if (val instanceof Long l) {
            // REAL values are stored as IEEE 754 bits in a long
            if (io.trino.spi.type.StandardTypes.REAL.equals(c.getType().getBaseName())) {
                return (double) Float.intBitsToFloat(l.intValue());
            }
            return l.doubleValue();
        }
        return null;
    }

    // ---- Delegation for all other ConnectorMetadata methods ----

    // Schema operations
    /**
     * Returns whether the named schema exists.
     *
     * @param session the connector session
     * @param schemaName the schema name
     * @return true if the schema exists
     */
    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName) {
        return delegate.schemaExists(session, schemaName);
    }

    /**
     * Returns the schema names visible to the session.
     *
     * @param session the connector session
     * @return the schema names
     */
    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return delegate.listSchemaNames(session);
    }

    /**
     * Returns the properties of the named schema.
     *
     * @param session the connector session
     * @param schemaName the schema name
     * @return the schema properties
     */
    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName) {
        return delegate.getSchemaProperties(session, schemaName);
    }

    /**
     * Returns the owner of the named schema.
     *
     * @param session the connector session
     * @param schemaName the schema name
     * @return the schema owner, if any
     */
    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, String schemaName) {
        return delegate.getSchemaOwner(session, schemaName);
    }

    // Table lookup and metadata
    /**
     * Returns the table handle for the given name and version range.
     *
     * @param session the connector session
     * @param tableName the schema-qualified table name
     * @param startVersion the start table version, if any
     * @param endVersion the end table version, if any
     * @return the table handle
     */
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
        return delegate.getTableHandle(session, tableName, startVersion, endVersion);
    }

    /**
     * Returns the metadata for the given table.
     *
     * @param session the connector session
     * @param table the table handle
     * @return the table metadata
     */
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
                                                   ConnectorTableHandle table) {
        return delegate.getTableMetadata(session, table);
    }

    /**
     * Returns the schema-qualified name for the given table handle.
     *
     * @param session the connector session
     * @param table the table handle
     * @return the schema-qualified table name
     */
    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table) {
        return delegate.getTableName(session, table);
    }

    /**
     * Returns the connector-level properties of the given table.
     *
     * @param session the connector session
     * @param table the table handle
     * @return the table properties
     */
    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session,
                                                       ConnectorTableHandle table) {
        return delegate.getTableProperties(session, table);
    }

    /**
     * Returns connector-specific info for the given table.
     *
     * @param session the connector session
     * @param table the table handle
     * @return the table info, if any
     */
    @Override
    public Optional<Object> getInfo(ConnectorSession session, ConnectorTableHandle table) {
        return delegate.getInfo(session, table);
    }

    /**
     * Returns the system table for the given name, if any.
     *
     * @param session the connector session
     * @param tableName the schema-qualified table name
     * @return the system table, if any
     */
    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName) {
        return delegate.getSystemTable(session, tableName);
    }

    // Table listing
    /**
     * Returns the tables in the given schema (or all schemas).
     *
     * @param session the connector session
     * @param schemaName the schema name, if any
     * @return the schema-qualified table names
     */
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        return delegate.listTables(session, schemaName);
    }

    /**
     * Returns the relation types for the given schema (or all schemas).
     *
     * @param session the connector session
     * @param schemaName the schema name, if any
     * @return the relation types keyed by name
     */
    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session,
                                                               Optional<String> schemaName) {
        return delegate.getRelationTypes(session, schemaName);
    }

    // Column operations
    /**
     * Returns the column handles for the given table, recording its visibility column.
     *
     * @param session the connector session
     * @param tableHandle the table handle
     * @return the column handles keyed by column name
     */
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                                                      ConnectorTableHandle tableHandle) {
        Map<String, ColumnHandle> handles = delegate.getColumnHandles(session, tableHandle);
        // Record the table's visibility column at analysis time so the Trino-layer
        // VisibilityAccessControl (which only receives a SchemaTableName, no session)
        // can read it warm. Best-effort: never let bookkeeping break column resolution.
        try {
            geomCatalog.recordVisibilityColumn(delegate.getTableName(session, tableHandle),
                handles.keySet());
        } catch (RuntimeException e) {
            LOG.debug("Could not record visibility column: {}", e.getMessage());
        }
        return handles;
    }

    /**
     * Returns the metadata for the given column.
     *
     * @param session the connector session
     * @param tableHandle the table handle
     * @param columnHandle the column handle
     * @return the column metadata
     */
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                            ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle) {
        return delegate.getColumnMetadata(session, tableHandle, columnHandle);
    }

    /**
     * Returns the columns for tables matching the given prefix.
     *
     * @param session the connector session
     * @param prefix the schema-table prefix
     * @return the column metadata lists keyed by table name
     */
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                                                                       SchemaTablePrefix prefix) {
        return delegate.listTableColumns(session, prefix);
    }

    /**
     * Streams the columns for tables matching the given prefix.
     *
     * @param session the connector session
     * @param prefix the schema-table prefix
     * @return an iterator over per-table column metadata
     */
    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session,
                                                             SchemaTablePrefix prefix) {
        return delegate.streamTableColumns(session, prefix);
    }

    /**
     * Streams the columns for relations in the given schema (or all schemas).
     *
     * @param session the connector session
     * @param schemaName the schema name, if any
     * @param relationFilter a filter over the candidate relation names
     * @return an iterator over per-relation column metadata
     */
    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session,
            Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter) {
        return delegate.streamRelationColumns(session, schemaName, relationFilter);
    }

    /**
     * Streams the comments for relations in the given schema (or all schemas).
     *
     * @param session the connector session
     * @param schemaName the schema name, if any
     * @param relationFilter a filter over the candidate relation names
     * @return an iterator over per-relation comment metadata
     */
    @Override
    public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session,
            Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter) {
        return delegate.streamRelationComments(session, schemaName, relationFilter);
    }

    // Query optimization — these are the most impactful missing delegates
    /**
     * Delegates projection pushdown to the underlying connector.
     *
     * @param session the connector session
     * @param handle the table handle
     * @param projections the projection expressions
     * @param assignments the variable-to-column assignments
     * @return the projection application result, or empty
     */
    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments) {
        return delegate.applyProjection(session, handle, projections, assignments);
    }

    /**
     * Delegates limit pushdown to the underlying connector.
     *
     * @param session the connector session
     * @param handle the table handle
     * @param limit the row limit
     * @return the limit application result, or empty
     */
    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session, ConnectorTableHandle handle, long limit) {
        return delegate.applyLimit(session, handle, limit);
    }

    /**
     * Delegating override is load-bearing: without it the default
     * {@code ConnectorMetadata.applyAggregation} returns empty, which means
     * "no aggregation pushdown supported." Trino then falls back to launching
     * the scan and aggregating row by row. The stock iceberg connector's
     * applyAggregation is what enables {@code SELECT count(*)} (and other
     * simple aggregates) to be answered from a sum of per-file
     * {@code record_count} values in the manifest list — no splits, no scan,
     * roughly 50ms instead of minutes on multi-billion-row tables.
     *
     * @param session the connector session
     * @param handle the table handle
     * @param aggregates the aggregate functions
     * @param assignments the variable-to-column assignments
     * @param groupingSets the grouping sets
     * @return the aggregation application result, or empty
     */
    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session, ConnectorTableHandle handle,
            List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets) {
        return delegate.applyAggregation(session, handle, aggregates, assignments, groupingSets);
    }

    /**
     * Delegates top-N pushdown to the underlying connector.
     *
     * @param session the connector session
     * @param handle the table handle
     * @param topNCount the number of rows to retain
     * @param sortItems the sort items
     * @param assignments the variable-to-column assignments
     * @return the top-N application result, or empty
     */
    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session, ConnectorTableHandle handle,
            long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments) {
        return delegate.applyTopN(session, handle, topNCount, sortItems, assignments);
    }

    /**
     * Returns the statistics for the given table.
     *
     * @param session the connector session
     * @param tableHandle the table handle
     * @return the table statistics
     */
    @Override
    public TableStatistics getTableStatistics(ConnectorSession session,
                                              ConnectorTableHandle tableHandle) {
        return delegate.getTableStatistics(session, tableHandle);
    }

    /**
     * Validates a scan against the given table handle.
     *
     * @param session the connector session
     * @param handle the table handle
     */
    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle) {
        delegate.validateScan(session, handle);
    }

    /**
     * Returns whether a read of the given table may be split into multiple sub-queries.
     *
     * @param session the connector session
     * @param tableHandle the table handle
     * @return true if the read may be split into multiple sub-queries
     */
    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session,
                                                            ConnectorTableHandle tableHandle) {
        return delegate.allowSplittingReadIntoMultipleSubQueries(session, tableHandle);
    }

    // Views
    /**
     * Returns the views in the given schema (or all schemas).
     *
     * @param session the connector session
     * @param schemaName the schema name, if any
     * @return the view names
     */
    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName) {
        return delegate.listViews(session, schemaName);
    }

    /**
     * Returns the view definitions in the given schema (or all schemas).
     *
     * @param session the connector session
     * @param schemaName the schema name, if any
     * @return the view definitions keyed by name
     */
    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session,
                                                                  Optional<String> schemaName) {
        return delegate.getViews(session, schemaName);
    }

    /**
     * Returns whether the named relation is a view.
     *
     * @param session the connector session
     * @param viewName the schema-qualified view name
     * @return true if the relation is a view
     */
    @Override
    public boolean isView(ConnectorSession session, SchemaTableName viewName) {
        return delegate.isView(session, viewName);
    }

    /**
     * Returns the definition of the named view, if any.
     *
     * @param session the connector session
     * @param viewName the schema-qualified view name
     * @return the view definition, if any
     */
    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session,
                                                     SchemaTableName viewName) {
        return delegate.getView(session, viewName);
    }

    // Materialized views
    /**
     * Returns the materialized views in the given schema (or all schemas).
     *
     * @param session the connector session
     * @param schemaName the schema name, if any
     * @return the materialized-view names
     */
    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session,
                                                       Optional<String> schemaName) {
        return delegate.listMaterializedViews(session, schemaName);
    }

    /**
     * Returns the materialized-view definitions in the given schema (or all schemas).
     *
     * @param session the connector session
     * @param schemaName the schema name, if any
     * @return the materialized-view definitions keyed by name
     */
    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(
            ConnectorSession session, Optional<String> schemaName) {
        return delegate.getMaterializedViews(session, schemaName);
    }

    /**
     * Returns the definition of the named materialized view, if any.
     *
     * @param session the connector session
     * @param viewName the schema-qualified materialized-view name
     * @return the materialized-view definition, if any
     */
    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(
            ConnectorSession session, SchemaTableName viewName) {
        return delegate.getMaterializedView(session, viewName);
    }

    /**
     * Returns the freshness of the named materialized view.
     *
     * @param session the connector session
     * @param name the schema-qualified materialized-view name
     * @return the materialized-view freshness
     */
    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session,
                                                                  SchemaTableName name) {
        return delegate.getMaterializedViewFreshness(session, name);
    }

    /**
     * Returns the redirected location of the named table, if any.
     *
     * @param session the connector session
     * @param tableName the schema-qualified table name
     * @return the redirected table location, if any
     */
    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session,
                                                          SchemaTableName tableName) {
        return delegate.redirectTable(session, tableName);
    }

    // ── Metadata-only DDL — forwarded so the catalog is manageable through
    //    spatial_iceberg (schema/table/view existence, names, comments,
    //    properties, ownership). These produce NO rows and don't alter column
    //    structure, so they can't create the companion-less-row trap that keeps
    //    INSERT/CTAS/MERGE/refresh and column-level schema evolution blocked
    //    (see SpatialConnectorMetadataDelegationTest#INTENTIONALLY_NOT_FORWARDED).
    //    When DDL drops or renames a table/schema, the GeoMesaColumnCatalog cache
    //    is invalidated (see dropTable/renameTable/dropSchema/renameSchema below)
    //    so a later table reusing the same name doesn't inherit stale
    //    geometry/visibility descriptors. ──

    /**
     * Creates a schema via the delegate connector.
     *
     * @param session the connector session
     * @param schemaName the schema name
     * @param properties the schema properties
     * @param owner the schema owner
     */
    @Override
    public void createSchema(ConnectorSession session, String schemaName,
                             Map<String, Object> properties, TrinoPrincipal owner) {
        delegate.createSchema(session, schemaName, properties, owner);
    }

    /**
     * Drops a schema via the delegate connector and invalidates its cached state.
     *
     * @param session the connector session
     * @param schemaName the schema name
     * @param cascade whether to drop contained relations
     */
    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade) {
        delegate.dropSchema(session, schemaName, cascade);
        geomCatalog.invalidateSchema(schemaName);
    }

    /**
     * Renames a schema via the delegate connector and invalidates the source's cached state.
     *
     * @param session the connector session
     * @param source the source schema name
     * @param target the target schema name
     */
    @Override
    public void renameSchema(ConnectorSession session, String source, String target) {
        delegate.renameSchema(session, source, target);
        geomCatalog.invalidateSchema(source);
    }

    /**
     * Sets the owner of a schema via the delegate connector.
     *
     * @param session the connector session
     * @param schemaName the schema name
     * @param principal the new owner
     */
    @Override
    public void setSchemaAuthorization(ConnectorSession session, String schemaName,
                                       TrinoPrincipal principal) {
        delegate.setSchemaAuthorization(session, schemaName, principal);
    }

    /**
     * Drops a table via the delegate connector and invalidates its cached state.
     *
     * @param session the connector session
     * @param tableHandle the table handle
     */
    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
        SchemaTableName name = delegate.getTableName(session, tableHandle);
        delegate.dropTable(session, tableHandle);
        geomCatalog.invalidate(name);
    }

    /**
     * Renames a table via the delegate connector and invalidates the old and new cached state.
     *
     * @param session the connector session
     * @param tableHandle the table handle
     * @param newTableName the new schema-qualified table name
     */
    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle,
                            SchemaTableName newTableName) {
        SchemaTableName oldName = delegate.getTableName(session, tableHandle);
        delegate.renameTable(session, tableHandle, newTableName);
        geomCatalog.invalidate(oldName);
        geomCatalog.invalidate(newTableName);
    }

    /**
     * Sets a table's comment via the delegate connector.
     *
     * @param session the connector session
     * @param tableHandle the table handle
     * @param comment the new comment, if any
     */
    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle,
                                Optional<String> comment) {
        delegate.setTableComment(session, tableHandle, comment);
    }

    /**
     * Sets a table's properties via the delegate connector.
     *
     * @param session the connector session
     * @param tableHandle the table handle
     * @param properties the properties to set
     */
    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle,
                                   Map<String, Optional<Object>> properties) {
        delegate.setTableProperties(session, tableHandle, properties);
    }

    /**
     * Sets the owner of a table via the delegate connector.
     *
     * @param session the connector session
     * @param tableName the schema-qualified table name
     * @param principal the new owner
     */
    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName,
                                      TrinoPrincipal principal) {
        delegate.setTableAuthorization(session, tableName, principal);
    }

    /**
     * Sets a column's comment via the delegate connector.
     *
     * @param session the connector session
     * @param tableHandle the table handle
     * @param column the column handle
     * @param comment the new comment, if any
     */
    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle,
                                 ColumnHandle column, Optional<String> comment) {
        delegate.setColumnComment(session, tableHandle, column, comment);
    }

    /**
     * Creates a view via the delegate connector.
     *
     * @param session the connector session
     * @param viewName the schema-qualified view name
     * @param definition the view definition
     * @param viewProperties the view properties
     * @param replace whether to replace an existing view
     */
    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName,
                           ConnectorViewDefinition definition, Map<String, Object> viewProperties,
                           boolean replace) {
        delegate.createView(session, viewName, definition, viewProperties, replace);
    }

    /**
     * Drops a view via the delegate connector.
     *
     * @param session the connector session
     * @param viewName the schema-qualified view name
     */
    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName) {
        delegate.dropView(session, viewName);
    }

    /**
     * Renames a view via the delegate connector.
     *
     * @param session the connector session
     * @param source the source schema-qualified view name
     * @param target the target schema-qualified view name
     */
    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target) {
        delegate.renameView(session, source, target);
    }

    /**
     * Sets the owner of a view via the delegate connector.
     *
     * @param session the connector session
     * @param viewName the schema-qualified view name
     * @param principal the new owner
     */
    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName,
                                     TrinoPrincipal principal) {
        delegate.setViewAuthorization(session, viewName, principal);
    }

    /**
     * Sets a view's comment via the delegate connector.
     *
     * @param session the connector session
     * @param viewName the schema-qualified view name
     * @param comment the new comment, if any
     */
    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName,
                               Optional<String> comment) {
        delegate.setViewComment(session, viewName, comment);
    }

    /**
     * Sets a view column's comment via the delegate connector.
     *
     * @param session the connector session
     * @param viewName the schema-qualified view name
     * @param columnName the column name
     * @param comment the new comment, if any
     */
    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName,
                                     String columnName, Optional<String> comment) {
        delegate.setViewColumnComment(session, viewName, columnName, comment);
    }
}
