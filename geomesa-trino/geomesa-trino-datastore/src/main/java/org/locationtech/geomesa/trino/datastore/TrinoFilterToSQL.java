/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.filter.Id;
import org.geotools.api.filter.identity.Identifier;
import org.geotools.api.filter.spatial.BBOX;
import org.geotools.api.filter.spatial.DWithin;
import org.geotools.api.filter.spatial.Intersects;
import org.geotools.api.filter.spatial.Within;
import org.geotools.api.filter.temporal.During;
import org.geotools.api.filter.expression.Expression;
import org.geotools.api.filter.expression.Literal;
import org.geotools.api.filter.expression.PropertyName;
import org.geotools.api.geometry.BoundingBox;
import org.geotools.api.temporal.Period;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.data.jdbc.FilterToSQLException;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Translates GeoTools filters to Trino SQL WHERE clauses against tables that follow
 * this project's spatial-column convention: {@code geom VARBINARY} (raw WKB) plus
 * companions {@code __geom_bbox__ row(xmin, ymin, xmax, ymax)} and
 * {@code __geom_z2__ varchar} / {@code __geom_xz2__ varchar}. If using the spatial
 * catalog, applies bbox/Z2-stat pushdown on top of the same SQL via
 * {@code SpatialConnectorMetadata.applyFilter}.
 *
 * <p>Geom column references are always wrapped in {@code ST_GeomFromBinary(geom)}
 * before being handed to Trino's stock spatial functions.
 */
public class TrinoFilterToSQL extends FilterToSQL {

    private static final Logger LOG = Logger.getLogger(TrinoFilterToSQL.class.getName());

    /**
     * Creates a filter translator that double-quotes identifiers.
     */
    public TrinoFilterToSQL() {
        setSqlNameEscape("\"");  // quote identifiers in base-class comparisons too
    }

    /** Double-quote an identifier, doubling embedded quotes. */
    private static String quoteIdent(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    /** {@code ST_GeometryFromText('<wkt>')} for a geometry, with any single quote in the WKT
     *  doubled. {@link WKTWriter} emits only numerics/keywords/punctuation today (no quotes),
     *  so this is defense-in-depth — it keeps the geometry literal on the same escaping
     *  discipline as identifiers ({@link #quoteIdent}), feature ids, and the visibility literal. */
    private String geomFromText(Geometry geom) {
        return "ST_GeometryFromText('" + wkt.write(geom).replace("'", "''") + "')";
    }

    /** Per-instance, NOT static: {@link WKTWriter} is not thread-safe, and a
     *  {@link TrinoFilterToSQL} is created fresh per query (see
     *  {@code TrinoFeatureSource.encodeFilterSql}), so an instance field is
     *  thread-confined. A shared static writer would corrupt output under
     *  concurrent queries. */
    private final WKTWriter wkt = new WKTWriter();

    /** Approximate meters per degree of latitude (and of longitude at the equator). */
    private static final double METERS_PER_DEGREE = 111_111.0;

    /** Outward margin on the DWITHIN outer bbox, absorbing flat-vs-spherical
     *  projection error so rows whose true distance is just under d aren't excluded. */
    private static final double OUTER_SAFETY_MARGIN = 1.1;

    /** Inward margin on the DWITHIN inner inscribed rectangle, applied conservatively
     *  so the bbox-contained shortcut never produces a false TRUE. */
    private static final double INNER_SAFETY_MARGIN = 0.9;

    /** Half-diagonal-to-half-side factor for a square inscribed in the d-circle. */
    private static final double INSCRIBED_FACTOR = 1.0 / Math.sqrt(2.0);

    /** Latitude (deg) beyond which the flat cos(lat) longitude scaling degenerates: as
     *  {@code cos(lat) -> 0} the per-degree-longitude distance vanishes, so a within-d region
     *  spans (nearly) all longitudes. Past this we use a full-longitude outer band (so the
     *  prefilter never drops matching rows) and drop the inscribed-rectangle shortcut (whose
     *  flat approximation is unsound near the poles), falling through to the exact spherical
     *  distance check. Also avoids the division-by-zero at exactly ±90°. */
    private static final double NEAR_POLE_LAT = 85.0;

    // ── Spatial ───────────────────────────────────────────────────────────────

    /**
     * Translates a BBOX filter into a bbox-overlap predicate.
     *
     * @param filter bbox filter
     * @param extraData caller-supplied context, returned unchanged
     * @return extraData
     */
    @Override
    public Object visit(BBOX filter, Object extraData) {
        String col = filter.getExpression1() instanceof PropertyName pn
            ? pn.getPropertyName() : defaultGeomCol();
        BoundingBox b = filter.getBounds();
        writeBboxOverlap(col, new Envelope(b.getMinX(), b.getMaxX(), b.getMinY(), b.getMaxY()));
        return extraData;
    }

    /**
     * Translates an Intersects filter, with a bbox-overlap prefilter and row-level shortcut.
     *
     * @param filter intersects filter
     * @param extraData caller-supplied context, returned unchanged
     * @return extraData
     */
    @Override
    public Object visit(Intersects filter, Object extraData) {
        Expression e1 = filter.getExpression1();
        Expression e2 = filter.getExpression2();
        Geometry queryGeom = (Geometry) ((Literal) e2).getValue();

        // Fast path: ST_Intersects(point, rect) ⇔ bbox-overlap(bbox(point), rect)
        // because bbox(Point) = Point.
        // Soundness needs BOTH conditions:
        //  - Rectangle query: lines/polygons can have bboxes that overlap a non-
        //    rectangular envelope without g ∩ R ≠ ∅. Equivalence only holds for
        //    axis-aligned rectangles.
        //  - Point data: for any non-point geometry (Line, Polygon, MultiPoint),
        //    bbox(g) is a strict superset of g, so bbox-overlap is necessary but
        //    not sufficient for ST_Intersects.
        if (queryGeom instanceof Polygon p && p.isRectangle() && storedGeometryIsPoint()) {
            String col = e1 instanceof PropertyName pn ? pn.getPropertyName() : defaultGeomCol();
            writeBboxOverlap(col, p.getEnvelopeInternal());
            return extraData;
        }

        // General case: bbox-overlap (pushable to file-level pruning) AND
        // CASE WHEN bbox-contained THEN TRUE ELSE ST_Intersects (row-level shortcut).
        writeIntersectsWithBboxShortcut(e1, e2);
        return extraData;
    }

    /**
     * Translates a Within filter, using bbox-containment for rectangles and exact ST_Within otherwise.
     *
     * @param filter within filter
     * @param extraData caller-supplied context, returned unchanged
     * @return extraData
     */
    @Override
    public Object visit(Within filter, Object extraData) {
        // For axis-aligned rectangular query polygons, bbox⊆rect is equivalent to
        // ST_Within(geom, rect) — no row-level ST_Within evaluation needed at all.
        // For non-rectangular polygons, fall back to bbox-overlap (Z2 pushdown) AND
        // exact ST_Within at row level.
        Expression e1 = filter.getExpression1();
        Expression e2 = filter.getExpression2();
        Geometry queryGeom = (Geometry) ((Literal) e2).getValue();
        if (queryGeom instanceof Polygon p && p.isRectangle()) {
            writeWithinRectangleAsBboxContained(e1, p);
        } else {
            writeWithinNonRectangle(e1, queryGeom);
        }
        return extraData;
    }

    /**
     * Translates a DWithin filter into outer/inner bbox bounds plus an exact spherical distance check.
     *
     * @param filter distance-within filter
     * @param extraData caller-supplied context, returned unchanged
     * @return extraData
     */
    @Override
    public Object visit(DWithin filter, Object extraData) {
        String col = ((PropertyName) filter.getExpression1()).getPropertyName();
        Geometry refGeom = (Geometry) ((Literal) filter.getExpression2()).getValue();
        double distanceMeters = convertToMeters(filter.getDistance(), filter.getDistanceUnits());

        // Treat reference as a point (centroid for non-point geometries — same as before).
        double lon, lat;
        if (refGeom instanceof Point pt) {
            lon = pt.getX();
            lat = pt.getY();
        } else {
            lon = refGeom.getEnvelope().getCentroid().getX();
            lat = refGeom.getEnvelope().getCentroid().getY();
        }

        boolean nearPole = Math.abs(lat) >= NEAR_POLE_LAT;

        // OUTER bbox: bounding box of "every point within d of ref" — necessary-overlap
        // prefilter. The safety margin absorbs flat-vs-spherical projection error so we
        // don't accidentally exclude rows whose true distance is just under d. Near a pole
        // (or when the span would exceed 180°) the within-d region wraps all longitudes, so
        // we use a full-longitude band rather than a bounded (row-dropping) span.
        double outerDegLat = (distanceMeters / METERS_PER_DEGREE) * OUTER_SAFETY_MARGIN;
        double outerDegLon = nearPole ? Double.POSITIVE_INFINITY
            : (distanceMeters / (METERS_PER_DEGREE * Math.cos(Math.toRadians(lat)))) * OUTER_SAFETY_MARGIN;
        Envelope outer = outerDegLon >= 180.0
            ? new Envelope(-180.0, 180.0, lat - outerDegLat, lat + outerDegLat)
            : new Envelope(lon - outerDegLon, lon + outerDegLon, lat - outerDegLat, lat + outerDegLat);

        String ptWkt = wkt.write(refGeom).replace("'", "''");
        String bboxCol = bboxColName(col);
        // Exact spherical distance — always correct, used as the fallback (and, near the poles,
        // the sole) row-level check.
        String distanceCheck = String.format(Locale.ROOT,
            "ST_Distance(to_spherical_geography(ST_GeomFromBinary(%s)),"
            + " to_spherical_geography(ST_GeometryFromText('%s'))) <= %.0f",
            quoteIdent(col), ptWkt, distanceMeters);

        if (nearPole) {
            // The flat inscribed-rectangle shortcut is unsound where longitude lines converge;
            // fall through to the exact distance check for every candidate row.
            write("(" + bboxOverlapSql(bboxCol, outer) + ") AND " + distanceCheck);
        } else {
            // INNER inscribed rectangle: half-sides (d × INNER_SAFETY_MARGIN × INSCRIBED_FACTOR)
            // scaled to lat/lon. Corners land at distance INNER_SAFETY_MARGIN × d from ref, so
            // any bbox(geom) ⊆ this rectangle ⇒ every point of geom is within d ⇒ DWITHIN=TRUE.
            double innerDegLat = (distanceMeters / METERS_PER_DEGREE) * INSCRIBED_FACTOR * INNER_SAFETY_MARGIN;
            double innerDegLon = (distanceMeters / (METERS_PER_DEGREE * Math.cos(Math.toRadians(lat)))) * INSCRIBED_FACTOR * INNER_SAFETY_MARGIN;
            Envelope inner = new Envelope(lon - innerDegLon, lon + innerDegLon,
                                          lat - innerDegLat, lat + innerDegLat);
            // Outer bbox-overlap (file/Z2 pruning) AND CASE WHEN inner-rectangle-contained
            // (sufficient for distance ≤ d) THEN TRUE ELSE exact spherical distance check.
            write("(" + bboxOverlapSql(bboxCol, outer) + ") AND "
                + "CASE WHEN " + bboxContainedSql(bboxCol, inner) + " THEN TRUE"
                + " ELSE " + distanceCheck + " END");
        }
        return extraData;
    }

    // ── Temporal ──────────────────────────────────────────────────────────────

    /**
     * Translates a During filter into a timestamp range predicate.
     *
     * @param filter temporal during filter
     * @param extraData caller-supplied context, returned unchanged
     * @return extraData
     */
    @Override
    public Object visit(During filter, Object extraData) {
        String col = ((PropertyName) filter.getExpression1()).getPropertyName();
        Period period = (Period) ((Literal) filter.getExpression2()).getValue();
        String begin = formatTimestamp(period.getBeginning().getPosition().getDate());
        String end   = formatTimestamp(period.getEnding().getPosition().getDate());
        write(String.format(
            "%s > TIMESTAMP '%s' AND %s < TIMESTAMP '%s'",
            quoteIdent(col), begin, quoteIdent(col), end));
        return extraData;
    }

    // ── Feature ID ────────────────────────────────────────────────────────────

    /**
     * Translates an Id filter into a feature-id IN predicate.
     *
     * @param filter id filter
     * @param extraData caller-supplied context, returned unchanged
     * @return extraData
     */
    @Override
    public Object visit(Id filter, Object extraData) {
        String ids = filter.getIdentifiers().stream()
            .map(Identifier::getID)
            .map(id -> "'" + id.toString().replace("'", "''") + "'")
            .collect(Collectors.joining(", "));
        write("\"__fid__\" IN (" + ids + ")");
        return extraData;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * True iff the FeatureType's geometry column is declared as exactly
     * {@link Point} (not MultiPoint, not a subclass). Used by
     * {@link #visit(Intersects, Object)} to detect when the bbox-overlap +
     * CASE WHEN rewrite collapses to bbox-overlap alone.
     * <p>
     * Strict {@code equals} (not {@code isAssignableFrom}) is required:
     * {@code bbox(MultiPoint)} is the union extent, so the optimization is
     * unsound for MultiPoint even though MultiPoint is "point-like". JTS does
     * not make MultiPoint a Point subclass, so this is also defensive.
     * <p>
     * Returns {@code false} when no FeatureType is set — callers must keep the
     * safe general-case rewrite in that branch.
     */
    private boolean storedGeometryIsPoint() {
        return featureType != null
            && featureType.getGeometryDescriptor() != null
            && Point.class.equals(featureType.getGeometryDescriptor().getType().getBinding());
    }

    /**
     * Emit the 4-clause necessary-overlap predicate against {@code __<col>_bbox__}.
     * Single source of truth for the bbox-overlap shape that SI's
     * {@code tryExtractBboxEnvelope} matches for file-level Z2 / bbox-stat pruning.
     */
    private void writeBboxOverlap(String col, Envelope env) {
        write(bboxOverlapSql(bboxColName(col), env));
    }

    /**
     * The 4-clause "bbox(geom) overlaps env" fragment. Necessary condition for any
     * geom/env interaction, and the exact shape SI's connector reads to push Z2
     * partition pruning + per-file bbox-stat pruning.
     */
    private static String bboxOverlapSql(String bboxCol, Envelope env) {
        String q = quoteIdent(bboxCol);
        return String.format(
            "%s.xmax >= %s AND %s.xmin <= %s" +
            " AND %s.ymax >= %s AND %s.ymin <= %s",
            q, env.getMinX(), q, env.getMaxX(),
            q, env.getMinY(), q, env.getMaxY());
    }

    /** The 4-clause "bbox(geom) fully contained in env" fragment. */
    private static String bboxContainedSql(String bboxCol, Envelope env) {
        String q = quoteIdent(bboxCol);
        return String.format(
            "%s.xmin >= %s AND %s.xmax <= %s" +
            " AND %s.ymin >= %s AND %s.ymax <= %s",
            q, env.getMinX(), q, env.getMaxX(),
            q, env.getMinY(), q, env.getMaxY());
    }

    /** Returns the synthetic bbox struct column name for a given geometry column. */
    private static String bboxColName(String col) {
        return "__" + col + "_bbox__";
    }

    /** Fallback geometry column name when no PropertyName is available. */
    private String defaultGeomCol() {
        return featureType != null && featureType.getGeometryDescriptor() != null
            ? featureType.getGeometryDescriptor().getLocalName()
            : "geom";
    }

    /**
     * For an axis-aligned rectangular query polygon, ST_Within(geom, rect) is
     * exactly equivalent to {@code bbox(geom) ⊆ rect}: bbox⊆rect ⇒ geom⊆rect
     * (since geom⊆bbox), and bbox⊄rect ⇒ geom touches the bbox side that's
     * outside rect ⇒ geom has a point outside rect ⇒ NOT geom⊆rect.
     * <p>So we emit bbox-overlap (for SI's Z2 partition pushdown via the existing
     * pattern matcher) AND bbox-contained (the actual exact predicate). No
     * row-level ST_Within evaluation needed.
     */
    private void writeWithinRectangleAsBboxContained(Expression e1, Polygon rect) {
        String col = e1 instanceof PropertyName pn ? pn.getPropertyName() : defaultGeomCol();
        String bboxCol = bboxColName(col);
        Envelope env = rect.getEnvelopeInternal();
        // bbox-overlap (lets SI reconstruct the envelope and push Z2 ranges) AND
        // bbox-contained (equivalent to ST_Within(geom, rect) for rectangles).
        write("(" + bboxOverlapSql(bboxCol, env) + ") AND (" + bboxContainedSql(bboxCol, env) + ")");
    }

    /**
     * Non-rectangular polygons require the exact ST_Within row-level test —
     * bbox⊆env(polygon) doesn't prove geom⊆polygon since the polygon is a strict
     * subset of its envelope. We still emit bbox-overlap as a leading conjunct so
     * SI's connector can push Z2 partition pruning + bbox file-stat pruning.
     */
    private void writeWithinNonRectangle(Expression e1, Geometry queryGeom) {
        String col = ((PropertyName) e1).getPropertyName();
        String bboxCol = bboxColName(col);
        Envelope env = queryGeom.getEnvelopeInternal();
        write("(" + bboxOverlapSql(bboxCol, env) + ")"
            + " AND ST_Within(ST_GeomFromBinary(" + quoteIdent(col) + "),"
            + " " + geomFromText(queryGeom) + ")");
    }

    /**
     * Emits a two-part predicate that gives both file-level Z2/bbox pruning AND
     * row-level short-circuiting:
     *
     * <pre>
     * (bbox-overlap)  AND  CASE WHEN bbox-contained THEN TRUE ELSE ST_Intersects(...) END
     * </pre>
     *
     * <p>The leading bbox-overlap conjunct is the same shape that
     * {@code SpatialConnectorMetadata.tryExtractBboxEnvelope} already recognizes —
     * the SI connector extracts the envelope from it and pushes Z2 partition
     * pruning + per-file bbox-stat pruning. The CASE WHEN tail survives Trino's
     * optimizer intact and short-circuits the WKB decode + ST_Intersects test
     * for any row whose bbox is fully inside the envelope.
     *
     * <p><b>Soundness:</b>
     * <ul>
     *   <li>bbox-overlap=FALSE ⇒ ST_Intersects=FALSE, AND short-circuits, row excluded ✓</li>
     *   <li>bbox-overlap=TRUE, bbox-contained=TRUE ⇒ CASE returns TRUE, row included
     *       (correct: bbox⊆env ⇒ geom intersects env, since bbox contains points of geom) ✓</li>
     *   <li>bbox-overlap=TRUE, bbox-contained=FALSE ⇒ CASE returns exact ST_Intersects ✓</li>
     * </ul>
     *
     * <p><b>Why CASE not OR:</b> Trino's optimizer distributes OR over AND
     * ({@code (A AND B AND C AND D) OR X} → {@code (A OR X) AND (B OR X) AND (C OR X) AND (D OR X)}),
     * causing ST_Intersects to evaluate up to 4× per row (3.3× slowdown measured).
     * CASE WHEN is opaque to that rewrite.
     */
    private void writeIntersectsWithBboxShortcut(Expression e1, Expression e2) {
        String col = ((PropertyName) e1).getPropertyName();
        String bboxCol = bboxColName(col);
        Geometry geom = (Geometry) ((Literal) e2).getValue();
        Envelope env = geom.getEnvelopeInternal();
        // bbox-overlap (necessary; pushable to file-level pruning) AND CASE WHEN
        // bbox-contained (sufficient) THEN TRUE ELSE exact ST_Intersects — the
        // row-level shortcut. CASE WHEN (not OR) survives Trino's optimizer intact.
        write("(" + bboxOverlapSql(bboxCol, env) + ") AND "
            + "CASE WHEN " + bboxContainedSql(bboxCol, env) + " THEN TRUE"
            + " ELSE ST_Intersects(ST_GeomFromBinary(" + quoteIdent(col) + "),"
            + " " + geomFromText(geom) + ") END");
    }

    private static String formatTimestamp(Date date) {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        return fmt.format(date);
    }

    private static double convertToMeters(double distance, String units) {
        if (units == null) return distance;  // GeoTools default: treat as meters.
        return switch (units.toLowerCase(Locale.ROOT)) {
            case "meters", "m", "meter"     -> distance;
            case "kilometers", "km"         -> distance * 1000.0;
            case "feet", "ft"               -> distance * 0.3048;
            case "miles", "mi"              -> distance * 1609.344;
            case "nautical miles", "nm"     -> distance * 1852.0;
            default -> {
                LOG.warning("Unrecognized DWITHIN distance unit '" + units + "'; treating distance as meters.");
                yield distance;
            }
        };
    }

    private void write(String sql) {
        try {
            out.write(sql);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write SQL", e);
        }
    }

    /**
     * Override encodeToString to initialize {@code out} before visiting and
     * bypass the default capabilities check in {@link FilterToSQL#encode}.
     *
     * @param filter filter to translate
     * @return the SQL WHERE-clause fragment
     */
    @Override
    public String encodeToString(org.geotools.api.filter.Filter filter) throws FilterToSQLException {
        StringWriter sw = new StringWriter();
        out = sw;
        filter.accept(this, null);
        return sw.toString();
    }
}
