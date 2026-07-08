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
import org.geotools.api.filter.spatial.Beyond;
import org.geotools.api.filter.spatial.BinarySpatialOperator;
import org.geotools.api.filter.spatial.Contains;
import org.geotools.api.filter.spatial.Crosses;
import org.geotools.api.filter.spatial.Disjoint;
import org.geotools.api.filter.spatial.DWithin;
import org.geotools.api.filter.spatial.Equals;
import org.geotools.api.filter.spatial.Intersects;
import org.geotools.api.filter.spatial.Overlaps;
import org.geotools.api.filter.spatial.Touches;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger LOG = LoggerFactory.getLogger(TrinoFilterToSQL.class);

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
    //
    // All binary spatial operators funnel through the base class's dispatcher: the
    // public visit(*) methods delegate to visitBinarySpatialOperator, which splits
    // the operands into the property/literal pair before calling the hook below. BBOX
    // keeps a dedicated visit() override because its literal is an envelope, not a
    // Geometry.

    /** Single translation point for every binary spatial operator. {@code swapped}
     *  means the geometry literal was expression1 (e.g. {@code INTERSECTS(POLYGON,
     *  geom)}) — irrelevant for symmetric operators, semantics-reversing for
     *  Within/Contains (which are each other's complement: {@code contains(a, b) ⇔
     *  within(b, a)}). Disjoint and Beyond get NO bbox prefilter */
    @Override
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter,
            PropertyName property, Literal literal, boolean swapped, Object extraData) {
        String col = property.getPropertyName();
        if (!(literal.getValue() instanceof Geometry geom)) {
            throw new IllegalArgumentException(
                "Unsupported spatial filter literal (expected a geometry): " + filter);
        }
        if (filter instanceof Intersects) {                  // symmetric
            writeIntersectsFor(col, geom);
        } else if (filter instanceof Within) {
            if (swapped) {
                writeLiteralWithinColumn(col, geom);
            } else {
                writeWithin(col, geom);
            }
        } else if (filter instanceof Contains) {
            if (swapped) {
                writeWithin(col, geom);                      // CONTAINS(lit, geom) ⇔ WITHIN(geom, lit)
            } else {
                writeColumnContainsLiteral(col, geom);
            }
        } else if (filter instanceof DWithin d) {            // symmetric
            writeDWithin(col, geom, convertToMeters(d.getDistance(), d.getDistanceUnits()));
        } else if (filter instanceof Beyond b) {             // symmetric
            writeBeyond(col, geom, convertToMeters(b.getDistance(), b.getDistanceUnits()));
        } else if (filter instanceof Crosses) {              // symmetric
            writeIntersectionImplyingOp("ST_Crosses", col, geom);
        } else if (filter instanceof Touches) {              // symmetric
            writeIntersectionImplyingOp("ST_Touches", col, geom);
        } else if (filter instanceof Overlaps) {             // symmetric
            writeIntersectionImplyingOp("ST_Overlaps", col, geom);
        } else if (filter instanceof Equals) {               // symmetric
            writeIntersectionImplyingOp("ST_Equals", col, geom);
        } else if (filter instanceof Disjoint) {             // symmetric
            write("ST_Disjoint(ST_GeomFromBinary(" + quoteIdent(col) + "),"
                + " " + geomFromText(geom) + ")");
        } else {
            throw new IllegalArgumentException("Unsupported spatial operator: " + filter);
        }
        return extraData;
    }

    /** Other operand shapes the base dispatcher couldn't split into a property/literal pair
     *  are unsupported. */
    @Override
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter,
            Expression e1, Expression e2, Object extraData) {
        throw new IllegalArgumentException(
            "Unsupported spatial filter operands (expected one geometry literal and one property name): " + filter);
    }

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

    /** Intersects translation.
     *  Fast path: ST_Intersects(point, rect) ⇔ bbox-overlap(bbox(point), rect)
     *  because bbox(Point) = Point. Soundness needs BOTH conditions:
     *  a rectangle query (equivalence only holds for axis-aligned rectangles) AND
     *  point data (for any non-point geometry, bbox(g) is a strict superset of g,
     *  so bbox-overlap is necessary but not sufficient). Otherwise: bbox-overlap
     *  (pushable to file-level pruning) AND either the CASE WHEN bbox-contained
     *  shortcut (rectangular queries only) or the exact row-level ST_Intersects. */
    private void writeIntersectsFor(String col, Geometry geom) {
        if (geom instanceof Polygon p && p.isRectangle() && storedGeometryIsPoint(col)) {
            writeBboxOverlap(col, p.getEnvelopeInternal());
            return;
        }
        writeIntersects(col, geom);
    }

    /** WITHIN(geom, literal): for axis-aligned rectangular query polygons,
     *  bbox⊆rect is equivalent to ST_Within(geom, rect) — no row-level ST_Within
     *  needed at all. For anything else, bbox-overlap (Z2 pushdown) AND exact
     *  ST_Within at row level. Also serves swapped Contains
     *  ({@code CONTAINS(lit, geom) ⇔ WITHIN(geom, lit)}). */
    private void writeWithin(String col, Geometry geom) {
        if (geom instanceof Polygon p && p.isRectangle()) {
            writeWithinRectangleAsBboxContained(col, p);
        } else {
            writeWithinNonRectangle(col, geom);
        }
    }

    /** DWithin translation: outer/inner bbox bounds plus an exact spherical distance check. */
    private void writeDWithin(String col, Geometry refGeom, double distanceMeters) {

        // Treat reference as a point (centroid for non-point geometries — same as before).
        double lon, lat;
        if (refGeom instanceof Point pt) {
            lon = pt.getX();
            lat = pt.getY();
        } else {
            lon = refGeom.getEnvelope().getCentroid().getX();
            lat = refGeom.getEnvelope().getCentroid().getY();
        }

        // OUTER bbox: bounding box of "every point within d of ref" — necessary-overlap
        // prefilter. The safety margin absorbs flat-vs-spherical projection error so we
        // don't accidentally exclude rows whose true distance is just under d.
        // Longitude scaling: degrees of longitude shrink toward the poles, so cos() is
        // evaluated at the POLEWARD EDGE of the latitude band (not its center) — the
        // narrowest point, where the required longitude span is widest; sizing by the
        // center latitude under-covers for large radii at high latitudes and drops rows
        // (e.g. lat 60°, d=1000 km: cos(60°)/cos(69.9°) ≈ 1.46, past the 1.1 margin).
        // The near-pole gate is likewise evaluated at the band edge. Near a pole (or when
        // the span would exceed 180°) the within-d region wraps all longitudes, so we use
        // a full-longitude band rather than a bounded (row-dropping) span.
        double outerDegLat = (distanceMeters / METERS_PER_DEGREE) * OUTER_SAFETY_MARGIN;
        double polewardLat = Math.min(Math.abs(lat) + outerDegLat, 90.0);
        boolean nearPole = polewardLat >= NEAR_POLE_LAT;
        double outerDegLon = nearPole ? Double.POSITIVE_INFINITY
            : (distanceMeters / (METERS_PER_DEGREE * Math.cos(Math.toRadians(polewardLat)))) * OUTER_SAFETY_MARGIN;
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
            // scaled to lat/lon. Corners land at distance ≤ INNER_SAFETY_MARGIN × d from ref,
            // so any bbox(geom) ⊆ this rectangle ⇒ every point of geom is within d ⇒ TRUE.
            // Longitude scaling mirrors the outer box in the conservative direction: cos()
            // is evaluated at the EQUATORWARD EDGE of the band (its physically widest
            // point), so the rectangle's real half-width never exceeds the target anywhere
            // in the band — sizing by the center latitude lets the equatorward corners land
            // beyond d (e.g. lat 70°, d=1000 km: corner ≈ 1.03 d), wrongly including rows.
            double innerDegLat = (distanceMeters / METERS_PER_DEGREE) * INSCRIBED_FACTOR * INNER_SAFETY_MARGIN;
            double equatorwardLat = Math.max(Math.abs(lat) - innerDegLat, 0.0);
            double innerDegLon = (distanceMeters / (METERS_PER_DEGREE * Math.cos(Math.toRadians(equatorwardLat))))
                * INSCRIBED_FACTOR * INNER_SAFETY_MARGIN;
            Envelope inner = new Envelope(lon - innerDegLon, lon + innerDegLon,
                                          lat - innerDegLat, lat + innerDegLat);
            // Outer bbox-overlap (file/Z2 pruning) AND CASE WHEN inner-rectangle-contained
            // (sufficient for distance ≤ d) THEN TRUE ELSE exact spherical distance check.
            write("(" + bboxOverlapSql(bboxCol, outer) + ") AND "
                + "CASE WHEN " + bboxContainedSql(bboxCol, inner) + " THEN TRUE"
                + " ELSE " + distanceCheck + " END");
        }
    }

    // ── Other binary spatial operators ────────────────────────────────────────
    //
    // Crosses/Touches/Overlaps/Equals all imply a non-empty intersection, so each
    // gets the pushable bbox-overlap prefilter (Z2/file pruning) plus the exact
    // row-level ST_* test. No CASE WHEN shortcuts: envelope containment proves
    // nothing for these predicates (see the Intersects rectangle gate).

    /** {@code CONTAINS(geom, literal)} — the row geometry contains the literal, so
     *  the pushable prefilter is bbox-COVERS (the row bbox must contain the literal's
     *  envelope) plus exact {@code ST_Contains}. The literal-first form is handled in
     *  the dispatcher as {@code WITHIN(geom, literal)}. */
    private void writeColumnContainsLiteral(String col, Geometry geom) {
        String q = quoteIdent(bboxColName(col));
        Envelope env = geom.getEnvelopeInternal();
        String bboxCovers = String.format(
            "%s.xmin <= %s AND %s.xmax >= %s AND %s.ymin <= %s AND %s.ymax >= %s",
            q, env.getMinX(), q, env.getMaxX(), q, env.getMinY(), q, env.getMaxY());
        write("(" + bboxCovers + ")"
            + " AND ST_Contains(ST_GeomFromBinary(" + quoteIdent(col) + "),"
            + " " + geomFromText(geom) + ")");
    }

    /** Beyond — DWithin's complement: exact spherical distance check, no prefilter. */
    private void writeBeyond(String col, Geometry refGeom, double distanceMeters) {
        String refWkt = wkt.write(refGeom).replace("'", "''");
        write(String.format(Locale.ROOT,
            "ST_Distance(to_spherical_geography(ST_GeomFromBinary(%s)),"
            + " to_spherical_geography(ST_GeometryFromText('%s'))) > %.0f",
            quoteIdent(col), refWkt, distanceMeters));
    }

    /** Shared shape for the intersection-implying operators (Crosses, Touches,
     *  Overlaps, Equals): the pushable bbox-overlap prefilter (necessary — each of
     *  these predicates implies a non-empty intersection, which implies overlapping
     *  envelopes) AND the exact row-level test. */
    private void writeIntersectionImplyingOp(String function, String col, Geometry geom) {
        write("(" + bboxOverlapSql(bboxColName(col), geom.getEnvelopeInternal()) + ")"
            + " AND " + function + "(ST_GeomFromBinary(" + quoteIdent(col) + "),"
            + " " + geomFromText(geom) + ")");
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
     * True iff the named geometry column is declared as exactly {@link Point}.
     * Used by {@link #visit(Intersects, Object)} to detect when the bbox-overlap +
     * CASE WHEN rewrite collapses to bbox-overlap alone. The binding comes from
     * schema discovery, which declares {@code Point} for columns carrying a
     * {@code __<col>_z2__} companion.
     * <p>
     * Strict {@code equals} (not {@code isAssignableFrom}) is required:
     * {@code bbox(MultiPoint)} is the union extent, so the optimization is
     * unsound for MultiPoint even though MultiPoint is "point-like". JTS does
     * not make MultiPoint a Point subclass, so this is also defensive.
     * <p>
     * Returns {@code false} when no FeatureType is set — callers must keep the
     * safe general-case rewrite in that branch.
     */
    private boolean storedGeometryIsPoint(String col) {
        if (featureType == null) return false;
        var descriptor = featureType.getDescriptor(col);
        return descriptor != null && Point.class.equals(descriptor.getType().getBinding());
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
    private void writeWithinRectangleAsBboxContained(String col, Polygon rect) {
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
    private void writeWithinNonRectangle(String col, Geometry queryGeom) {
        String bboxCol = bboxColName(col);
        Envelope env = queryGeom.getEnvelopeInternal();
        write("(" + bboxOverlapSql(bboxCol, env) + ")"
            + " AND ST_Within(ST_GeomFromBinary(" + quoteIdent(col) + "),"
            + " " + geomFromText(queryGeom) + ")");
    }

    /**
     * {@code WITHIN(literal, geom)} — the literal contained in the row geometry. The
     * necessary pushable prefilter is bbox-COVERS: the row bbox must contain the
     * literal's envelope (literal ⊆ geom ⇒ env(literal) ⊆ env(geom) = bbox). The exact
     * test is {@code ST_Within(literal, geom)} with the operands in the original order.
     */
    private void writeLiteralWithinColumn(String col, Geometry literalGeom) {
        String q = quoteIdent(bboxColName(col));
        Envelope env = literalGeom.getEnvelopeInternal();
        String bboxCovers = String.format(
            "%s.xmin <= %s AND %s.xmax >= %s AND %s.ymin <= %s AND %s.ymax >= %s",
            q, env.getMinX(), q, env.getMaxX(), q, env.getMinY(), q, env.getMaxY());
        write("(" + bboxCovers + ")"
            + " AND ST_Within(" + geomFromText(literalGeom) + ","
            + " ST_GeomFromBinary(" + quoteIdent(col) + "))");
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
     * <p><b>The CASE WHEN shortcut applies ONLY to axis-aligned rectangular
     * query polygons</b> (where the polygon equals its envelope). For any other query
     * geometry, bbox-containment in the ENVELOPE does not imply intersection with the
     * GEOMETRY (examples, a polygon with a hole, a crescent, etc. contain
     * envelope regions outside the polygon), so the exact {@code ST_Intersects} runs for
     * every bbox-overlapping row instead. For rectangles:
     * <ul>
     *   <li>bbox-overlap=FALSE ⇒ ST_Intersects=FALSE, AND short-circuits, row excluded ✓</li>
     *   <li>bbox-overlap=TRUE, bbox-contained=TRUE ⇒ CASE returns TRUE, row included
     *       (bbox ⊆ rect ⇒ geom ⊆ rect ⇒ intersects) ✓</li>
     *   <li>bbox-overlap=TRUE, bbox-contained=FALSE ⇒ CASE returns exact ST_Intersects ✓</li>
     * </ul>
     *
     * <p><b>Why CASE not OR:</b> Trino's optimizer distributes OR over AND
     * ({@code (A AND B AND C AND D) OR X} → {@code (A OR X) AND (B OR X) AND (C OR X) AND (D OR X)}),
     * causing ST_Intersects to evaluate up to 4× per row (3.3× slowdown measured).
     * CASE WHEN is opaque to that rewrite.
     */
    private void writeIntersects(String col, Geometry geom) {
        String bboxCol = bboxColName(col);
        Envelope env = geom.getEnvelopeInternal();
        String exact = "ST_Intersects(ST_GeomFromBinary(" + quoteIdent(col) + "),"
            + " " + geomFromText(geom) + ")";
        if (geom instanceof Polygon p && p.isRectangle()) {
            // Rectangle: bbox-overlap (necessary; pushable to file-level pruning) AND
            // CASE WHEN bbox-contained (sufficient — the polygon IS its envelope) THEN TRUE
            // ELSE exact ST_Intersects. CASE WHEN (not OR) survives Trino's optimizer intact.
            write("(" + bboxOverlapSql(bboxCol, env) + ") AND "
                + "CASE WHEN " + bboxContainedSql(bboxCol, env) + " THEN TRUE"
                + " ELSE " + exact + " END");
        } else {
            // Non-rectangular query: envelope containment alone doesn't imply intersection
            // with the geometry itself, so no row-level shortcut — just the
            // pushable bbox-overlap prefilter and the exact test.
            write("(" + bboxOverlapSql(bboxCol, env) + ") AND " + exact);
        }
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
                LOG.warn("Unrecognized DWITHIN distance unit '" + units + "'; treating distance as meters.");
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
