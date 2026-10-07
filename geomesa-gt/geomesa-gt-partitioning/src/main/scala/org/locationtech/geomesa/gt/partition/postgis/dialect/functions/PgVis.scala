/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package functions

import org.locationtech.geomesa.gt.partition.postgis.dialect.SqlStatements

/**
 * Provides per-row visibility filtering
 */
object PgVis extends PgVis with AdvisoryLock {
  override protected val lockId: Long = 1957550962396498252L
}

class PgVis extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    Seq(
      """-- Evaluate visibilities against authorizations.
        |--
        |-- Note that visibility strings are expected to be well formed - invalid strings will not raise errors but will
        |-- always evaluate to 'false'. Empty/null strings are considered invalid.
        |--
        |-- A valid expression is a sequence of tokens (chars from [A-Za-z0-9_-.:/], or any chars if quoted with ' or ")
        |-- joined by the binary operators & (and) or | (or). & and | may not be mixed at the same level without
        |-- parentheses to disambiguate, e.g. 'A|B&C' is invalid but '(A|B)&C' and 'A|(B&C)' are valid.
        |--
        |-- arguments:
        |--   vis   - visibility expression, consisting of tokens separated by & and |
        |--   auths - user authorization tokens
        |-- returns: the result of evaluating the visibilities against the authorizations, i.e. true if the user can see the piece of data
        |CREATE OR REPLACE FUNCTION pg_vis(vis varchar, auths varchar[]) RETURNS boolean AS $BODY$
        |  DECLARE
        |    i               int := 1;
        |    c_len           int;
        |    c               int;
        |    j               int;
        |    jc              int;
        |    escaped         boolean;
        |    expect_value    boolean := true;
        |    depth           int := 0;
        |    group_op        text[] := ARRAY['']::text[];
        |    group_value     boolean[] := ARRAY[false]::boolean[];
        |    group_has_value boolean[] := ARRAY[false]::boolean[];
        |    token_value     boolean;
        |  BEGIN
        |    IF vis IS NULL OR vis = '' THEN
        |      RETURN false;
        |    END IF;
        |
        |    c_len := char_length(vis);
        |    WHILE i <= c_len LOOP
        |      c := ascii(substring(vis, i, 1));
        |      IF (c >= 45 AND c <= 58) OR (c >= 65 AND c <= 90) OR c = 95 OR (c >= 97 AND c <= 122) THEN
        |        IF NOT expect_value THEN
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |        j := i + 1;
        |        WHILE j <= c_len LOOP
        |          c := ascii(substring(vis, j, 1));
        |          IF c <= 44 OR (c >= 59 AND c <= 64) OR (c >= 91 AND c <= 94) OR c = 96 OR c >= 123 THEN
        |            EXIT;
        |          END IF;
        |          j := j + 1;
        |        END LOOP;
        |        token_value := substring(vis, i, j - i) = ANY(auths);
        |        i := j - 1;
        |        expect_value := false;
        |        IF NOT group_has_value[depth + 1] THEN
        |          group_value[depth + 1] := token_value;
        |          group_has_value[depth + 1] := true;
        |        ELSIF group_op[depth + 1] = '&' THEN
        |          group_value[depth + 1] := group_value[depth + 1] AND token_value;
        |        ELSIF group_op[depth + 1] = '|' THEN
        |          group_value[depth + 1] := group_value[depth + 1] OR token_value;
        |        ELSE
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |      ELSIF c = 34 THEN
        |        IF NOT expect_value THEN
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |        j := i + 1;
        |        escaped := false;
        |        WHILE j <= c_len LOOP
        |          IF escaped THEN
        |            escaped := false;
        |          ELSE
        |            jc := ascii(substring(vis, j, 1));
        |            IF jc = c THEN
        |              EXIT;
        |            ELSIF jc = 92 THEN
        |              escaped := true;
        |            END IF;
        |          END IF;
        |          j := j + 1;
        |        END LOOP;
        |        token_value := regexp_replace(substring(vis, i + 1, (j - i) - 1), '[\\](.)', '\1', 'g') = ANY(auths);
        |        i := j;
        |        expect_value := false;
        |        IF NOT group_has_value[depth + 1] THEN
        |          group_value[depth + 1] := token_value;
        |          group_has_value[depth + 1] := true;
        |        ELSIF group_op[depth + 1] = '&' THEN
        |          group_value[depth + 1] := group_value[depth + 1] AND token_value;
        |        ELSIF group_op[depth + 1] = '|' THEN
        |          group_value[depth + 1] := group_value[depth + 1] OR token_value;
        |        ELSE
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |      ELSIF c = 38 THEN
        |        IF expect_value OR (group_op[depth + 1] <> '' AND group_op[depth + 1] <> '&') THEN
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |        group_op[depth + 1] := '&';
        |        expect_value := true;
        |      ELSIF c = 124 THEN
        |        IF expect_value OR (group_op[depth + 1] <> '' AND group_op[depth + 1] <> '|') THEN
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |        group_op[depth + 1] := '|';
        |        expect_value := true;
        |      ELSIF c = 40 THEN
        |        IF NOT expect_value THEN
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |        depth := depth + 1;
        |        group_op[depth + 1] := '';
        |        group_value[depth + 1] := false;
        |        group_has_value[depth + 1] := false;
        |      ELSIF c = 41 THEN
        |        IF expect_value OR depth = 0 THEN
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |        token_value := group_value[depth + 1];
        |        depth := depth - 1;
        |        expect_value := false;
        |        IF NOT group_has_value[depth + 1] THEN
        |          group_value[depth + 1] := token_value;
        |          group_has_value[depth + 1] := true;
        |        ELSIF group_op[depth + 1] = '&' THEN
        |          group_value[depth + 1] := group_value[depth + 1] AND token_value;
        |        ELSIF group_op[depth + 1] = '|' THEN
        |          group_value[depth + 1] := group_value[depth + 1] OR token_value;
        |        ELSE
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |      ELSE
        |        RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |        RETURN false;
        |      END IF;
        |      i := i + 1;
        |    END LOOP;
        |    IF expect_value OR depth <> 0 OR NOT group_has_value[1] THEN
        |      RAISE WARNING 'Invalid visibility expression: %', vis;
        |      RETURN false;
        |    END IF;
        |    RETURN group_value[1];
        |  EXCEPTION
        |    WHEN OTHERS THEN
        |      RAISE WARNING 'Invalid visibility expression: %', vis;
        |      RETURN false;
        |  END;
        |$BODY$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
        |""".stripMargin
    )
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] = Seq.empty // function is shared between types
}
