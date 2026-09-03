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
      """-- Evaluate visibilities against authorizations. Note that visibility strings are expected to be well formed -
        |-- invalid strings will not raise errors but will always evaluate to 'false'.
        |-- a valid expression is a sequence of tokens (chars from [A-Za-z0-9_-.:/], or any chars if quoted with ' or ")
        |-- joined by the binary operators & (and) or | (or). & and | may not be mixed at the same level without
        |-- parentheses to disambiguate, e.g. 'A|B&C' is invalid but '(A|B)&C' and 'A|(B&C)' are valid.
        |-- arguments:
        |--   vis   - visibility expression, consisting of tokens separated by & and |
        |--   auths - user authorization tokens
        |-- returns: the result of evaluating the visibilities against the authorizations, i.e. true if the user can see the piece of data
        |CREATE OR REPLACE FUNCTION pg_vis(vis varchar, auths varchar[]) RETURNS boolean AS $BODY$
        |  DECLARE
        |    i              int := 1;        -- current position in the parser
        |    c_len          int;             -- length of the input
        |    c              int;             -- current char we're parsing
        |    j              int;             -- sub-position when parsing strings
        |    jc             int;             -- sub-char when parsing strings
        |    escaped        boolean;         -- track escape sequences in tokens
        |    expect_value   boolean := true; -- true if the next token must be an operand or '(', false if it must be an operator or ')'
        |    depth          int := 0;        -- current parenthesis nesting depth
        |    group_op       text[] := ARRAY['']::text[]; -- operator ('&' or '|') established for the group at each depth; '' if none yet
        |    operator_stack text[] := ARRAY[]::text[];       -- operator stack, from shunting yard algo
        |    output_stack   text[] := ARRAY[]::text[];       -- output stack, from shunting yard algo
        |    result_stack   boolean[] := ARRAY[]::boolean[]; -- stack for evaluating RPN
        |  BEGIN
        |    IF vis IS NULL OR vis = '' THEN
        |      return true;
        |    END IF;
        |
        |    c_len := char_length(vis);
        |    -- based on https://en.wikipedia.org/wiki/Shunting_yard_algorithm
        |    WHILE i <= c_len LOOP
        |      -- get the next char
        |      c := ascii(substring(vis, i, 1));
        |      -- match unquoted string
        |      -- these numbers correspond to: a-z [97-122], A-Z [65-90], 0-9 [48-57], _ [95], : [58], - [45], . [46], / [47]
        |      IF (c >= 45 AND c <= 58) OR (c >= 65 AND c <= 90) OR c = 95 OR (c >= 97 AND c <= 122) THEN
        |        -- an operand may only appear where a value is expected
        |        IF NOT expect_value THEN
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |        j := i + 1;
        |        WHILE j <= c_len LOOP
        |          c := ascii(substring(vis, j, 1));
        |          IF c <= 44 OR (c >= 59 AND c <= 64) OR (c >= 91 AND c <= 94) OR c = 96 OR c >= 123 THEN
        |            EXIT; -- exit loop
        |          END IF;
        |          j := j + 1;
        |        END LOOP;
        |        -- push the value onto the output stack - note, we evaluate the token against the auths before pushing it to the stack
        |        output_stack := (substring(vis, i, j - i) = ANY(auths))::text || output_stack;
        |        i := j - 1;
        |        expect_value := false;
        |      -- match quoted string
        |      -- " [34], ' [39]
        |      ELSIF c = 34 OR c = 39 THEN
        |        -- an operand may only appear where a value is expected
        |        IF NOT expect_value THEN
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |        j := i + 1;
        |        escaped := false;
        |        WHILE j <= c_len LOOP
        |          IF escaped = true THEN
        |            escaped = false;
        |          ELSE
        |            jc := ascii(substring(vis, j, 1));
        |            IF jc = c THEN
        |              EXIT; -- exit loop
        |             -- \ [92]
        |            ELSIF jc = 92 THEN
        |              escaped := true;
        |            END IF;
        |          END IF;
        |          j := j + 1;
        |        END LOOP;
        |        -- push the value onto the output stack - note, we evaluate the token against the auths before pushing it to the stack
        |        -- remove escape backslashes with a regex
        |        output_stack := (regexp_replace(substring(vis, i + 1, (j - i) - 1), '[\\](.)', '\1', 'g') = ANY(auths))::text || output_stack;
        |        i := j;
        |        expect_value := false;
        |      -- match boolean AND
        |      -- & [38]
        |      ELSIF c = 38 THEN
        |        -- an operator may only appear after an operand or ')', and & and | may not be mixed within the same group
        |        IF expect_value OR (group_op[depth + 1] <> '' AND group_op[depth + 1] <> '&') THEN
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |        group_op[depth + 1] := '&';
        |        -- pop any ANDs off the operator stack and onto the output stack
        |        WHILE operator_stack[1] = '&' LOOP
        |          output_stack := '&'::text || output_stack;
        |          operator_stack := operator_stack[2:];
        |        END LOOP;
        |        -- push the AND onto the operator stack
        |        operator_stack := '&'::text || operator_stack;
        |        expect_value := true;
        |      -- match boolean OR
        |      -- | [124]
        |      ELSIF c = 124 THEN
        |        -- an operator may only appear after an operand or ')', and & and | may not be mixed within the same group
        |        IF expect_value OR (group_op[depth + 1] <> '' AND group_op[depth + 1] <> '|') THEN
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |        group_op[depth + 1] := '|';
        |        -- pop any ORs off the operator stack and onto the output stack
        |        WHILE operator_stack[1] = '|' LOOP
        |          output_stack := '|'::text || output_stack;
        |          operator_stack := operator_stack[2:];
        |        END LOOP;
        |        -- push the OR onto the operator stack
        |        operator_stack := '|'::text || operator_stack;
        |        expect_value := true;
        |      -- match open parenthesis
        |      -- ( [40]
        |      ELSIF c = 40 THEN
        |        -- a '(' may only appear where a value is expected
        |        IF NOT expect_value THEN
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |        -- push it onto the operator stack
        |        operator_stack :=  '('::text || operator_stack;
        |        depth := depth + 1;
        |        group_op[depth + 1] := ''; -- reset the operator tracking for the new group
        |      -- match close parenthesis
        |      -- ) [41]
        |      ELSIF c = 41 THEN
        |        -- a ')' may only appear after an operand or ')', and only if there is a matching '('
        |        IF expect_value OR depth = 0 THEN
        |          RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |          RETURN false;
        |        END IF;
        |        -- pop everything inside the parentheses off the operator stack and onto the output stack
        |        WHILE operator_stack[1] <> '(' LOOP
        |          output_stack := operator_stack[1] || output_stack;
        |          operator_stack := operator_stack[2:];
        |        END LOOP;
        |        -- pop and discard the opening parentheses
        |        operator_stack := operator_stack[2:];
        |        depth := depth - 1;
        |        -- a completed group acts as an operand
        |        expect_value := false;
        |      ELSE
        |        RAISE WARNING 'Invalid visibility expression at index %: %', i, vis;
        |        RETURN false;
        |      END IF;
        |      i := i + 1;
        |    END LOOP;
        |
        |    -- a valid expression cannot end expecting a value (trailing operator or unclosed '(') or with unbalanced parens
        |    IF expect_value OR depth <> 0 THEN
        |      RAISE WARNING 'Invalid visibility expression: %', vis;
        |      RETURN false;
        |    END IF;
        |
        |    -- pop any remaining operators into the output stack
        |    i := 1;
        |    j := array_length(operator_stack, 1);
        |    WHILE i <= j LOOP
        |      output_stack := operator_stack[i] || output_stack;
        |      i := i + 1;
        |    END LOOP;
        |
        |    -- now evaluate the output stack, starting at the bottom
        |    i := array_length(output_stack, 1);
        |    WHILE i > 0 LOOP
        |      IF output_stack[i] = '&' THEN
        |        -- pop 2 values off the result stack and put the resulting AND back on the stack
        |        result_stack := (result_stack[1] AND result_stack[2]) || result_stack[3:];
        |      ELSIF output_stack[i] = '|' THEN
        |        -- pop 2 values off the result stack and put the resulting OR back on the stack
        |        result_stack := (result_stack[1] OR result_stack[2]) || result_stack[3:];
        |      ELSE
        |        result_stack := output_stack[i]::boolean || result_stack;
        |      END IF;
        |      i := i - 1;
        |    END LOOP;
        |
        |    IF array_length(result_stack, 1) < 1 THEN
        |      RAISE WARNING 'Invalid visibility expression: %', vis;
        |      RETURN false;
        |    END IF;
        |
        |    RETURN result_stack[1];
        |  EXCEPTION
        |    -- any error indicates an invalid visibility expression - evaluate to 'false' rather than raising
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
