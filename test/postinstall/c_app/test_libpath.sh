#!/bin/sh
. ../common.sh

EXPECTED_LIBPATH="$(pkg-config proj --variable=libdir)"

test_libpath c_app "${EXPECTED_LIBPATH}" libproj
