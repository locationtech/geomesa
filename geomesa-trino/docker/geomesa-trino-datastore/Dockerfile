# Carrier image for the GeoMesa Trino GeoTools DataStore.
#
# This image does NOT run anything. It exists only so a Kubernetes init
# container can copy the DataStore JARs onto the query-api worker / layer-manager
# classpath extension dir (/app/extensions), which the Jib launcher already
# globs (`java ... -cp .../app/extensions/* ...`). No query-api image rebuild.
#
# Contents (the ONLY runtime deps the query-api image doesn't already provide):
#   - geomesa-trino-datastore-*.jar  (the GeoTools DataStoreFactorySpi, ~25 KB)
#   - trino-jdbc-*.jar               (self-contained Trino JDBC driver, ~13 MB)
# GeoTools (34.2), JTS, and Jackson are already on the query-api classpath.
#
# Build context is the directory holding both JARs:
#   docker build -f docker/geomesa-trino-datastore/Dockerfile \
#     -t <registry>/ccri/geomesa-trino-datastore:<tag> \
#     dist/geomesa-trino-datastore
#
# Run `bash build.sh` first so the JARs exist under dist/geomesa-trino-datastore/.
FROM alpine:3.20

# Plain COPY: creates /datastore as 0755 (traversable) and preserves the JARs'
# source mode (0644, world-readable) in a single layer. Do NOT use
# `COPY --chmod=0644` — that also sets the /datastore DIRECTORY to 0644 (no
# execute bit), so a non-root init container can't traverse it.
COPY *.jar /datastore/
