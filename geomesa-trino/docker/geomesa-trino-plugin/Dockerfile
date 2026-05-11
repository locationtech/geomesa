# Carrier image for the spatial_iceberg Trino connector.
#
# This image does NOT run Trino. It exists only so a Kubernetes init container
# can copy the connector fat JAR onto the Trino pod's plugin path (an emptyDir
# mounted at /usr/lib/trino/plugin/iceberg-spatial). See the query-api Trino
# values (apps/query-api/values/trino.yaml.gotmpl) in the thresher-helmfiles repo.
#
# Build context is the directory holding the built JAR:
#   docker build -f docker/geomesa-trino-plugin/Dockerfile \
#     -t <registry>/ccri/geomesa-trino-plugin:<tag> \
#     dist/geomesa-trino-plugin
#
# Run `bash build.sh` first so the JAR exists under dist/geomesa-trino-plugin/.
FROM alpine:3.20

# The Trino plugin loader requires every JAR for a connector to live in its own
# directory under the plugin dir; the init container copies the contents of
# /plugin into that per-connector emptyDir. Plain COPY creates /plugin as 0755
# (traversable) and preserves the JAR's source mode (0644, world-readable) in a
# single layer. Do NOT use `COPY --chmod=0644` — it also sets the /plugin
# DIRECTORY to 0644 (no execute bit), so a non-root init container (runAsUser
# 1000, all caps dropped) can't traverse it to read the JAR.
COPY *.jar /plugin/
