CREATE SEQUENCE "STORAGE_PARTITIONS_ID_SEQ" START WITH 5;
CREATE TABLE "storage_meta"(
    "root" CHARACTER VARYING(256) NOT NULL,
    "value" CHARACTER VARYING NOT NULL
);
ALTER TABLE "storage_meta" ADD CONSTRAINT "CONSTRAINT_C" PRIMARY KEY("root");
INSERT INTO "storage_meta" VALUES
('/tmp/geomesa7932145190517218983', U&'{\000a    "encoding" : "parquet",\000a    "feature-type" : {\000a        "attributes" : [\000a            {\000a                "name" : "name",\000a                "type" : "String"\000a            },\000a            {\000a                "default" : "true",\000a                "name" : "dtg",\000a                "type" : "Date"\000a            },\000a            {\000a                "default" : "true",\000a                "name" : "geom",\000a                "srid" : "4326",\000a                "type" : "Point"\000a            }\000a        ],\000a        "type-name" : "metadata",\000a        "user-data" : {\000a            "desc.dtg" : "\3072\3065\3051",\000a            "desc.geom" : "\c88c\d45c",\000a            "desc.name" : "\59d3\540d",\000a            "geomesa.index.dtg" : "dtg",\000a            "geomesa.user-data.prefix" : "desc"\000a        }\000a    },\000a    "leaf-storage" : true,\000a    "partition-scheme" : {\000a        "options" : {},\000a        "scheme" : "hourly,z2-2bits"\000a    }\000a}\000a');
CREATE TABLE "storage_partitions"(
    "root" CHARACTER VARYING(256) NOT NULL,
    "name" CHARACTER VARYING(256) NOT NULL,
    "id" INTEGER NOT NULL,
    "action" CHARACTER(1) NOT NULL,
    "features" BIGINT,
    "bounds_xmin" DOUBLE PRECISION,
    "bounds_xmax" DOUBLE PRECISION,
    "bounds_ymin" DOUBLE PRECISION,
    "bounds_ymax" DOUBLE PRECISION
);
ALTER TABLE "storage_partitions" ADD CONSTRAINT "CONSTRAINT_8" PRIMARY KEY("root", "name", "id");
INSERT INTO "storage_partitions" VALUES
('/tmp/geomesa7932145190517218983', '1', 1, 'a', 10, -10.0, 10.0, -5.0, 5.0),
('/tmp/geomesa7932145190517218983', '1', 2, 'a', 20, -11.0, 11.0, -5.0, 5.0),
('/tmp/geomesa7932145190517218983', '2', 3, 'a', 20, -1.0, 1.0, -5.0, 5.0),
('/tmp/geomesa7932145190517218983', '1', 4, 'd', 5, -11.0, 11.0, -5.0, 5.0);
CREATE TABLE "storage_partition_files"(
    "root" CHARACTER VARYING(256) NOT NULL,
    "name" CHARACTER VARYING(256) NOT NULL,
    "id" INTEGER NOT NULL,
    "file" CHARACTER VARYING(256) NOT NULL
);
ALTER TABLE "storage_partition_files" ADD CONSTRAINT "CONSTRAINT_88" PRIMARY KEY("root", "name", "id", "file");
INSERT INTO "storage_partition_files" VALUES
('/tmp/geomesa7932145190517218983', '1', 1, 'file1'),
('/tmp/geomesa7932145190517218983', '1', 2, 'file2'),
('/tmp/geomesa7932145190517218983', '1', 2, 'file3'),
('/tmp/geomesa7932145190517218983', '2', 3, 'file5'),
('/tmp/geomesa7932145190517218983', '2', 3, 'file6'),
('/tmp/geomesa7932145190517218983', '1', 4, 'file2');
