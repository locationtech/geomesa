-- Version of the database structure.
-- The major number indicates an incompatible change (e.g. table or column
-- removed or renamed).
-- The minor number is incremented if a backward compatible change done, that
-- is the new database can still work with an older PROJ version.
-- When updating those numbers, the DATABASE_LAYOUT_VERSION_MAJOR and
-- DATABASE_LAYOUT_VERSION_MINOR constants in src/iso19111/factory.cpp must be
-- updated as well.
INSERT INTO "metadata" VALUES('DATABASE.LAYOUT.VERSION.MAJOR', 1);
INSERT INTO "metadata" VALUES('DATABASE.LAYOUT.VERSION.MINOR', 2);

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 74eac2217b (typo fixes)
=======
=======
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
INSERT INTO "metadata" VALUES('EPSG.VERSION', 'v10.081');
INSERT INTO "metadata" VALUES('EPSG.DATE', '2023-01-19');
=======
INSERT INTO "metadata" VALUES('EPSG.VERSION', 'v10.078');
INSERT INTO "metadata" VALUES('EPSG.DATE', '2022-12-13');
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locationtech-main
=======
INSERT INTO "metadata" VALUES('EPSG.VERSION', 'v10.078');
INSERT INTO "metadata" VALUES('EPSG.DATE', '2022-12-13');
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74eac2217b (typo fixes)
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
INSERT INTO "metadata" VALUES('EPSG.VERSION', 'v10.081');
INSERT INTO "metadata" VALUES('EPSG.DATE', '2023-01-19');
>>>>>>> e4a6fd6d75 (typo fixes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa21c6fa76 (typo fixes)
=======
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
INSERT INTO "metadata" VALUES('EPSG.VERSION', 'v10.078');
INSERT INTO "metadata" VALUES('EPSG.DATE', '2022-12-13');
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
INSERT INTO "metadata" VALUES('EPSG.VERSION', 'v10.078');
INSERT INTO "metadata" VALUES('EPSG.DATE', '2022-12-13');
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
INSERT INTO "metadata" VALUES('EPSG.VERSION', 'v10.081');
INSERT INTO "metadata" VALUES('EPSG.DATE', '2023-01-19');
>>>>>>> 86ade66356 (typo fixes)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> aa21c6fa76 (typo fixes)
>>>>>>> 74eac2217b (typo fixes)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)

-- The value of ${PROJ_VERSION} is substituted at build time by the actual
-- value.
INSERT INTO "metadata" VALUES('PROJ.VERSION', '${PROJ_VERSION}');

-- Version of the PROJ-data package with which this database is the most
-- compatible.
INSERT INTO "metadata" VALUES('PROJ_DATA.VERSION', '1.13');
