#!/bin/bash

conda update -n base -c defaults conda -y
conda install conda-build ninja compilers -y

pwd
ls
git clone https://github.com/conda-forge/proj.4-feedstock.git

cd proj.4-feedstock
cat > recipe/recipe_clobber.yaml <<EOL
source:
  git_url: https://github.com/OSGeo/PROJ.git
  git_rev: ${GITHUB_SHA}
  url:
  sha256:
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
  patches:
=======
>>>>>>> locationtech-main
=======
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
=======
  patches:
>>>>>>> e4a6fd6d75 (typo fixes)
=======
  patches:
=======
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> locationtech-main

build:
  number: 2112
EOL

ls recipe
