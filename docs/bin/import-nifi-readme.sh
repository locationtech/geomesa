#!/bin/bash
#
# Import several README.md files from the GeoMesa nifi distribution,
# using ``pandoc`` to convert Markdown to RST.
#
# ----------------------------------------------------------------------
# WARNING: This is not completely automated. You will almost certainly
# have to hand-edit the files after this import!
# ----------------------------------------------------------------------

# Update for your system
GEOMESA_PATH=/opt/devel/geomesa
TUTORIAL_PATH=/opt/devel/geomesa-nifi

echo "## Nifi:"

# copy the README.md file
text_src=$TUTORIAL_PATH/README.md
text_dst=$GEOMESA_PATH/docs/user/nifi.rst
echo "Converting $text_src => $text_dst"
pandoc -i $text_src -o $text_dst

# fix links and bad RST generated by pandoc
echo "Fixing links and bad RST"
sed -i -e 's/.. code::/.. code-block::/' $text_dst
