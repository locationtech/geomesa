import sys, os
sys.path.append(os.path.abspath('.'))
from common import *

# Suffix of source filenames
source_suffix = '.rst'

# Encoding of source files
source_encoding = 'utf-8'

# Master toctree document
master_doc = 'index'

# HTML title
html_title = 'GeoMesa %s Manuals' % release

exclude_patterns = [ 'README.md' ]

def setup(app):
    app.add_css_file('https://fonts.googleapis.com/css?family=Roboto:400,700')
