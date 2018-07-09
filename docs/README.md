# GeoMesa manual documentation

This is not the GeoMesa documentation itself, but rather notes on how to build it.

## Setup

The documentation is built using Python's [Sphinx](http://sphinx-doc.org/) module.

Installing Sphinx and its dependencies in a Python ``virtualenv``:

    $ virtualenv .sphinx && source .sphinx/bin/activate
    $ pip install -r requirements.txt
    
Alternatively use ``sudo`` with the ``pip`` command to install the packages in the system Python distribution.

    $ sudo pip install -r requirements.txt

Optional:  if you want to build the PDF version of the manual, install LaTeX:

    # on Ubuntu
    $ sudo apt-get install texlive-latex-base texlive-latex-recommended texlive-latex-extra texlive-fonts-recommended

The LaTeX distribution is pretty big, so you can skip it if you're just interested in the HTML docs.
You will also need ``make``.

## Building

To build HTML versions of the manuals:

    $ mvn clean install -Pdocs -pl docs

If you do not have Sphinx installed the manual will not be built.
The outputted files are written to the ``target/html`` directory. 

To build a PDF version:

    $ mvn clean install -Pdocs,latex
    $ cd target/latex
    $ make

There may be some errors during the LaTeX build; just press *Enter*
when it prompts for input. It should build the whole document. If the
table of contents doesn't render properly, delete ``GeoMesa.pdf``
and run ``make`` again.

To build a single HTML file containing all three manuals:

    $ mvn clean install -Pdocs,singlehtml

## About

There are currently three main manuals: the User Manual (``user``), the Developer Manual (``developer``),
and Tutorials (``tutorials``).

In each manual, the root page of the documentation is ``index.rst``. Any static files included go in
the ``_static`` directory. 

The files themselves are written in [reStructuredText](http://docutils.sourceforge.net/rst.html) (RST) and have ``*.rst``
extensions. Markdown files are also supported but do not support any of Docutils or Sphinx's special directives
(cross-references, admonitions, variable substitution, etc.).

For the most part, the syntax of RST is pretty similar to Markdown. Two particular sticking points are links and
code blocks. Inline links should be written like this:
```
`Link text <http://example.com/>`_
```
The final underscore is important!

Code blocks should always be indented with 4 spaces, and prefixed with the `.. code-block:: language` directive:
```
.. code-block:: scala

    val ds = new DataStore()
```

To include an image, copy it into ``_static/img`` and use the following:
```
.. image:: _static/img/name-of-file
```

You can also specify options for how to use the various directives. For
example, this adds line numbers to a code block:
```
.. code-block:: scala
    :linenos:

    val ds = new DataStore()
```

See the [Sphinx reStructuredText Primer](http://sphinx-doc.org/rest.html) for more information.
