Composite Converters
====================

Composite converters are designed to allow processing of mixed data streams. Each message that comes in can be
routed to a separate converter definition based on a predicate. Currently composite converters are available
for the :ref:`delimited_text_converter` and the :ref:`json_converter`.

.. _composite_predicates:

Predicates
----------

Predicates are an extension of the normal converter transform language. The inputs to the expression will
vary depending on the converter being used, but generally the inputs are available using the standard expressions
``$0``, ``$1``, etc. Operators are defined that allow comparisons between different transform expressions. Each
predicate must evaluate to a Boolean true or false, generally using a comparison operator. The normal transform
functions are available (see :ref:`converter_functions`).

The following operators are defined: ``==``, ``!=``, ``>``, ``>=``, ``<``, ``<=``, ``!``, ``&&``, ``||``

Operators can be grouped using parenthesis, for example ``$0 == 'a' && ($1 == 'b' || $2 == 'c')``

Note that the comparison operators (``>``, ``>=``, ``<``, ``<=``) can only operate on comparable types
(e.g. primitive ints, doubles, etc, or any class implementing ``java.lang.Comparable``).
