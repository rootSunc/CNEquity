"""The dashboard's operations routes.

The job runner, operations catalog and scheduler they call moved to
``cnequity.ops``: the CLI uses them too and should not depend on the web
package. The old module names here still import, and stay runnable with
``python -m``: an installed OS timer runs ``cnequity.serve.ops.scheduled``.
"""
