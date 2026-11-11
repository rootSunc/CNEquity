"""Checks of the environment, the sources and past runs.

``cne doctor`` (``report``, ``packages``, ``preflight``), the source probes and
their health page, SLO and resilience reports (``source_*``, ``health_page``,
``substitutes``), run stability and per-step metrics. Import the module you
need; the package itself loads nothing, so the low-layer ``metrics`` stays
cheap to import.
"""
