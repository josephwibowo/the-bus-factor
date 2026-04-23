"""Shared pure-Python helpers used by Bruin Python assets and tests.

Nothing under ``pipeline/lib`` may perform I/O beyond reading
``pipeline/config/scoring.yml`` and the pipeline-managed DuckDB file
passed in explicitly. The module is deliberately side-effect free so that
the scoring arithmetic can be unit-tested without a DuckDB warehouse.
"""
