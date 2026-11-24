"""Storage layer: the history database schema and the rules for writing to it.

Boundary: only this package talks to PostgreSQL. Sources (``app.sources``)
turn external records into plain row values; collector and backfill decide
*what* to write, this package decides *how* it lands without duplicates.
"""
