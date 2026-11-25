"""Finding the accounts that hold an asset: discovery.

Two ways, one result — the account is added to the watch list like any
other. A pass reads the holders of the asset from Horizon, page by page
(:mod:`app.discovery.scan`). Between passes, a trustline opened on the asset
is seen in the history the worker stores (:mod:`app.discovery.events`).
"""
