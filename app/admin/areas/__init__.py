"""One module a group of pages of the admin UI. Each has ``build(door)`` that
returns its router, with paths relative to ``/admin``: the first page of the
group is ``""`` on the router's own prefix, the others start with a slash.
A page that changes something is a POST that begins with
``login, form = await door.form_of(request)``; a page that only shows reads
the database and asks no server.

One group is at ``/admin`` itself — the dashboard — and has no prefix to
add: it puts its page on ``door.router`` and returns a router without routes
(``app.admin.web`` mounts what a group returns, and nothing else).
"""
