"""The admin UI: the pages under ``/admin`` by which the owner runs the
installation from a browser, and the service functions they call.

The pages change nothing that the commands could not: every action is the
service function the command of the earlier stage uses — adding an account,
planning an import, registering a subscription — behind a door
(:mod:`app.admin.door`) and a form's token. What takes time is not done in
the request that asked for it: it is a task (:mod:`app.tasks`) with a state
the page shows.

    support.py   what the pages share: templates, filters, pagination
    door.py      the login and the logout; what pages and forms stand behind
    web.py       puts it all on the application
    areas/       one module a page group, each building its router
    lists.py     the changes to the watch lists the pages make, each in one transaction
"""
