"""The smoke test of the integration with a real Operations Notifier — the
one check of stage 06 that is not run here, because it touches a live
service: it is prepared, and its owner runs it when they allow it.

It makes **one** subscription, for the test and by the test, on an account
its owner names; waits, asking no server, for a delivery of that
subscription to arrive through the webhook and for its transaction to be
brought in; and deletes that subscription again — only that one. Everything
else at the Notifier is read, never touched.
"""
