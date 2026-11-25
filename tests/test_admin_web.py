"""The admin UI as a browser meets it: closed without a secret, a login that
counts its failures, a session in a cookie, a token in every form, pages
that escape what they show and tell no secret, and a task page that changes
nothing by being looked at."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.admin import auth
from app.api.app import create_app
from app.config import PUBLIC_NETWORK_PASSPHRASE, Settings
from app.storage import service_jobs
from app.storage.models import ServiceJob
from tests.fake_horizon import serve

SECRET = "a-secret-of-the-admin"
EVIL = "<script>alert('x')</script>"


def settings_for(url: str, **changes: Any) -> Settings:
    return Settings(
        **{  # type: ignore[arg-type]
            "database_url": url,
            "network_passphrase": PUBLIC_NETWORK_PASSPHRASE,
            "admin_secret": SECRET,
            **changes,
        }
    )


def login(client: TestClient, secret: str = SECRET, **headers: str) -> Any:
    return client.post("/admin/login", data={"secret": secret}, follow_redirects=False, headers=headers)


def csrf_of(page: Any) -> str:
    assert page.status_code == 200, page.text
    import re

    found = re.search(r'name="csrf" value="([0-9a-f]{64})"', page.text)
    assert found, "the page carries the form's token"
    return found.group(1)


def a_task(engine: Any, error: str | None = None) -> int:
    job, _ = service_jobs.enqueue(engine, "pool_check", {"repeats": 1}, one_at_a_time="pool")
    if error:
        with Session(engine) as session, session.begin():
            session.get(ServiceJob, job.id).last_error = error  # type: ignore[union-attr]
    return job.id


# -- closed without a secret ------------------------------------------------------------


def test_without_a_secret_the_admin_ui_is_closed_for_everyone(make_database: Callable[[], str]) -> None:
    url = make_database()
    with TestClient(create_app(Settings(database_url=url))) as client:
        page = client.get("/admin/login")
        assert page.status_code == 503 and "MTL_HORIZON_ADMIN_SECRET" in page.text
        assert client.get("/admin/tasks", follow_redirects=False).status_code == 303
        assert client.get("/admin/tasks", follow_redirects=False).headers["location"] == "/admin/login"
        tried = client.post("/admin/login", data={"secret": ""}, follow_redirects=False)
        assert tried.status_code == 503 and "set-cookie" not in tried.headers
        # The door is not even compared against a secret that is not there.
        forged = client.post("/admin/login", data={"secret": ""}, follow_redirects=False)
        assert forged.status_code == 503
    with TestClient(create_app(Settings(database_url=""))) as client:
        assert client.get("/admin/login").status_code == 503  # no database: closed all the same
        assert client.get("/admin/tasks", follow_redirects=False).status_code == 303


def test_the_static_files_are_served_and_set_no_cookie(make_database: Callable[[], str]) -> None:
    with TestClient(create_app(Settings(database_url=make_database()))) as client:
        for name in ("admin.css", "admin.js"):
            answer = client.get(f"/admin/static/{name}")
            assert answer.status_code == 200 and "set-cookie" not in answer.headers
        assert client.get("/admin/static/../auth.py").status_code in (403, 404)
        assert client.get("/admin/static/nothing.css").status_code == 404


# -- the login ---------------------------------------------------------------------------


def test_the_login_opens_a_session_in_a_cookie_that_only_the_admin_path_sees(make_database: Callable[[], str]) -> None:
    url = make_database()
    engine = create_engine(url)
    try:
        a_task(engine, EVIL)
        with TestClient(create_app(settings_for(url))) as client:
            wrong = login(client, "not-it")
            assert wrong.status_code == 401 and "set-cookie" not in wrong.headers and SECRET not in wrong.text
            page = client.get("/admin/tasks", follow_redirects=False)
            assert page.status_code == 303 and page.headers["location"] == "/admin/login"
            answer = login(client)
            assert answer.status_code == 303 and answer.headers["location"] == "/admin"
            cookie = answer.headers["set-cookie"].lower()
            assert auth.COOKIE + "=" in cookie and "httponly" in cookie and "samesite=strict" in cookie and "path=/admin" in cookie
            assert "secure" not in cookie, "plain HTTP on localhost: no Secure, or the cookie would not come back"
            assert SECRET not in cookie
            # The cookie is sent for /admin only.
            assert client.get("/health").request.headers.get("cookie") is None
            tasks = client.get("/admin/tasks")
            assert tasks.status_code == 200 and "mtlh_admin" in client.cookies
            assert "&lt;script&gt;" in tasks.text and "<script>alert" not in tasks.text, "what a page shows is escaped"
            assert SECRET not in tasks.text and url not in tasks.text and "not-a-password" not in tasks.text
            detail = client.get(f"/admin/tasks/{a_task(engine)}")
            assert detail.status_code == 200 and "pool_check" in detail.text
    finally:
        engine.dispose()


def test_behind_a_public_origin_the_cookie_is_secure_and_changes_come_from_there(make_database: Callable[[], str]) -> None:
    url = make_database()
    engine = create_engine(url)
    try:
        job_id = a_task(engine)
        origin = {"origin": "https://horizon.example.org"}
        # The installation is reached over HTTPS there, and a Secure cookie travels over nothing else.
        with TestClient(
            create_app(settings_for(url, public_origin="https://horizon.example.org")), base_url="https://horizon.example.org"
        ) as client:
            assert login(client, **{"origin": "https://evil.example.org"}).status_code == 403
            answer = login(client, **origin)
            assert answer.status_code == 303 and "secure" in answer.headers["set-cookie"].lower()
            token = csrf_of(client.get(f"/admin/tasks/{job_id}"))
            foreign = client.post(f"/admin/tasks/{job_id}/cancel", data={"csrf": token}, headers={"origin": "https://evil.example.org"}, follow_redirects=False)
            assert foreign.status_code == 403
            own = client.post(f"/admin/tasks/{job_id}/cancel", data={"csrf": token}, headers=origin, follow_redirects=False)
            assert own.status_code == 303
    finally:
        engine.dispose()


def test_a_form_without_its_token_changes_nothing(make_database: Callable[[], str]) -> None:
    url = make_database()
    engine = create_engine(url)
    try:
        job_id = a_task(engine)
        with TestClient(create_app(settings_for(url))) as client:
            login(client)
            stranger = client.post(f"/admin/tasks/{job_id}/cancel", data={}, follow_redirects=False)
            assert stranger.status_code == 403 and "токен" in stranger.text
            forged = client.post(f"/admin/tasks/{job_id}/cancel", data={"csrf": "0" * 64}, follow_redirects=False)
            assert forged.status_code == 403
            job = service_jobs.get(engine, job_id)
            assert job is not None and job.state == "queued" and not job.cancel_requested
            token = csrf_of(client.get(f"/admin/tasks/{job_id}"))
            done = client.post(f"/admin/tasks/{job_id}/cancel", data={"csrf": token}, follow_redirects=False)
            assert done.status_code == 303 and done.headers["location"].startswith(f"/admin/tasks/{job_id}")
            job = service_jobs.get(engine, job_id)
            assert job is not None and job.state == "cancelled"
            # Ended: a task is put again from its page, with what it asked the first time.
            again = client.post(f"/admin/tasks/{job_id}/again", data={"csrf": token}, follow_redirects=False)
            assert again.status_code == 303
            queued = [row for row in service_jobs.recent(engine, states=("queued",))]
            assert len(queued) == 1 and queued[0].params == {"repeats": 1} and queued[0].id != job_id
            missing = client.post("/admin/tasks/9999/cancel", data={"csrf": token}, follow_redirects=False)
            assert missing.status_code == 403
    finally:
        engine.dispose()


def test_logins_that_fail_too_often_are_refused_before_the_secret_is_looked_at(make_database: Callable[[], str]) -> None:
    url = make_database()
    with TestClient(create_app(settings_for(url))) as client:
        for number in range(5):
            assert login(client, "wrong").status_code == (429 if number == 4 else 401)
        right = login(client)
        assert right.status_code == 429 and "set-cookie" not in right.headers and "UTC" in right.text
        assert SECRET not in right.text
        # Another address is not blocked: the count is per address.
        client2 = TestClient(create_app(settings_for(url)), client=("198.51.100.9", 1234))
        with client2:
            assert login(client2).status_code == 303


def test_the_logout_ends_the_session_and_the_next_page_goes_to_the_login(make_database: Callable[[], str]) -> None:
    url = make_database()
    with TestClient(create_app(settings_for(url))) as client:
        login(client)
        token = csrf_of(client.get("/admin/tasks"))
        out = client.post("/admin/logout", data={"csrf": token}, follow_redirects=False)
        assert out.status_code == 303 and out.headers["location"] == "/admin/login"
        assert client.get("/admin/tasks", follow_redirects=False).status_code == 303
    with TestClient(create_app(settings_for(url))) as client:
        login(client)
        assert client.post("/admin/logout", data={"csrf": "0" * 64}, follow_redirects=False).status_code == 403
        assert client.get("/admin/tasks").status_code == 200, "a logout without the token logs nobody out"


def test_after_the_login_only_a_page_of_the_admin_ui_is_opened(make_database: Callable[[], str]) -> None:
    url = make_database()
    with TestClient(create_app(settings_for(url))) as client:
        for asked, expected in (("/admin/tasks", "/admin/tasks"), ("https://evil.example.org/", "/admin"), ("/webhooks/notifier", "/admin")):
            answer = client.post("/admin/login?next=" + asked, data={"secret": SECRET}, follow_redirects=False)
            assert answer.status_code == 303 and answer.headers["location"] == expected


def test_an_opened_session_ends_with_its_time(make_database: Callable[[], str]) -> None:
    url = make_database()
    engine = create_engine(url)
    try:
        with TestClient(create_app(settings_for(url, admin_session_seconds=3600))) as client:
            login(client)
            assert client.get("/admin/tasks").status_code == 200
            with Session(engine) as session, session.begin():
                session.execute(
                    __import__("sqlalchemy").text("UPDATE admin_session SET expires_at = now() - interval '1 second'")
                )
            assert client.get("/admin/tasks", follow_redirects=False).status_code == 303
    finally:
        engine.dispose()


def test_looking_at_a_page_asks_no_server(make_database: Callable[[], str], monkeypatch: Any) -> None:
    url = make_database()
    engine = create_engine(url)
    try:
        a_task(engine)
        fake = __import__("tests.fake_horizon", fromlist=["FakeHorizon"]).FakeHorizon()
        serve(monkeypatch, fake)
        with TestClient(create_app(settings_for(url))) as client:
            login(client)
            for path in ("/admin/tasks", "/admin/tasks/1", "/admin/tasks?state=queued&kind=pool_check"):
                assert client.get(path).status_code == 200
        assert fake.requests == []
    finally:
        engine.dispose()
