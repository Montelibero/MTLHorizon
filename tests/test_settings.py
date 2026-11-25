"""The servers of the pool and the pace, as the environment says them."""

from __future__ import annotations

import pytest

from app.config import ConfigError, Settings, shown_url

BASE = {"MTL_HORIZON_DATABASE_URL": "postgresql://x"}


def urls(env: dict[str, str]) -> tuple[str, ...]:
    return Settings.from_env({**BASE, **env}).horizon_urls


def test_the_public_server_is_the_pool_when_nothing_is_said() -> None:
    assert urls({}) == ("https://horizon.stellar.org",)


def test_the_single_address_of_the_earlier_stages_still_names_the_pool() -> None:
    assert urls({"MTL_HORIZON_URL": "https://horizon.example.org"}) == ("https://horizon.example.org",)


def test_a_list_names_the_pool_with_its_first_server_as_the_primary() -> None:
    listed = urls({"MTL_HORIZON_URLS": " https://a.example.org , https://b.example.org/ ,https://c.example.org:8443"})
    assert listed == ("https://a.example.org", "https://b.example.org", "https://c.example.org:8443")


def test_both_variables_may_be_set_when_they_agree_on_the_primary() -> None:
    both = {"MTL_HORIZON_URLS": "https://a.example.org,https://b.example.org", "MTL_HORIZON_URL": "https://a.example.org/"}
    assert urls(both) == ("https://a.example.org", "https://b.example.org")


def test_two_primaries_are_refused() -> None:
    both = {"MTL_HORIZON_URLS": "https://a.example.org,https://b.example.org", "MTL_HORIZON_URL": "https://b.example.org"}
    with pytest.raises(ConfigError, match="keep one — the primary is the first of the list"):
        urls(both)


@pytest.mark.parametrize(
    "listed",
    [
        "https://a.example.org,,https://b.example.org",
        "https://a.example.org,",
        "https://a.example.org,https://A.example.org:443/",
        "https://a.example.org/history",
        "https://a.example.org/?key=1",
        "ftp://a.example.org",
        "a.example.org",
    ],
)
def test_what_is_not_a_list_of_server_addresses_is_refused(listed: str) -> None:
    with pytest.raises(ConfigError, match="MTL_HORIZON_URLS"):
        urls({"MTL_HORIZON_URLS": listed})


def test_a_secret_in_an_address_never_reaches_a_message_or_a_name() -> None:
    with pytest.raises(ConfigError) as refused:
        urls({"MTL_HORIZON_URL": "https://user:s3cret@h.example.org:bad"})
    assert "s3cret" not in str(refused.value)
    assert shown_url("https://user:s3cret@h.example.org:8443/key?token=abc#frag") == "https://h.example.org:8443"
    assert shown_url("https://user:s3cret@h.example.org") == "https://h.example.org"
    assert shown_url("not an address") == "<not an http address>"


def test_the_pool_settings_have_their_defaults_and_refuse_what_is_not_positive() -> None:
    settings = Settings.from_env(BASE)
    assert (settings.horizon_health_check_interval, settings.horizon_failover_cooldown) == (300.0, 60.0)
    assert settings.horizon_requests_per_second == 1.0
    tuned = Settings.from_env(
        {**BASE, "MTL_HORIZON_HEALTH_CHECK_INTERVAL": "60", "MTL_HORIZON_FAILOVER_COOLDOWN": "5", "MTL_HORIZON_REQUESTS_PER_SECOND": "0.5"}
    )
    assert (tuned.horizon_health_check_interval, tuned.horizon_failover_cooldown, tuned.horizon_requests_per_second) == (60.0, 5.0, 0.5)
    for name in ("HEALTH_CHECK_INTERVAL", "FAILOVER_COOLDOWN", "REQUESTS_PER_SECOND"):
        with pytest.raises(ConfigError, match=f"MTL_HORIZON_{name} must be positive"):
            Settings.from_env({**BASE, f"MTL_HORIZON_{name}": "0"})
        with pytest.raises(ConfigError, match=f"MTL_HORIZON_{name} must be a number"):
            Settings.from_env({**BASE, f"MTL_HORIZON_{name}": "soon"})
