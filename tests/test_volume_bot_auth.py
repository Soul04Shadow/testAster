from decimal import Decimal
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from src.bots.volume_generator import AccountClient, AccountConfig, VolumeBotConfig, VolumeGeneratorBot
from src.utils.auth import create_signature


class DummySession:
    def get(self, *args, **kwargs):  # pragma: no cover - network not used in tests
        raise AssertionError("Unexpected GET request during unit test")

    def post(self, *args, **kwargs):  # pragma: no cover - network not used in tests
        raise AssertionError("Unexpected POST request during unit test")


def test_signed_payload_uses_hmac_signature():
    account = AccountConfig(name="demo", api_key="key", api_secret="mysecret")
    client = AccountClient(account, "https://fapi.asterdex.com", 5000, session_factory=lambda: DummySession())

    signed = client._sign_params({"symbol": "BTCUSDT", "side": "BUY"}, timestamp_ms=1710000000000)  # pylint: disable=protected-access

    expected_query = "symbol=BTCUSDT&side=BUY&recvWindow=5000&timestamp=1710000000000"
    expected_signature = create_signature(expected_query, "mysecret")

    assert signed["recvWindow"] == "5000"
    assert signed["timestamp"] == "1710000000000"
    assert signed["signature"] == expected_signature


def test_quantity_rounding_protects_against_zero():
    config = VolumeBotConfig.from_dict(
        {
            "symbol": "ASTERUSDT",
            "target_notional_usdt": "1",
            "quantity_step": "0.5",
            "hold_duration_seconds": 1,
            "cooldown_seconds": 1,
            "account_pairs": [{"long_account": "a", "short_account": "b"}],
            "accounts": [
                {"name": "a", "api_key": "key-a", "api_secret": "secret-a"},
                {"name": "b", "api_key": "key-b", "api_secret": "secret-b"},
            ],
        }
    )

    with pytest.raises(ValueError):
        config.format_quantity(Decimal("0.01"))


def test_quantity_rounding_can_skip_minimum_when_requested():
    config = VolumeBotConfig.from_dict(
        {
            "symbol": "ASTERUSDT",
            "target_notional_usdt": "5",
            "quantity_step": "0.1",
            "min_quantity": "1",
            "hold_duration_seconds": 1,
            "cooldown_seconds": 1,
            "account_pairs": [{"long_account": "a", "short_account": "b"}],
            "accounts": [
                {"name": "a", "api_key": "key-a", "api_secret": "secret-a"},
                {"name": "b", "api_key": "key-b", "api_secret": "secret-b"},
            ],
        }
    )

    assert config.format_quantity(Decimal("0.6"), enforce_min=False) == Decimal("0.6")


def test_config_accepts_quantity_usdt_alias():
    config = VolumeBotConfig.from_dict(
        {
            "symbol": "ASTERUSDT",
            "quantity_usdt": "75",
            "quantity_step": "0.01",
            "hold_duration_seconds": 1,
            "cooldown_seconds": 1,
            "account_pairs": [{"long_account": "a", "short_account": "b"}],
            "accounts": [
                {"name": "a", "api_key": "key-a", "api_secret": "secret-a"},
                {"name": "b", "api_key": "key-b", "api_secret": "secret-b"},
            ],
        }
    )

    assert config.target_notional_usdt == Decimal("75")


class _StubMarginClient:
    def __init__(self, name: str, available: str):
        self._available = Decimal(available)
        self.account = AccountConfig(name=name, api_key="key", api_secret="secret")

    def get_available_margin(self) -> Decimal:
        return self._available


def test_quantity_scales_down_when_margin_is_tight():
    config = VolumeBotConfig.from_dict(
        {
            "symbol": "ASTERUSDT",
            "quantity_usdt": "100",
            "quantity_step": "0.01",
            "leverage": 50,
            "min_free_margin_usdt": "5",
            "hold_duration_seconds": 1,
            "cooldown_seconds": 1,
            "configure_leverage": False,
            "account_pairs": [{"long_account": "long", "short_account": "short"}],
            "accounts": [
                {"name": "long", "api_key": "key-a", "api_secret": "secret-a"},
                {"name": "short", "api_key": "key-b", "api_secret": "secret-b"},
            ],
        }
    )

    bot = VolumeGeneratorBot(config, session_factory=lambda: DummySession())
    bot._clients = {  # pylint: disable=protected-access
        "long": _StubMarginClient("long", "10"),
        "short": _StubMarginClient("short", "6"),
    }

    pair = config.account_pairs[0]
    quantity = bot._calculate_quantity(pair, Decimal("1"))  # pylint: disable=protected-access

    assert quantity == Decimal("50.00")