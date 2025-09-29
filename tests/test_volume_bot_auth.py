from decimal import Decimal
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from src.bots.volume_generator import AccountClient, AccountConfig, VolumeBotConfig
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