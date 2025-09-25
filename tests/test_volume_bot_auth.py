from decimal import Decimal

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from web3 import Web3

from src.bots.volume_generator import (
    AccountConfig,
    AsterAgentAuthenticator,
    VolumeBotConfig,
)


def test_agent_signature_matches_documentation_example():
    account = AccountConfig(
        name="demo",
        user="0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e",
        signer="0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0",
        private_key="0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1",
    )
    auth = AsterAgentAuthenticator(account)

    payload = {
        "symbol": "SANDUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "positionSide": "BOTH",
        "quantity": "190",
        "price": "0.28694",
    }

    signed = auth.sign(payload, timestamp_ms=1749545309665, nonce_us=1748310859508867)

    assert signed["timestamp"] == "1749545309665"
    assert signed["recvWindow"] == "50000"
    assert signed["nonce"] == "1748310859508867"
    from web3 import Web3

    assert signed["user"] == Web3.to_checksum_address(account.user)
    assert signed["signer"] == Web3.to_checksum_address(account.signer)
    assert (
        signed["signature"]
        == "0x0337dd720a21543b80ff861cd3c26646b75b3a6a4b5d45805d4c1d6ad6fc33e65f0722778dd97525466560c69fbddbe6874eb4ed6f5fa7e576e486d9b5da67f31b"
    )


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
                {
                    "name": "a",
                    "user": "0x0000000000000000000000000000000000000001",
                    "signer": "0x0000000000000000000000000000000000000002",
                    "private_key": "0x0000000000000000000000000000000000000000000000000000000000000001",
                },
                {
                    "name": "b",
                    "user": "0x0000000000000000000000000000000000000003",
                    "signer": "0x0000000000000000000000000000000000000004",
                    "private_key": "0x0000000000000000000000000000000000000000000000000000000000000001",
                },
            ],
        }
    )

    with pytest.raises(ValueError):
        config.format_quantity(Decimal("0.01"))
