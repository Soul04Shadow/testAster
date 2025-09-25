"""Simplified delta-neutral volume generation bot for Aster Pro."""
from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Dict, List, Optional

import requests
from eth_abi import encode as abi_encode
from eth_account import Account
from eth_account.messages import encode_defunct
from requests import Response, Session
from web3 import Web3

# Increase decimal precision for leverage/quantity calculations
getcontext().prec = 28


class AsterProClientError(RuntimeError):
    """Raised when the Aster Pro API returns an error response."""

    def __init__(self, message: str, response: Optional[Response] = None):
        super().__init__(message)
        self.response = response


@dataclass
class AccountConfig:
    """Configuration for an individual trading account."""

    name: str
    user: str
    signer: str
    private_key: str
    display_name: Optional[str] = None

    def __post_init__(self) -> None:
        # Normalise addresses to checksum format for signing.
        self.user_checksum = Web3.to_checksum_address(self.user)
        self.signer_checksum = Web3.to_checksum_address(self.signer)
        # eth_account expects hex prefixed keys
        if not self.private_key.startswith("0x"):
            raise ValueError("Private keys must be 0x-prefixed hex strings")


@dataclass
class AccountPair:
    """Pair of accounts used to build delta-neutral exposure."""

    long_account: str
    short_account: str


@dataclass
class VolumeBotConfig:
    """Runtime configuration for the volume generator bot."""

    base_url: str
    symbol: str
    target_notional_usdt: Decimal
    leverage: int
    quantity_step: Decimal
    hold_duration_seconds: float
    cooldown_seconds: float
    account_pairs: List[AccountPair]
    accounts: Dict[str, AccountConfig]
    recv_window: int = 50_000
    max_cycles: Optional[int] = None
    price_source_url: Optional[str] = None
    min_quantity: Optional[Decimal] = None

    @classmethod
    def from_dict(cls, raw: Dict) -> "VolumeBotConfig":
        base_url = raw.get("base_url", "https://fapi.asterdex.com")
        symbol = raw["symbol"].upper()
        target_notional = Decimal(str(raw.get("target_notional_usdt", "0")))
        leverage = int(raw.get("leverage", 50))
        quantity_step = Decimal(str(raw.get("quantity_step", "0.001")))
        hold_duration = float(raw.get("hold_duration_seconds", 2))
        cooldown = float(raw.get("cooldown_seconds", 3))
        recv_window = int(raw.get("recv_window", 50_000))
        max_cycles = raw.get("max_cycles")
        min_qty = raw.get("min_quantity")
        price_source_url = raw.get("price_source_url")

        if target_notional <= 0:
            raise ValueError("target_notional_usdt must be greater than zero")
        if quantity_step <= 0:
            raise ValueError("quantity_step must be a positive decimal")

        accounts = {
            entry["name"]: AccountConfig(
                name=entry["name"],
                user=entry["user"],
                signer=entry["signer"],
                private_key=entry["private_key"],
                display_name=entry.get("display_name"),
            )
            for entry in raw.get("accounts", [])
        }

        account_pairs = [
            AccountPair(long_account=item["long_account"], short_account=item["short_account"])
            for item in raw.get("account_pairs", [])
        ]

        if not account_pairs:
            raise ValueError("At least one account pair must be configured")
        if not accounts:
            raise ValueError("Account credentials are required")

        if isinstance(max_cycles, str) and max_cycles.strip():
            max_cycles = int(max_cycles)
        elif max_cycles is not None:
            max_cycles = int(max_cycles)

        min_quantity_decimal = Decimal(str(min_qty)) if min_qty is not None else None

        return cls(
            base_url=base_url,
            symbol=symbol,
            target_notional_usdt=target_notional,
            leverage=leverage,
            quantity_step=quantity_step,
            hold_duration_seconds=hold_duration,
            cooldown_seconds=cooldown,
            account_pairs=account_pairs,
            accounts=accounts,
            recv_window=recv_window,
            max_cycles=max_cycles,
            price_source_url=price_source_url,
            min_quantity=min_quantity_decimal,
        )

    def format_quantity(self, quantity: Decimal) -> Decimal:
        """Round quantity down to the configured step size."""
        quantized = (quantity / self.quantity_step).to_integral_value(rounding=ROUND_DOWN) * self.quantity_step
        if self.min_quantity and quantized < self.min_quantity:
            quantized = self.min_quantity
        if quantized <= 0:
            raise ValueError("calculated quantity rounded to zero; adjust target_notional_usdt or quantity_step")
        return quantized


def _trim_dict(data: Dict) -> Dict:
    """Convert nested payload items to strings and remove None values."""
    result = {}
    for key, value in data.items():
        if value is None:
            continue
        if isinstance(value, dict):
            result[key] = json.dumps(_trim_dict(value), separators=(",", ":"))
        elif isinstance(value, list):
            coerced: List = []
            for item in value:
                if isinstance(item, dict):
                    coerced.append(json.dumps(_trim_dict(item), separators=(",", ":")))
                else:
                    coerced.append(str(item))
            result[key] = json.dumps(coerced, separators=(",", ":"))
        else:
            result[key] = str(value)
    return result


class AsterAgentAuthenticator:
    """Implements the agent-based signing scheme described in the API docs."""

    def __init__(self, account: AccountConfig, recv_window: int = 50_000):
        self.account = account
        self.recv_window = recv_window

    def sign(self, payload: Dict, *, timestamp_ms: Optional[int] = None, nonce_us: Optional[int] = None) -> Dict:
        cleaned = _trim_dict(dict(payload))
        cleaned.setdefault("recvWindow", str(self.recv_window))
        if timestamp_ms is None:
            timestamp_ms = int(time.time() * 1000)
        cleaned["timestamp"] = str(timestamp_ms)

        json_str = json.dumps(cleaned, sort_keys=True, separators=(",", ":"))

        if nonce_us is None:
            nonce_us = math.trunc(time.time() * 1_000_000)

        encoded = abi_encode(
            ["string", "address", "address", "uint256"],
            [json_str, self.account.user_checksum, self.account.signer_checksum, nonce_us],
        )
        keccak_hex = Web3.keccak(encoded).hex()

        message = encode_defunct(hexstr=keccak_hex)
        signature = Account.sign_message(signable_message=message, private_key=self.account.private_key)

        signed_payload = cleaned.copy()
        signed_payload.update(
            {
                "nonce": str(nonce_us),
                "user": self.account.user_checksum,
                "signer": self.account.signer_checksum,
                "signature": "0x" + signature.signature.hex(),
            }
        )
        return signed_payload


class AsterProClient:
    """Lightweight HTTP client focused on the endpoints required by the simple bot."""

    def __init__(self, account: AccountConfig, base_url: str, recv_window: int = 50_000, session: Optional[Session] = None):
        self.account = account
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.auth = AsterAgentAuthenticator(account, recv_window=recv_window)

    def _handle_response(self, response: Response) -> Dict:
        try:
            data = response.json()
        except ValueError as exc:  # pragma: no cover - defensive branch
            raise AsterProClientError("Invalid JSON response", response=response) from exc

        if response.status_code >= 400:
            message = data.get("msg") or data.get("message") or response.text
            raise AsterProClientError(f"API error ({response.status_code}): {message}", response=response)
        return data

    def post_private(self, path: str, payload: Dict) -> Dict:
        url = f"{self.base_url}{path}"
        signed_payload = self.auth.sign(payload)
        response = self.session.post(
            url,
            data=signed_payload,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "AsterVolumeBot/0.1",
            },
            timeout=10,
        )
        return self._handle_response(response)

    def place_market_order(
        self,
        *,
        symbol: str,
        side: str,
        quantity: Decimal,
        position_side: str,
        reduce_only: bool = False,
    ) -> Dict:
        payload = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "positionSide": position_side,
            "quantity": f"{quantity:f}",
            "reduceOnly": "true" if reduce_only else "false",
        }
        return self.post_private("/fapi/v3/order", payload)


class VolumeGeneratorBot:
    """Coordinates delta-neutral market orders across multiple accounts."""

    def __init__(self, config: VolumeBotConfig, session_factory=requests.Session):
        self.config = config
        self.session_factory = session_factory
        self.clients = {
            name: AsterProClient(account=acct, base_url=config.base_url, recv_window=config.recv_window, session=session_factory())
            for name, acct in config.accounts.items()
        }
        self.total_volume = Decimal("0")
        self.total_trades = 0

    def get_last_price(self) -> Decimal:
        url = self.config.price_source_url or f"{self.config.base_url}/fapi/v1/ticker/price"
        response = requests.get(url, params={"symbol": self.config.symbol}, timeout=10)
        response.raise_for_status()
        payload = response.json()
        price = payload["price"] if isinstance(payload, dict) else payload[0]["price"]
        return Decimal(str(price))

    def _execute_pair_cycle(self, pair: AccountPair, quantity: Decimal, notional: Decimal) -> None:
        long_client = self.clients[pair.long_account]
        short_client = self.clients[pair.short_account]

        print(f"  Opening positions: {pair.long_account} LONG / {pair.short_account} SHORT (qty={quantity})")
        long_client.place_market_order(symbol=self.config.symbol, side="BUY", quantity=quantity, position_side="LONG")
        short_client.place_market_order(symbol=self.config.symbol, side="SELL", quantity=quantity, position_side="SHORT")

        time.sleep(self.config.hold_duration_seconds)

        print(f"  Closing positions: {pair.long_account} LONG / {pair.short_account} SHORT")
        long_client.place_market_order(
            symbol=self.config.symbol,
            side="SELL",
            quantity=quantity,
            position_side="LONG",
            reduce_only=True,
        )
        short_client.place_market_order(
            symbol=self.config.symbol,
            side="BUY",
            quantity=quantity,
            position_side="SHORT",
            reduce_only=True,
        )

        trades_in_cycle = 4
        self.total_trades += trades_in_cycle
        self.total_volume += notional * trades_in_cycle

    def run(self) -> None:
        cycle = 0
        print("Starting Aster delta-neutral volume generator...")
        print(f"Symbol: {self.config.symbol}, leverage: {self.config.leverage}x, target notional: {self.config.target_notional_usdt} USDT")
        print(f"Account pairs: {[f'{p.long_account}->{p.short_account}' for p in self.config.account_pairs]}")

        try:
            while self.config.max_cycles is None or cycle < self.config.max_cycles:
                cycle += 1
                price = self.get_last_price()
                quantity = self.config.format_quantity(self.config.target_notional_usdt / price)
                notional = quantity * price

                print(f"\nCycle {cycle}: last price {price:.6f} -> quantity {quantity} ({notional:.2f} USDT per leg)")

                for pair in self.config.account_pairs:
                    self._execute_pair_cycle(pair, quantity, notional)

                print(
                    f"Cycle {cycle} complete: total trades={self.total_trades}, total volume={self.total_volume:.2f} USDT"
                )

                time.sleep(self.config.cooldown_seconds)
        except KeyboardInterrupt:
            print("\nBot stopped by user. Final volume: {:.2f} USDT across {} trades.".format(self.total_volume, self.total_trades))
        except requests.RequestException as exc:
            raise RuntimeError(f"Network error when communicating with Aster Pro: {exc}") from exc
