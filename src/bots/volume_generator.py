"""Delta-neutral volume generator built on top of the repo's API utilities."""

from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, getcontext
import time
from typing import Dict, List, Optional
from urllib.parse import urlencode

import requests
from requests import Response, Session

from src.utils.auth import create_signature
from src.utils.utils import log

# Ensure decimal operations maintain precision when sizing orders
getcontext().prec = 28


class VolumeBotError(RuntimeError):
    """Raised when the volume generator encounters an API error."""

    def __init__(self, message: str, response: Optional[Response] = None) -> None:
        super().__init__(message)
        self.response = response


@dataclass
class AccountConfig:
    """Configuration for an API-key authenticated account."""

    name: str
    api_key: str
    api_secret: str
    display_name: Optional[str] = None

    def label(self) -> str:
        return self.display_name or self.name


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
    recv_window: int = 5_000
    max_cycles: Optional[int] = None
    price_source_url: Optional[str] = None
    min_quantity: Optional[Decimal] = None
    configure_leverage: bool = True

    @classmethod
    def from_dict(cls, raw: Dict) -> "VolumeBotConfig":
        base_url = raw.get("base_url", "https://fapi.asterdex.com").rstrip("/")
        symbol = raw["symbol"].upper()
        target_notional = Decimal(str(raw.get("target_notional_usdt", "0")))
        leverage = int(raw.get("leverage", 50))
        quantity_step = Decimal(str(raw.get("quantity_step", "0.001")))
        hold_duration = float(raw.get("hold_duration_seconds", 2))
        cooldown = float(raw.get("cooldown_seconds", 3))
        recv_window = int(raw.get("recv_window", 5_000))
        max_cycles = raw.get("max_cycles")
        price_source_url = raw.get("price_source_url")
        min_qty = raw.get("min_quantity")
        configure_leverage = bool(raw.get("configure_leverage", True))

        if target_notional <= 0:
            raise ValueError("target_notional_usdt must be greater than zero")
        if quantity_step <= 0:
            raise ValueError("quantity_step must be a positive decimal")

        accounts = {}
        for entry in raw.get("accounts", []):
            if not entry.get("api_key") or not entry.get("api_secret"):
                raise ValueError("Each account requires api_key and api_secret")
            account = AccountConfig(
                name=entry["name"],
                api_key=entry["api_key"],
                api_secret=entry["api_secret"],
                display_name=entry.get("display_name"),
            )
            accounts[account.name] = account

        account_pairs = [
            AccountPair(long_account=item["long_account"], short_account=item["short_account"])
            for item in raw.get("account_pairs", [])
        ]

        if not accounts:
            raise ValueError("Account credentials are required")
        if not account_pairs:
            raise ValueError("At least one account pair must be configured")

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
            configure_leverage=configure_leverage,
        )

    def format_quantity(self, quantity: Decimal) -> Decimal:
        """Round quantity down to the configured step size."""
        quantized = (quantity / self.quantity_step).to_integral_value(rounding=ROUND_DOWN) * self.quantity_step
        if self.min_quantity and quantized < self.min_quantity:
            quantized = self.min_quantity
        if quantized <= 0:
            raise ValueError("calculated quantity rounded to zero; adjust target_notional_usdt or quantity_step")
        return quantized


class AccountClient:
    """Minimal REST client for a single API key/secret pair."""

    def __init__(
        self,
        account: AccountConfig,
        base_url: str,
        recv_window: int,
        session_factory=requests.Session,
    ) -> None:
        self.account = account
        self.base_url = base_url
        self.recv_window = recv_window
        self._session: Session = session_factory()

    def _sign_params(self, params: Dict[str, str], timestamp_ms: Optional[int] = None) -> OrderedDict:
        cleaned = OrderedDict()
        for key, value in params.items():
            if value is None:
                continue
            cleaned[key] = str(value)

        cleaned.setdefault("recvWindow", str(self.recv_window))
        if timestamp_ms is None:
            timestamp_ms = int(time.time() * 1000)
        cleaned["timestamp"] = str(timestamp_ms)

        query_string = urlencode(list(cleaned.items()))
        signature = create_signature(query_string, self.account.api_secret)
        cleaned["signature"] = signature
        return cleaned

    def _handle_response(self, response: Response) -> Dict:
        if response.status_code >= 400:
            try:
                payload = response.json()
            except ValueError:
                payload = response.text
            message = payload if isinstance(payload, str) else payload.get("msg") or str(payload)
            raise VolumeBotError(f"API request failed: {message}", response=response)
        if not response.text:
            return {}
        return response.json()

    def signed_get(self, path: str, params: Optional[Dict] = None) -> Dict:
        signed = self._sign_params(params or {})
        url = f"{self.base_url}{path}"
        response = self._session.get(url, params=signed, headers={"X-MBX-APIKEY": self.account.api_key}, timeout=10)
        return self._handle_response(response)

    def signed_post(self, path: str, params: Optional[Dict] = None) -> Dict:
        signed = self._sign_params(params or {})
        url = f"{self.base_url}{path}"
        response = self._session.post(
            url,
            data=signed,
            headers={"X-MBX-APIKEY": self.account.api_key, "Content-Type": "application/x-www-form-urlencoded"},
            timeout=10,
        )
        return self._handle_response(response)

    def set_leverage(self, symbol: str, leverage: int) -> None:
        try:
            self.signed_post("/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})
            log.info(f"[{self.account.label()}] Set leverage to {leverage}x for {symbol}")
        except VolumeBotError as exc:
            log.warning(f"[{self.account.label()}] Unable to set leverage: {exc}")

    def place_market_order(
        self,
        symbol: str,
        side: str,
        position_side: str,
        quantity: Decimal,
        reduce_only: bool = False,
    ) -> Dict:
        payload = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "positionSide": position_side,
            "quantity": format(quantity.normalize(), "f"),
        }
        if reduce_only:
            payload["reduceOnly"] = "true"
        response = self.signed_post("/fapi/v1/order", payload)
        price_info = response.get("avgPrice") or response.get("price") or "MARKET"
        log.trade_placed(symbol, f"{side} {position_side}", payload["quantity"], price_info)
        return response


class VolumeGeneratorBot:
    """Simple loop that opens and closes matched positions across account pairs."""

    def __init__(self, config: VolumeBotConfig, session_factory=requests.Session):
        self.config = config
        self._session_factory = session_factory
        self._clients = {
            name: AccountClient(account, config.base_url, config.recv_window, session_factory=session_factory)
            for name, account in config.accounts.items()
        }
        self._public_session: Session = session_factory()
        self._total_volume: Decimal = Decimal("0")

        if self.config.configure_leverage:
            for client in self._clients.values():
                client.set_leverage(self.config.symbol, self.config.leverage)

    @property
    def total_volume(self) -> Decimal:
        return self._total_volume

    def _get_price(self) -> Decimal:
        price_url = self.config.price_source_url or f"{self.config.base_url}/fapi/v1/ticker/price"
        params = {"symbol": self.config.symbol}
        response = self._public_session.get(price_url, params=params, timeout=10)
        if response.status_code >= 400:
            raise VolumeBotError(f"Failed to fetch price: {response.text}")
        data = response.json()
        price_value = data.get("price") if isinstance(data, dict) else None
        if price_value is None:
            raise VolumeBotError("Ticker price response missing 'price' field")
        return Decimal(str(price_value))

    def _cycle_pair(self, pair: AccountPair) -> None:
        long_client = self._clients[pair.long_account]
        short_client = self._clients[pair.short_account]

        price = self._get_price()
        quantity = self.config.format_quantity(self.config.target_notional_usdt / price)
        notional = price * quantity

        log.info(
            f"Executing delta-neutral cycle on {self.config.symbol}: qty={quantity} price={price} notional≈${notional:,.2f}"
        )

        long_client.place_market_order(self.config.symbol, "BUY", "LONG", quantity)
        short_client.place_market_order(self.config.symbol, "SELL", "SHORT", quantity)
        self._total_volume += notional * 2

        log.info(f"Holding positions for {self.config.hold_duration_seconds:.1f}s")
        time.sleep(self.config.hold_duration_seconds)

        long_client.place_market_order(
            self.config.symbol,
            "SELL",
            "LONG",
            quantity,
            reduce_only=True,
        )
        short_client.place_market_order(
            self.config.symbol,
            "BUY",
            "SHORT",
            quantity,
            reduce_only=True,
        )
        self._total_volume += notional * 2

        log.info(
            f"Closed cycle for pair {pair.long_account}/{pair.short_account}. Total generated volume: ${self._total_volume:,.2f}"
        )

    def run(self) -> None:
        log.startup("Starting simple delta-neutral volume generator")
        cycle = 0
        try:
            while self.config.max_cycles is None or cycle < self.config.max_cycles:
                for pair in self.config.account_pairs:
                    try:
                        self._cycle_pair(pair)
                    except Exception as exc:  # pylint: disable=broad-except
                        log.error(f"Cycle failed for pair {pair.long_account}/{pair.short_account}: {exc}")
                    time.sleep(self.config.cooldown_seconds)
                cycle += 1
        except KeyboardInterrupt:
            log.warning("Received keyboard interrupt, shutting down volume generator")
        finally:
            log.shutdown(f"Total notional volume generated: ${self._total_volume:,.2f}")
