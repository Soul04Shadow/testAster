from decimal import Decimal

from scripts.fetch_total_fees import collect_commissions, sum_commission_records, WINDOW_MS


def test_sum_commission_records_handles_negative_and_positive_values():
    records = [
        {"incomeType": "COMMISSION", "income": "-0.10", "asset": "USDT"},
        {"incomeType": "FUNDING_FEE", "income": "0.02", "asset": "USDT"},
        {"incomeType": "COMMISSION", "income": "-0.30", "asset": "USDT"},
    ]

    total, assets = sum_commission_records(records)

    assert total == Decimal("0.40")
    assert assets == ["USDT"]


class SequenceFetcher:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, params):
        self.calls.append(params)
        if self.responses:
            return self.responses.pop(0)
        return []


def test_collect_commissions_iterates_through_multiple_pages():
    responses = [
        [
            {"incomeType": "COMMISSION", "income": "-0.05", "asset": "USDT", "time": 0},
            {"incomeType": "COMMISSION", "income": "-0.15", "asset": "USDT", "time": 1},
        ],
        [
            {"incomeType": "COMMISSION", "income": "-0.20", "asset": "USDT", "time": 2},
        ],
    ]
    fetcher = SequenceFetcher(responses)

    total, assets = collect_commissions(
        fetcher,
        start_ms=0,
        end_ms=WINDOW_MS,
        limit=2,
        window_ms=WINDOW_MS,
    )

    assert total == Decimal("0.40")
    assert assets == ["USDT"]
    assert len(fetcher.calls) == 2
    assert fetcher.calls[0]["limit"] == 2
