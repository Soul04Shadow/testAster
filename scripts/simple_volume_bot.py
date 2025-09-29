"""CLI entrypoint for the simplified volume generation bot."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.bots import VolumeBotConfig, VolumeGeneratorBot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a simple Aster delta-neutral volume bot")
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the JSON configuration file (see volume_bot_config.example.json)",
    )
    return parser.parse_args()


def load_config(path: Path) -> VolumeBotConfig:
    try:
        data = json.loads(path.read_text())
    except FileNotFoundError as exc:
        raise SystemExit(f"Configuration file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in configuration file {path}: {exc}") from exc
    return VolumeBotConfig.from_dict(data)


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    bot = VolumeGeneratorBot(config)
    bot.run()


if __name__ == "__main__":
    main()
