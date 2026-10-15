"""Backward-compatible command-line entry point for the benchmark."""

from bench_runner import run


if __name__ == "__main__":
    raise SystemExit(run())
