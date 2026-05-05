from pathlib import Path


def test_daily_pipeline_script_calls_run_day() -> None:
    text = Path("scripts/run_daily_pipeline.sh").read_text(encoding="utf-8")

    assert "pixi run htcondor-accounting run-day" in text
    assert '--day "${DAY}"' in text
    assert '--output-root "${OUTPUT_ROOT}"' in text


def test_daily_reports_script_calls_render_range() -> None:
    text = Path("scripts/run_daily_reports.sh").read_text(encoding="utf-8")

    assert "pixi run htcondor-accounting render-range" in text
    assert '--start "${DAY}"' in text
    assert '--end "${DAY}"' in text
    assert '--output-root "${OUTPUT_ROOT}"' in text
