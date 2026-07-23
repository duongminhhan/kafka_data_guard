from scripts.game_day_batch import build_workload, default_key_base


def test_default_batch_has_twenty_transactions_and_three_zero_event_items() -> None:
    work = build_workload(
        total=20,
        zero_event_count=3,
        owner="C##CDCUSER",
        captured_table="CDC_REMEDIATION_POC",
        zero_event_table="CDC_ZERO_EVENT_TEST",
        seed=20260721,
        key_base=1000,
    )

    assert len(work) == 20
    assert sum(item.captured_events == 0 for item in work) == 3
    assert all(item.captured_events in {0, 2, 3} for item in work)
    assert all(
        item.table == "C##CDCUSER.CDC_ZERO_EVENT_TEST"
        for item in work
        if item.captured_events == 0
    )
    assert sum(item.captured_events for item in work) == 42


def test_default_key_range_fits_number_10() -> None:
    base = default_key_base(total=20, precision=10)

    assert 1 <= base
    assert base + 20 <= 9_999_999_999
