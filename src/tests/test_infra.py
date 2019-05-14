from gobapi.infra import start_all_services


def test_start_all_services():
    result = (
        start_all_services(
            ['exist', 'no_exist'],
            factory=lambda x: x,
            registry={'exist': lambda: 'foo'}
        )
    )
    assert len(result) == 1
    assert result[0] == 'foo'