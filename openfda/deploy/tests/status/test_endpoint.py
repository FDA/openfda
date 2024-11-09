from openfda.tests import api_test_helpers


def assert_enough_results(query, count):
    meta, results = api_test_helpers.fetch(query)
    assert (
        meta["results"]["total"] > count
    ), "Query %s returned fewer results than expected: %d vs %d" % (
        query,
        count,
        meta["results"]["total"],
    )


def test_all_green():
    status = api_test_helpers.json("/status")
    print(status)
    for ep in status:
        assert ep["status"] == "GREEN"


def test_counts():
    status = api_test_helpers.json("/status")
    assert (next(x for x in status if x["endpoint"] == "deviceevent"))[
        "documents"
    ] >= 9400000
    assert (next(x for x in status if x["endpoint"] == "devicerecall"))[
        "documents"
    ] >= 43000
    assert (next(x for x in status if x["endpoint"] == "deviceclass"))[
        "documents"
    ] >= 6176
    assert (next(x for x in status if x["endpoint"] == "devicereglist"))[
        "documents"
    ] >= 227602
    assert (next(x for x in status if x["endpoint"] == "deviceclearance"))[
        "documents"
    ] >= 145930
    assert (next(x for x in status if x["endpoint"] == "devicepma"))[
        "documents"
    ] >= 33000
    assert (next(x for x in status if x["endpoint"] == "deviceudi"))[
        "documents"
    ] >= 2480000
    assert (next(x for x in status if x["endpoint"] == "drugevent"))[
        "documents"
    ] >= 11512000
    assert (next(x for x in status if x["endpoint"] == "druglabel"))[
        "documents"
    ] >= 160000
    assert (next(x for x in status if x["endpoint"] == "foodevent"))[
        "documents"
    ] >= 83000
    assert (next(x for x in status if x["endpoint"] == "deviceenforcement"))[
        "documents"
    ] >= 10000
    assert (next(x for x in status if x["endpoint"] == "drugenforcement"))[
        "documents"
    ] >= 5000
    assert (next(x for x in status if x["endpoint"] == "foodenforcement"))[
        "documents"
    ] >= 1000
    assert (next(x for x in status if x["endpoint"] == "animalandveterinarydrugevent"))[
        "documents"
    ] >= 1040000
    assert (next(x for x in status if x["endpoint"] == "drugsfda"))[
        "documents"
    ] >= 24000
