from openfda.tests.api_test_helpers import *


def test_consumer_merge():
    meta, results = fetch("/food/event.json?search=report_number:65420")
    eq_(len(results), 1)

    event = results[0]
    eq_(event["consumer"]["gender"], "M")
    eq_(event["consumer"]["age"], "33")
    eq_(event["consumer"]["age_unit"], "year(s)")


def test_consumer_merge_with_missing_data():
    meta, results = fetch("/food/event.json?search=report_number:65619")
    eq_(len(results), 1)

    event = results[0]
    eq_(event["consumer"]["gender"], "M")
    eq_(event["consumer"]["age"], "70")
    eq_(event["consumer"]["age_unit"], "year(s)")


def test_full_record():
    meta, results = fetch("/food/event.json?search=report_number:65619")
    eq_(len(results), 1)

    event = results[0]
    eq_(event["date_created"], "20040112")
    eq_(event["date_started"], "20031222")
    eq_(event["outcomes"], ["Disability"])

    products = sorted(event["products"], key=lambda k: k["name_brand"])
    eq_(len(products), 5)
    eq_(products[0]["industry_code"], "54")
    eq_(products[0]["industry_name"], "Vit/Min/Prot/Unconv Diet(Human/Animal)")
    eq_(products[0]["name_brand"], "ACEYTL-L-CARNITINE")
    eq_(products[0]["role"], "SUSPECT")

    eq_(products[1]["industry_code"], "54")
    eq_(products[1]["industry_name"], "Vit/Min/Prot/Unconv Diet(Human/Animal)")
    eq_(products[1]["name_brand"], "ALPHA LIPOIC")
    eq_(products[1]["role"], "SUSPECT")

    eq_(products[2]["industry_code"], "54")
    eq_(products[2]["industry_name"], "Vit/Min/Prot/Unconv Diet(Human/Animal)")
    eq_(products[2]["name_brand"], "CALCIUM CALTRATE")
    eq_(products[2]["role"], "SUSPECT")

    eq_(products[3]["industry_code"], "54")
    eq_(products[3]["industry_name"], "Vit/Min/Prot/Unconv Diet(Human/Animal)")
    eq_(products[3]["name_brand"], "MULTIVITAMIN")
    eq_(products[3]["role"], "SUSPECT")

    eq_(products[4]["industry_code"], "54")
    eq_(products[4]["industry_name"], "Vit/Min/Prot/Unconv Diet(Human/Animal)")
    eq_(products[4]["name_brand"], "VITAMIN E")
    eq_(products[4]["role"], "SUSPECT")

    eq_(
        sorted(event["reactions"], key=lambda k: k),
        [
            "ASTHENIA",
            "DEPRESSED MOOD",
            "DIZZINESS",
            "IMPAIRED DRIVING ABILITY",
            "LETHARGY",
            "PHYSICAL EXAMINATION",
        ],
    )
    eq_(event["report_number"], "65619")

    eq_(event["consumer"]["gender"], "M")
    eq_(event["consumer"]["age"], "70")
    eq_(event["consumer"]["age_unit"], "year(s)")


def test_sort_by_date_created():
    meta, results = fetch(
        "/food/event.json?search=date_created:[20170220+TO+20170225]+AND+reactions:OVARIAN+CANCER&sort=date_created:asc"
    )
    eq_(len(results), 1)

    event = results[0]
    eq_(event["date_created"], "20170221")

    meta, results = fetch(
        "/food/event.json?search=date_created:[20170220+TO+20170225]+AND+reactions:OVARIAN+CANCER&sort=date_created:desc"
    )
    eq_(len(results), 1)

    event = results[0]
    eq_(event["date_created"], "20170224")
