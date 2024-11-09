# coding=utf-8
import inspect
import sys

from openfda.tests.api_test_helpers import *


def test_nullified_records():
    NULLIFIED = [
        "USA-FDACVM-2018-US-045311",
        "USA-FDACVM-2018-US-048571",
        "USA-FDACVM-2018-US-046672",
        "USA-FDACVM-2017-US-070108",
        "USA-FDACVM-2017-US-002864",
        "USA-FDACVM-2017-US-002866",
        "USA-FDACVM-2017-US-052458",
        "USA-FDACVM-2017-US-055193",
        "USA-FDACVM-2017-US-043931",
        "USA-FDACVM-2018-US-002321",
        "USA-FDACVM-2017-US-042492",
        "USA-FDACVM-2018-US-044065",
    ]

    for case_num in NULLIFIED:
        meta, results = fetch(
            "/animalandveterinary/event.json?search=unique_aer_id_number:" + case_num
        )
        eq_(results, None)


def test_single_ae_record():
    meta, results = fetch(
        "/animalandveterinary/event.json?search=unique_aer_id_number:USA-USFDACVM-2015-US-094810"
    )
    eq_(len(results), 1)

    ae = results[0]

    eq_("USA-USFDACVM-2015-US-094810", ae["unique_aer_id_number"])
    eq_(None, ae.get("@id"))
    eq_("N141251", ae["report_id"])
    eq_("20150126", ae["original_receive_date"])
    eq_(
        "Food and Drug Administration Center for Veterinary Medicine",
        ae["receiver"]["organization"],
    )
    eq_("7500 Standish Place (HFV-210) Room N403", ae["receiver"]["street_address"])
    eq_("Rockville", ae["receiver"]["city"])
    eq_("MD", ae["receiver"]["state"])
    eq_("20855", ae["receiver"]["postal_code"])
    eq_("USA", ae["receiver"]["country"])
    eq_("Other", ae["primary_reporter"])
    eq_("Safety Issue", ae["type_of_information"])
    eq_("true", ae["serious_ae"])
    eq_("1", ae["number_of_animals_treated"])
    eq_("1", ae["number_of_animals_affected"])
    eq_("Dog", ae["animal"]["species"])
    eq_("Male", ae["animal"]["gender"])
    eq_("Neutered", ae["animal"]["reproductive_status"])
    eq_("NOT APPLICABLE", ae["animal"]["female_animal_physiological_status"])

    eq_("1.00", ae["animal"]["age"]["min"])
    eq_(None, ae["animal"]["age"].get("max"))
    eq_("Year", ae["animal"]["age"]["unit"])
    eq_("Measured", ae["animal"]["age"]["qualifier"])

    eq_("38.419", ae["animal"]["weight"]["min"])
    eq_(None, ae["animal"]["weight"].get("max"))
    eq_("Kilogram", ae["animal"]["weight"]["unit"])
    eq_("Measured", ae["animal"]["weight"]["qualifier"])

    eq_("false", ae["animal"]["breed"]["is_crossbred"])
    eq_("Retriever - Labrador", ae["animal"]["breed"]["breed_component"])

    eq_("Recovered/Normal", ae["outcome"][0]["medical_status"])
    eq_("1", ae["outcome"][0]["number_of_animals_affected"])

    eq_("Good", ae["health_assessment_prior_to_exposure"]["condition"])
    eq_("Veterinarian", ae["health_assessment_prior_to_exposure"]["assessed_by"])

    eq_("20141222", ae["onset_date"])
    eq_({"value": "4", "unit": "Week"}, ae.get("duration"))

    eq_("11", ae["reaction"][0]["veddra_version"])
    eq_("129", ae["reaction"][0]["veddra_term_code"])
    eq_("Vocalisation", ae["reaction"][0]["veddra_term_name"])
    eq_("1", ae["reaction"][0]["number_of_animals_affected"])
    eq_("Actual", ae["reaction"][0]["accuracy"])

    eq_("11", ae["reaction"][1]["veddra_version"])
    eq_("960", ae["reaction"][1]["veddra_term_code"])
    eq_("Pruritus", ae["reaction"][1]["veddra_term_name"])
    eq_("1", ae["reaction"][1]["number_of_animals_affected"])
    eq_("Actual", ae["reaction"][1]["accuracy"])

    eq_(None, ae.get("time_between_exposure_and_onset"))
    eq_("false", ae["treated_for_ae"])

    eq_(1, len(ae["drug"]))

    eq_("20141222", ae["drug"][0]["first_exposure_date"])
    eq_("20141222", ae["drug"][0]["last_exposure_date"])

    eq_("Animal Owner", ae["drug"][0]["administered_by"])
    eq_("Topical", ae["drug"][0]["route"])
    eq_("1", ae["drug"][0]["dose"]["numerator"])
    eq_("tube", ae["drug"][0]["dose"]["numerator_unit"])
    eq_("1", ae["drug"][0]["dose"]["denominator"])
    eq_("dose", ae["drug"][0]["dose"]["denominator_unit"])
    eq_("false", ae["drug"][0].get("used_according_to_label"))
    eq_("Overdosed", ae["drug"][0].get("off_label_use"))
    eq_("false", ae["drug"][0]["previous_exposure_to_drug"])
    eq_(None, ae["drug"][0].get("previous_ae_to_drug"))
    eq_(None, ae["drug"][0].get("ae_abated_after_stopping_drug"))
    eq_(None, ae["drug"][0].get("ae_reappeared_after_resuming_drug"))
    eq_(None, ae["drug"][0].get("manufacturing_date"))
    eq_("KP09ECX KP09C4D", ae["drug"][0].get("lot_number"))
    eq_("2017-01", ae["drug"][0].get("lot_expiration"))
    eq_("000859-2339", ae["drug"][0].get("product_ndc"))
    eq_("MSK", ae["drug"][0]["brand_name"])
    eq_("Solution", ae["drug"][0]["dosage_form"])
    eq_("MSK", ae["drug"][0]["manufacturer"]["name"])
    eq_("USA-USFDACVM-N141251", ae["drug"][0]["manufacturer"]["registration_number"])
    eq_(None, ae["drug"][0].get("number_of_defective_items"))
    eq_(None, ae["drug"][0].get("number_of_items_returned"))
    eq_("QP54AB52", ae["drug"][0]["atc_vet_code"])
    eq_("Imidacloprid", ae["drug"][0]["active_ingredients"][0]["name"])
    eq_("500", ae["drug"][0]["active_ingredients"][0]["dose"]["numerator"])
    eq_("Milligram", ae["drug"][0]["active_ingredients"][0]["dose"]["numerator_unit"])
    eq_("5", ae["drug"][0]["active_ingredients"][0]["dose"]["denominator"])
    eq_("mL", ae["drug"][0]["active_ingredients"][0]["dose"]["denominator_unit"])


if __name__ == "__main__":
    all_functions = inspect.getmembers(sys.modules[__name__], inspect.isfunction)
    for key, func in all_functions:
        if key.find("test_") > -1:
            func()
