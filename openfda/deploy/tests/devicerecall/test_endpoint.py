
from openfda.tests.api_test_helpers import *


def test_pma_parsed_out():
    meta, results = fetch(
        "/device/recall.json?search=product_res_number.exact:Z-0567-2013"
    )
    eq_(
        sorted(results[0]["pma_numbers"]), ["P960009S007", "P960009S051", "P960009S134"]
    )


def test_510k_parsed_out():
    meta, results = fetch(
        "/device/recall.json?search=product_res_number.exact:Z-1535-2009"
    )
    eq_(sorted(results[0]["k_numbers"]), ["K003205", "K984357", "K990848", "K991723"])


def test_product_code_ooo():
    meta, results = fetch(
        "/device/recall.json?search=product_res_number.exact:Z-2153-2014"
    )
    eq_(results[0]["product_code"], "N/A")


def test_product_code_na():
    meta, results = fetch(
        "/device/recall.json?search=product_res_number.exact:Z-0094-2021"
    )
    eq_(results[0]["product_code"], "N/A")


def test_old_product_code():
    meta, results = fetch(
        "/device/recall.json?search=product_res_number.exact:Z-0351-03+AND+res_event_number:25150"
    )
    eq_(results[0]["product_code"], "74")


def test_other_submission_description():
    meta, results = fetch(
        "/device/recall.json?search=product_res_number.exact:Z-2153-2014"
    )
    eq_(results[0]["other_submission_description"], "Exempt")

    meta, results = fetch(
        "/device/recall.json?search=product_res_number.exact:Z-1650-2010"
    )
    eq_(results[0]["other_submission_description"], "Illegally Marketed")


def test_unparseable_address_flattened():
    meta, results = fetch(
        "/device/recall.json?search=product_res_number.exact:Z-0432-2021"
    )
    eq_(
        results[0]["recalling_firm"],
        "Inpeco S.A. Via San Gottardo 10 Lugano Switzerland",
    )
    eq_(results[0].get("address_1"), None)
    eq_(results[0].get("address_2"), None)
    eq_(results[0].get("city"), None)
    eq_(results[0].get("state"), None)
    eq_(results[0].get("postal_code"), None)
    eq_(results[0].get("country"), None)


def test_one_complete_record():
    meta, results = fetch(
        "/device/recall.json?search=product_res_number.exact:Z-0164-2014"
    )
    eq_(results[0]["cfres_id"], "122136")
    eq_(results[0]["res_event_number"], "66351")
    eq_(results[0]["product_res_number"], "Z-0164-2014")
    eq_(results[0]["firm_fei_number"], "1217052")
    eq_(results[0]["event_date_terminated"], "2016-08-22")
    eq_(results[0]["event_date_initiated"], "2013-09-23")
    eq_(results[0]["event_date_posted"], "2013-11-05")
    eq_(results[0]["recall_status"], "Terminated")
    eq_(
        results[0]["product_description"],
        "Portex¿ Saddleblock Tray with Drugs  25g Quincke; 4795-20\n\nThe Regional Anesthesia family of products is comprised of sterile (unless otherwise indicated in the product scope), single-use devices designed to perform epidural, spinal, combined epidural/spinal, nerve block, lumbar puncture and regional anesthesia procedures. The spinal products are comprised of spinal needles, introducer needles and accessories required to perform a spinal procedure.",
    )
    eq_(results[0]["code_info"], "Lot: 2455104")
    eq_(
        results[0]["action"],
        "Smiths Medical sent an Urgent Recall Notice dated September 23, 2013, to all affected customers.  The notice identified the product, the problem, and the action to be taken by the customer.  Customers were instructed to inspect their inventory for the affected product and remove from use.  Complete the Confirmation Form and return by fax to 603-358-1017 or by email to spinal@smiths-medical.com.  Upon receipt of the completed form, a customer service representative would contact them to arrange for exchange of their unused affected devices for credit or replacement.  Customers were also instructed to forward the notice to all personnel who need to be aware within their organization.  Customers with questions were instructed to contact Smiths Medical's Customer Service Department at 800-258-5361.\r\nFor questions regarding this recall call 800-258-5361.",
    )
    eq_(results[0]["product_quantity"], "222")
    eq_(
        results[0]["distribution_pattern"],
        "Nationwide Distribution including AK, AR, AZ, CA, CT, FL, GA, HI, IA, ID, IL, IN, KS, KY, LA, MA, MD, ME, MI, MN, MO, MS, NC, NE, NJ, NY, OH, OK, OR, PA, RI, SC, TX, UT, VA, VT, WA, WI, WY.",
    )
    eq_(results[0]["recalling_firm"], "Smiths Medical ASD, Inc.")
    eq_(results[0]["address_1"], "10 Bowman Dr")
    eq_(results[0].get("address_2"), None)
    eq_(results[0]["city"], "Keene")
    eq_(results[0]["state"], "NH")
    eq_(results[0]["postal_code"], "03431-5043")
    eq_(results[0]["additional_info_contact"], "800-258-5361")
    eq_(results[0]["root_cause_description"], "Material/Component Contamination")
    eq_(
        results[0]["reason_for_recall"],
        "Visual particulate in the glass ampules of 5% Lidocaine HCL and 7.5% Dextrose Injection, USP, 2 mL Ampoule, NDC # 0409-4712-01, Hospira Lot Number 23-227-DK.  These ampules are included in certain Portex Spinal Anaesthesia Trays.",
    )
    eq_(results[0]["product_code"], "CAZ")
    eq_(results[0].get("k_numbers"), None)
    eq_(results[0]["pma_numbers"], ["D028296"])
    eq_(results[0].get("other_submission_description"), None)

    eq_(results[0]["openfda"]["device_name"], "Anesthesia Conduction Kit")
    eq_(results[0]["openfda"]["medical_specialty_description"], "Anesthesiology")
    eq_(results[0]["openfda"]["device_class"], "2")
    eq_(results[0]["openfda"]["regulation_number"], "868.5140")


def test_fei_number_in_place():
    assert_total("/device/recall.json?search=firm_fei_number:3001236905", 12)


def test_grouping_by_event_number():
    assert_total("/device/recall.json?search=res_event_number:81166", 20)


def test_exact_count_0():
    assert_count_top(
        "/device/recall.json?count=root_cause_description.exact",
        [
            "Device Design",
            "Nonconforming Material/Component",
            "Process control",
            "Software design",
            "Other",
        ],
    )


def test_date_1():
    assert_total("/device/recall.json?search=event_date_terminated:20050922", 7)


def test_date_range_2():
    assert_total(
        "/device/recall.json?search=event_date_terminated:([20050922+TO+20051231])", 600
    )


def test_openfda_3():
    assert_total("/device/recall.json?search=openfda.regulation_number:878.4370", 700)


def test_dupe_removal_fda_392():
    assert_total_exact(
        "/device/recall.json?search=product_res_number.exact:Z-1844-2017", 1
    )
    assert_total_exact(
        "/device/recall.json?search=product_res_number.exact:Z-0592-2011", 1
    )
    meta, results = fetch(
        "/device/recall.json?search=product_res_number.exact:Z-0592-2011"
    )
    eq_(results[0]["res_event_number"], "56864")
    eq_(results[0]["product_res_number"], "Z-0592-2011")
    eq_(results[0]["firm_fei_number"], "3000206154")
    eq_(results[0]["event_date_terminated"], "2010-12-14")
    eq_(results[0]["root_cause_description"], "Error in labeling")
    eq_(results[0]["product_code"], "KKX")
    eq_(results[0].get("pma_numbers"), None)
    eq_(results[0]["k_numbers"], ["K050322"])
    eq_(results[0]["openfda"]["device_name"], "Drape, Surgical")
    eq_(
        results[0]["openfda"]["medical_specialty_description"],
        "General, Plastic Surgery",
    )
    eq_(results[0]["openfda"]["device_class"], "2")
    eq_(results[0]["openfda"]["regulation_number"], "878.4370")
