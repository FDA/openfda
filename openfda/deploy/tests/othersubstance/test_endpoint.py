# coding=utf-8
import inspect
import sys

from openfda.tests.api_test_helpers import *


def test_total_count():
    assert_total("/other/substance.json", 109200)


def test_top_level():
    meta, results = fetch("/other/substance.json?search=unii:X1468QSS0Q")
    eq_(len(results), 1)

    substance = results[0]
    eq_(substance["uuid"], "5c013332-a41e-49b8-a135-fbb0eaf7ee78")
    eq_(substance["substance_class"], "structurallyDiverse")
    eq_(substance["definition_type"], "PRIMARY")
    eq_(substance["version"], "1")
    eq_(substance["definition_level"], "COMPLETE")


def test_codes():
    meta, results = fetch("/other/substance.json?search=unii:981Y8SX18M")
    eq_(len(results), 1)

    codes = sorted(results[0]["codes"], key=lambda code: code["code"])[0]
    eq_(codes["code"], "1065221")
    eq_(codes["uuid"], "7040e30b-b3d2-ae4b-d8d2-2ab72578fa6b")
    eq_(codes["url"], "https://store.usp.org/product/1065221")
    eq_(codes["references"], ["2bec6c3a-ce05-37bb-b4f3-8770aedd51f7"])
    eq_(codes["type"], "PRIMARY")
    eq_(codes["code_system"], "RS_ITEM_NUM")


def test_mixture():
    meta, results = fetch("/other/substance.json?search=unii:F5UN44K0LE")
    eq_(len(results), 1)

    mixture = results[0]["mixture"]
    eq_(mixture["uuid"], "dbbd1271-908f-47b3-bc49-5bd11316bf9a")
    eq_(
        mixture["references"],
        [
            "b8fea4ad-ed4d-4ecc-b68f-32f0e2836b24",
            "bec2c10b-b21c-44c2-8efb-3e67ecc52d71",
        ],
    )

    components = mixture["components"][0]
    eq_(components["type"], "MUST_BE_PRESENT")
    eq_(components["uuid"], "119a7d5c-7ab7-4e00-911e-441101a6bd50")
    eq_(components["references"], [])

    substance = components["substance"]
    eq_(substance["unii"], "Z3S58SB58N")
    eq_(substance["uuid"], "e919e4ef-589f-411a-8bb6-d6c4558e14c1")
    eq_(substance["substance_class"], "reference")
    eq_(substance["refuuid"], "23ebfc5b-41d7-4798-b549-1402da529c1f")
    eq_(substance["ref_pname"], "4-HEXENAL, (4Z)-")
    eq_(substance["linking_id"], "Z3S58SB58N")
    eq_(substance["name"], "4-HEXENAL, (4Z)-")


def test_modifications():
    meta, results = fetch("/other/substance.json?search=unii:X1468QSS0Q")
    eq_(len(results), 1)

    modifications = results[0]["modifications"]
    eq_(modifications["uuid"], "03623958-d42e-4645-a319-1d642d0624ce")
    eq_(
        modifications["physical_modifications"][0]["uuid"],
        "5ea6be78-03f2-4870-80cc-cf4c3922dc63",
    )

    mod_parameters = modifications["physical_modifications"][0]["parameters"][0]
    eq_(mod_parameters["uuid"], "f576adb6-d4fa-4cc5-979e-ebf93f28e465")
    eq_(mod_parameters["parameter_name"], "HEAT")
    eq_(mod_parameters["amount"]["units"], "C")
    eq_(mod_parameters["amount"]["uuid"], "aa719c80-fc63-44a1-952c-8f736253e1f5")
    eq_(mod_parameters["amount"]["low_limit"], 60)


def test_moieties():
    meta, results = fetch("/other/substance.json?search=unii:981Y8SX18M")
    eq_(len(results), 1)

    moieties = results[0]["moieties"][0]
    eq_(moieties["count"], 1)
    eq_(moieties["smiles"], "Cl")
    eq_(
        moieties["molfile"],
        "\n  Marvin  01132110552D          \n\n  1  0  0  0  0  0            999 V2000\n   -2.3335   -4.9698    0.0000 Cl  0  0  0  0  0  0  0  0  0  0  0  0\nM  END",
    )
    eq_(moieties["defined_stereo"], 0)
    eq_(moieties["molecular_weight"], "36.461")
    eq_(moieties["ez_centers"], 0)
    eq_(moieties["charge"], 0)
    eq_(moieties["stereo_centers"], 0)
    eq_(moieties["formula"], "ClH")
    eq_(moieties["optical_activity"], "NONE")
    eq_(moieties["stereochemistry"], "ACHIRAL")
    eq_(moieties["id"], "d34f8e44-d289-4bd2-b1a8-c96965fc4435")

    count_amount = moieties["count_amount"]
    eq_(count_amount["units"], "MOL RATIO")
    eq_(count_amount["average"], 1)
    eq_(count_amount["type"], "MOL RATIO")
    eq_(count_amount["uuid"], "6a62169b-deaf-4f47-971e-db7aa4ab68d6")


def test_names():
    meta, results = fetch("/other/substance.json?search=unii:X1468QSS0Q")
    eq_(len(results), 1)

    names = results[0]["names"][0]
    eq_(names["display_name"], True)
    eq_(names["uuid"], "0ee6b739-e3ce-4cc5-8153-edeeb5858c1d")
    eq_(names["preferred"], False)
    eq_(names["languages"], ["en"])
    eq_(names["references"], ["fc0dc26c-e4a8-49a9-97b3-dc9d412bd96a"])
    eq_(names["type"], "cn")
    eq_(names["name"], "DANIO RERIO, COOKED")


def test_notes():
    meta, results = fetch("/other/substance.json?search=unii:X5B33C49HP")
    eq_(len(results), 1)

    notes = results[0]["notes"][0]
    eq_(
        notes["note"],
        "This is an incomplete polymer record. A more detailed definition may be available soon.",
    )
    eq_(notes["uuid"], None)
    eq_(notes["references"], [])


def test_nucleic_acid():
    meta, results = fetch("/other/substance.json?search=unii:3Z6W3S36X5")
    eq_(len(results), 1)

    nucleic_acid = results[0]["nucleic_acid"]
    eq_(nucleic_acid["nucleic_acid_type"], "OLIGONUCLEOTIDE")
    eq_(nucleic_acid["uuid"], "2b219491-b23a-4e0c-b301-6297410c72c7")
    eq_(
        nucleic_acid["references"],
        [
            "e1ba81ce-8fcd-4229-8683-ea33083fcef7",
            "fe33becb-5108-4398-8337-6df9d84f7fce",
        ],
    )

    sugars = nucleic_acid["sugars"][0]
    eq_(sugars["uuid"], "73318f42-0a0e-4da5-bf2d-c06ec5009216")
    eq_(sugars["sugar"], "dR")

    sites = sugars["sites"][0]
    eq_(sites["subunit_index"], 1)
    eq_(sites["residue_index"], 1)

    subunits = nucleic_acid["subunits"][0]
    eq_(subunits["subunit_index"], 1)
    eq_(subunits["uuid"], "33ee4c10-2e33-49a4-b48a-e8f4fe28b3f1")
    eq_(subunits["sequence"], "GCGTTTGCTCTTCTTCTTGCG")

    linkages = nucleic_acid["linkages"][0]
    eq_(linkages["references"], [])
    eq_(linkages["uuid"], "4d204101-896b-42a4-b72d-1f89b3ca6ea3")
    eq_(linkages["linkage"], "nasP")

    linkage_sites = linkages["sites"][0]
    eq_(linkage_sites["subunit_index"], 1)
    eq_(linkage_sites["residue_index"], 2)


def test_polymer():
    meta, results = fetch("/other/substance.json?search=unii:X5B33C49HP")
    eq_(len(results), 1)

    polymer = results[0]["polymer"]
    eq_(polymer["uuid"], "bcabb374-5683-4ec0-8d34-38c1eaceac31")
    eq_(
        polymer["references"],
        [
            "703c5b0b-aa5d-40fb-8e01-557a2ab4749d",
            "a49d7af2-6db4-4d4b-9e32-59f5e037df07",
        ],
    )

    classification = polymer["classification"]
    eq_(classification["uuid"], "445904ab-14d8-415b-bc22-5c39f65a3bd8")
    eq_(classification["polymer_class"], "HOMOPOLYMER")
    eq_(classification["polymer_geometry"], "STAR")

    idealized_structure = polymer["idealized_structure"]
    eq_(idealized_structure["count"], 1)
    eq_(idealized_structure["id"], "3c203a89-60f6-43f6-8658-b73047f20485")
    eq_(idealized_structure["references"], [])

    monomers = polymer["monomers"][0]
    eq_(monomers["uuid"], "c87d3e1a-da0b-4152-9cd5-a74ec1524534")
    eq_(monomers["type"], "MONOMER")
    eq_(monomers["defining"], False)

    monomer_substance = monomers["monomer_substance"]
    eq_(monomer_substance["unii"], "JJH7GNN18P")
    eq_(monomer_substance["uuid"], "4ed5e189-c399-4811-aa87-dd879901db63")
    eq_(monomer_substance["substance_class"], "reference")
    eq_(monomer_substance["refuuid"], "5753e233-3266-499e-9c8f-1024eb5452dd")
    eq_(monomer_substance["ref_pname"], "ETHYLENE OXIDE")
    eq_(monomer_substance["linking_id"], "JJH7GNN18P")
    eq_(monomer_substance["name"], "ETHYLENE OXIDE")

    amount = monomers["amount"]
    eq_(amount["average"], 18)
    eq_(amount["uuid"], "9e1540ef-c5c2-4fbb-a9ad-2a39ce4e1b74")
    eq_(amount["type"], "MOLE RATIO")

    display_structure = polymer["display_structure"]
    eq_(display_structure["count"], 1)
    eq_(display_structure["id"], "c522848e-6de9-4b93-83f0-e00e0980fe56")


def test_properties():
    meta, results = fetch("/other/substance.json?search=unii:X5B33C49HP")
    eq_(len(results), 1)

    properties = results[0]["properties"][0]
    eq_(properties["uuid"], "0437b524-7f20-4805-b1c1-3862016bb3d4")
    eq_(properties["type"], "amount")
    eq_(properties["property_type"], "CHEMICAL")
    eq_(properties["defining"], False)
    eq_(properties["name"], "MOL_WEIGHT:CALCULATED")

    value = properties["value"]
    eq_(value["units"], "Da")
    eq_(value["average"], 1360)
    eq_(value["type"], "CALCULATED")
    eq_(value["uuid"], "81ef2ee8-a48c-4ea8-b4bc-ff14edcaa3b1")


def test_protein():
    meta, results = fetch("/other/substance.json?search=unii:M91TG242P2")
    eq_(len(results), 1)

    protein = results[0]["protein"]
    eq_(protein["uuid"], "32077660-b8bf-4983-bbbb-35182654f11a")
    eq_(protein["protein_sub_type"], "")
    eq_(protein["sequence_type"], "COMPLETE")

    glycosylation = protein["glycosylation"]
    eq_(glycosylation["glycosylation_type"], "MAMMALIAN")
    eq_(glycosylation["uuid"], "54160485-e8d0-42e7-8cc1-9f51022f5dc8")

    n_glycosylation_sites = glycosylation["n_glycosylation_sites"][0]
    eq_(n_glycosylation_sites["subunit_index"], 1)
    eq_(n_glycosylation_sites["residue_index"], 84)

    disulfide_links_sites = protein["disulfide_links"][0]["sites"][0]
    eq_(disulfide_links_sites["subunit_index"], 1)
    eq_(disulfide_links_sites["residue_index"], 6)

    subunits = protein["subunits"][0]
    eq_(subunits["subunit_index"], 1)
    eq_(subunits["uuid"], "f721a8f4-75f5-4455-abd9-8c59a70ac2e5")

    other_links = protein["other_links"][0]
    eq_(other_links["linkage_type"], "COORDINATION COMPOUND")
    eq_(other_links["uuid"], "427c87f4-6005-4797-958e-1e098cb2c2b1")

    other_links_sites = other_links["sites"][0]
    eq_(other_links_sites["subunit_index"], 1)
    eq_(other_links_sites["residue_index"], 23)


def test_references():
    meta, results = fetch("/other/substance.json?search=unii:X1468QSS0Q")
    eq_(len(results), 1)

    references = sorted(results[0]["references"], key=lambda k: k["uuid"])
    eq_(references[2]["citation"], "NCBI")
    eq_(references[2]["uuid"], "ff641620-26d9-4cf9-81b1-0f8f95bc7ce9")
    eq_(references[2]["tags"], ["NOMEN"])
    eq_(references[2]["public_domain"], True)
    eq_(references[0]["doc_type"], "SRS")
    eq_(
        references[0]["url"],
        "http://fdasis.nlm.nih.gov/srs/srsdirect.jsp?regno=X1468QSS0Q",
    )
    eq_(references[0]["document_date"], 1493391472000)


def test_relationships():
    meta, results = fetch("/other/substance.json?search=unii:981Y8SX18M")
    eq_(len(results), 1)

    rel = sorted(results[0]["relationships"], key=lambda k: k["uuid"])[1]
    eq_(rel["type"], "PARENT->SALT/SOLVATE")
    eq_(rel["uuid"], "0d717420-619e-4e45-aa3e-d13593ea90c9")

    related_substance = rel["related_substance"]
    eq_(related_substance["unii"], "9266D9P3PQ")
    eq_(related_substance["uuid"], "294bf8e1-4639-4fc6-81ef-01e97dc51826")
    eq_(related_substance["substance_class"], "reference")
    eq_(related_substance["refuuid"], "9b2310d6-22b1-40d5-ae6e-145673ad5866")
    eq_(related_substance["ref_pname"], "BENDAMUSTINE")
    eq_(related_substance["linking_id"], "9266D9P3PQ")
    eq_(related_substance["name"], "BENDAMUSTINE")


def test_structurally_diverse():
    meta, results = fetch("/other/substance.json?search=unii:X1468QSS0Q")
    eq_(len(results), 1)

    structurally_diverse = results[0]["structurally_diverse"]
    eq_(structurally_diverse["source_material_type"], "BONY FISH")
    eq_(structurally_diverse["uuid"], "b1ab3af6-4a14-4d28-8f01-8084cc58f0aa")
    eq_(structurally_diverse["source_material_class"], "ORGANISM")
    eq_(structurally_diverse["part"], ["MUSCLE"])
    eq_(
        structurally_diverse["references"],
        [
            "ff641620-26d9-4cf9-81b1-0f8f95bc7ce9",
            "0578e457-dff1-41e3-ae2e-c61cb9308af1",
        ],
    )

    parent_substance = structurally_diverse["parent_substance"]
    eq_(parent_substance["unii"], "48HJA7IOQP")
    eq_(parent_substance["uuid"], "8c40fcc9-0950-45f8-857b-e9140d8dc692")
    eq_(parent_substance["substance_class"], "reference")
    eq_(parent_substance["refuuid"], "0bfddc8e-ff36-43d7-b549-301b9e161343")
    eq_(parent_substance["ref_pname"], "DANIO RERIO WHOLE")
    eq_(parent_substance["linking_id"], "48HJA7IOQP")
    eq_(parent_substance["name"], "DANIO RERIO WHOLE")


def test_structure():
    meta, results = fetch("/other/substance.json?search=unii:981Y8SX18M")
    eq_(len(results), 1)

    structure = results[0]["structure"]
    eq_(structure["count"], 1)
    eq_(structure["smiles"], "Cl.CN1C(CCCC(O)=O)=NC2=CC(=CC=C12)N(CCCl)CCCl")
    eq_(structure["defined_stereo"], 0)
    eq_(structure["molecular_weight"], "394.721")
    eq_(structure["atropisomerism"], "No")
    eq_(structure["ez_centers"], 0)
    eq_(structure["charge"], 0)
    eq_(
        structure["references"],
        [
            "b3f2c801-a26d-4a3e-aaa6-59d3874874ab",
            "4a37244b-6353-43d8-a8e9-384ba6686d00",
        ],
    )
    eq_(structure["stereo_centers"], 0)
    eq_(structure["stereochemistry"], "ACHIRAL")
    eq_(structure["optical_activity"], "NONE")
    eq_(structure["formula"], "C16H21Cl2N3O2.ClH")
    eq_(structure["id"], "8a878d3f-096b-4bfe-8e4a-13b62ff7179a")


def test_date_fields():
    assert_total(
        "/other/substance.json?search=_exists_:references.document_date", 90000
    )


if __name__ == "__main__":
    all_functions = inspect.getmembers(sys.modules[__name__], inspect.isfunction)
    for key, func in all_functions:
        if key.find("test_") > -1:
            func()
