#!/usr/local/bin/python

"""
Pipeline for converting Substance data to JSON and importing into Elasticsearch.
"""

import logging
import os
import re
from os.path import join, dirname
from dictsearch.search import iterate_dictionary
from bs4 import BeautifulSoup
import luigi
from urllib.parse import urljoin
from urllib.request import urlopen
from openfda import common, config, parallel, index_util
from openfda.common import first_file_timestamp

BASE_DIR = config.data_dir()
RAW_DIR = config.data_dir("substancedata/raw")
GINAS_ROOT_URL = "https://gsrs.ncats.nih.gov/"
SUBSTANCE_DATA_DOWNLOAD_PAGE_URL = urljoin(GINAS_ROOT_URL, "release/release.html")
SUBSTANCE_DATA_EXTRACT_DB = "substancedata/substancedata.db"


class DownloadSubstanceData(luigi.Task):

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(os.path.join(RAW_DIR, "dump-public.gsrs"))

    def run(self):
        fileURL = None
        soup = BeautifulSoup(urlopen(SUBSTANCE_DATA_DOWNLOAD_PAGE_URL).read(), "lxml")
        for a in soup.find_all(href=re.compile(".*.gsrs")):
            if "Full Public Data Dump" in a.text:
                fileURL = urljoin(GINAS_ROOT_URL, a["href"])

        common.download(fileURL, self.output().path)


class ExtractZip(luigi.Task):

    def requires(self):
        return DownloadSubstanceData()

    def output(self):
        return luigi.LocalTarget(config.data_dir("substancedata/raw/dump-public.json"))

    def run(self):
        logging.info("Extracting: %s", (self.input().path))

        extract_dir = dirname(self.input().path)
        gsrs_file_name = os.path.basename(self.input().path)
        gz_filename = os.path.splitext(gsrs_file_name)[0] + ".gz"
        gsrs_file = join(extract_dir, gsrs_file_name)

        gz_file = join(extract_dir, gz_filename)
        os.rename(gsrs_file, gz_file)
        common.shell_cmd_quiet("gunzip " + gz_file)
        os.rename(os.path.splitext(gz_file)[0], os.path.splitext(gz_file)[0] + ".json")


class SubstanceData2JSONMapper(parallel.Mapper):

    rename_map_5th_level_fields = {
        "modifications/physicalModifications/parameters/amount/lowLimit": "low_limit",
        "modifications/physicalModifications/parameters/amount/highLimit": "high_limit",
        "modifications/physicalModifications/parameters/amount/nonNumericValue": "non_numeric_value",
        "modifications/physicalModifications/parameters/amount/references": "references",
        "modifications/physicalModifications/parameters/amount/access": "remove",
        "modifications/physicalModifications/parameters/amount/created": "remove",
        "modifications/physicalModifications/parameters/amount/createdBy": "remove",
        "modifications/physicalModifications/parameters/amount/deprecated": "remove",
        "modifications/physicalModifications/parameters/amount/lastEdited": "remove",
        "modifications/physicalModifications/parameters/amount/lastEditedBy": "remove",
    }

    rename_map_4th_level_fields = {
        "mixture/components/substance/approvalID": "unii",
        "mixture/components/substance/linkingID": "linking_id",
        "mixture/components/substance/references": "references",
        "mixture/components/substance/refPname": "ref_pname",
        "mixture/components/substance/substanceClass": "substance_class",
        "mixture/components/substance/access": "remove",
        "mixture/components/substance/created": "remove",
        "mixture/components/substance/createdBy": "remove",
        "mixture/components/substance/deprecated": "remove",
        "mixture/components/substance/lastEdited": "remove",
        "mixture/components/substance/lastEditedBy": "remove",
        "modifications/agentModifications/agentSubstance/approvalID": "unii",
        "modifications/agentModifications/agentSubstance/linkingID": "linking_id",
        "modifications/agentModifications/agentSubstance/references": "references",
        "modifications/agentModifications/agentSubstance/refPname": "ref_pname",
        "modifications/agentModifications/agentSubstance/substanceClass": "substance_class",
        "modifications/agentModifications/agentSubstance/access": "remove",
        "modifications/agentModifications/agentSubstance/created": "remove",
        "modifications/agentModifications/agentSubstance/createdBy": "remove",
        "modifications/agentModifications/agentSubstance/deprecated": "remove",
        "modifications/agentModifications/agentSubstance/lastEdited": "remove",
        "modifications/agentModifications/agentSubstance/lastEditedBy": "remove",
        "modifications/agentModifications/amount/highLimit": "high_limit",
        "modifications/agentModifications/amount/lowLimit": "low_limit",
        "modifications/agentModifications/amount/nonNumericValue": "non_numeric_value",
        "modifications/agentModifications/amount/references": "references",
        "modifications/agentModifications/amount/access": "remove",
        "modifications/agentModifications/amount/created": "remove",
        "modifications/agentModifications/amount/createdBy": "remove",
        "modifications/agentModifications/amount/deprecated": "remove",
        "modifications/agentModifications/amount/lastEdited": "remove",
        "modifications/agentModifications/amount/lastEditedBy": "remove",
        "modifications/physicalModifications/parameters/parameterName": "parameter_name",
        "modifications/physicalModifications/parameters/references": "references",
        "modifications/physicalModifications/parameters/access": "remove",
        "modifications/physicalModifications/parameters/created": "remove",
        "modifications/physicalModifications/parameters/createdBy": "remove",
        "modifications/physicalModifications/parameters/deprecated": "remove",
        "modifications/physicalModifications/parameters/lastEdited": "remove",
        "modifications/physicalModifications/parameters/lastEditedBy": "remove",
        "modifications/structuralModifications/extentAmount/highLimit": "high_limit",
        "modifications/structuralModifications/extentAmount/lowLimit": "low_limit",
        "modifications/structuralModifications/extentAmount/nonNumericValue": "non_numeric_value",
        "modifications/structuralModifications/extentAmount/refPname": "ref_pname",
        "modifications/structuralModifications/extentAmount/access": "remove",
        "modifications/structuralModifications/extentAmount/created": "remove",
        "modifications/structuralModifications/extentAmount/createdBy": "remove",
        "modifications/structuralModifications/extentAmount/deprecated": "remove",
        "modifications/structuralModifications/extentAmount/lastEdited": "remove",
        "modifications/structuralModifications/extentAmount/lastEditedBy": "remove",
        "modifications/structuralModifications/molecularFragment/approvalID": "unii",
        "modifications/structuralModifications/molecularFragment/linkingID": "linking_id",
        "modifications/structuralModifications/molecularFragment/refPname": "ref_pname",
        "modifications/structuralModifications/molecularFragment/references": "references",
        "modifications/structuralModifications/molecularFragment/substanceClass": "substance_class",
        "modifications/structuralModifications/molecularFragment/access": "remove",
        "modifications/structuralModifications/molecularFragment/created": "remove",
        "modifications/structuralModifications/molecularFragment/createdBy": "remove",
        "modifications/structuralModifications/molecularFragment/deprecated": "remove",
        "modifications/structuralModifications/molecularFragment/lastEdited": "remove",
        "modifications/structuralModifications/molecularFragment/lastEditedBy": "remove",
        "modifications/structuralModifications/sites/residueIndex": "residue_index",
        "modifications/structuralModifications/sites/subunitIndex": "subunit_index",
        "nucleicAcid/linkages/sites/residueIndex": "residue_index",
        "nucleicAcid/linkages/sites/subunitIndex": "subunit_index",
        "nucleicAcid/sugars/sites/residueIndex": "residue_index",
        "nucleicAcid/sugars/sites/subunitIndex": "subunit_index",
        "polymer/classification/parentSubstance/approvalID": "unii",
        "polymer/classification/parentSubstance/linkingID": "linking_id",
        "polymer/classification/parentSubstance/refPname": "ref_pname",
        "polymer/classification/parentSubstance/substanceClass": "substance_class",
        "polymer/classification/parentSubstance/access": "remove",
        "polymer/classification/parentSubstance/created": "remove",
        "polymer/classification/parentSubstance/createdBy": "remove",
        "polymer/classification/parentSubstance/deprecated": "remove",
        "polymer/classification/parentSubstance/lastEdited": "remove",
        "polymer/classification/parentSubstance/lastEditedBy": "remove",
        "polymer/monomers/amount/highLimit": "high_limit",
        "polymer/monomers/amount/lowLimit": "low_limit",
        "polymer/monomers/amount/nonNumericValue": "non_numeric_value",
        "polymer/monomers/amount/references": "references",
        "polymer/monomers/amount/access": "remove",
        "polymer/monomers/amount/created": "remove",
        "polymer/monomers/amount/createdBy": "remove",
        "polymer/monomers/amount/deprecated": "remove",
        "polymer/monomers/amount/lastEdited": "remove",
        "polymer/monomers/amount/lastEditedBy": "remove",
        "polymer/monomers/monomerSubstance/linkingID": "linking_id",
        "polymer/monomers/monomerSubstance/references": "references",
        "polymer/monomers/monomerSubstance/refPname": "ref_pname",
        "polymer/monomers/monomerSubstance/substanceClass": "substance_class",
        "polymer/monomers/monomerSubstance/approvalID": "unii",
        "polymer/monomers/monomerSubstance/access": "remove",
        "polymer/monomers/monomerSubstance/created": "remove",
        "polymer/monomers/monomerSubstance/createdBy": "remove",
        "polymer/monomers/monomerSubstance/deprecated": "remove",
        "polymer/monomers/monomerSubstance/lastEdited": "remove",
        "polymer/monomers/monomerSubstance/lastEditedBy": "remove",
        "polymer/structural_units/amount/highLimit": "high_limit",
        "polymer/structural_units/amount/lowLimit": "low_limit",
        "polymer/structural_units/amount/nonNumericValue": "non_numeric_value",
        "polymer/structural_units/amount/access": "remove",
        "polymer/structural_units/amount/created": "remove",
        "polymer/structural_units/amount/createdBy": "remove",
        "polymer/structural_units/amount/deprecated": "remove",
        "polymer/structural_units/amount/lastEdited": "remove",
        "polymer/structural_units/amount/lastEditedBy": "remove",
        "properties/parameters/value/nonNumericValue": "non_numeric_value",
        "properties/parameters/value/access": "remove",
        "properties/parameters/value/created": "remove",
        "properties/parameters/value/createdBy": "remove",
        "properties/parameters/value/deprecated": "remove",
        "properties/parameters/value/lastEdited": "remove",
        "properties/parameters/value/lastEditedBy": "remove",
        "protein/disulfideLinks/sites/residueIndex": "residue_index",
        "protein/disulfideLinks/sites/subunitIndex": "subunit_index",
        "protein/glycosylation/CGlycosylationSites/residueIndex": "residue_index",
        "protein/glycosylation/CGlycosylationSites/subunitIndex": "subunit_index",
        "protein/glycosylation/NGlycosylationSites/residueIndex": "residue_index",
        "protein/glycosylation/NGlycosylationSites/subunitIndex": "subunit_index",
        "protein/glycosylation/OGlycosylationSites/residueIndex": "residue_index",
        "protein/glycosylation/OGlycosylationSites/subunitIndex": "subunit_index",
        "protein/otherLinks/sites/residueIndex": "residue_index",
        "protein/otherLinks/sites/subunitIndex": "subunit_index",
    }

    rename_map_3rd_level_fields = {
        "mixture/components/access": "remove",
        "mixture/components/created": "remove",
        "mixture/components/createdBy": "remove",
        "mixture/components/deprecated": "remove",
        "mixture/components/lastEdited": "remove",
        "mixture/components/lastEditedBy": "remove",
        "mixture/parentSubstance/approvalID": "unii",
        "mixture/parentSubstance/linkingID": "linking_id",
        "mixture/parentSubstance/refPname": "ref_pname",
        "mixture/parentSubstance/substanceClass": "substance_class",
        "mixture/parentSubstance/access": "remove",
        "mixture/parentSubstance/created": "remove",
        "mixture/parentSubstance/createdBy": "remove",
        "mixture/parentSubstance/deprecated": "remove",
        "mixture/parentSubstance/lastEdited": "remove",
        "mixture/parentSubstance/lastEditedBy": "remove",
        "modifications/agentModifications/agentModificationProcess": "agent_modification_process",
        "modifications/agentModifications/agentModificationRole": "agent_modification_role",
        "modifications/agentModifications/agentModificationType": "agent_modification_type",
        "modifications/agentModifications/agentSubstance": "agent_substance",
        "modifications/agentModifications/modificationGroup": "modification_group",
        "modifications/agentModifications/references": "references",
        "modifications/agentModifications/access": "remove",
        "modifications/agentModifications/created": "remove",
        "modifications/agentModifications/createdBy": "remove",
        "modifications/agentModifications/deprecated": "remove",
        "modifications/agentModifications/lastEdited": "remove",
        "modifications/agentModifications/lastEditedBy": "remove",
        "modifications/physicalModifications/modification_group": "modification_group",
        "modifications/physicalModifications/physicalModificationRole": "physical_modification_role",
        "modifications/physicalModifications/references": "references",
        "modifications/physicalModifications/access": "remove",
        "modifications/physicalModifications/created": "remove",
        "modifications/physicalModifications/createdBy": "remove",
        "modifications/physicalModifications/deprecated": "remove",
        "modifications/physicalModifications/lastEdited": "remove",
        "modifications/physicalModifications/lastEditedBy": "remove",
        "modifications/structuralModifications/extentAmount": "extent_amount",
        "modifications/structuralModifications/locationType": "location_type",
        "modifications/structuralModifications/modificationGroup": "modification_group",
        "modifications/structuralModifications/molecularFragment": "molecular_fragment",
        "modifications/structuralModifications/references": "references",
        "modifications/structuralModifications/residueModified": "residue_modified",
        "modifications/structuralModifications/structuralModificationType": "structural_modification_type",
        "modifications/structuralModifications/access": "remove",
        "modifications/structuralModifications/created": "remove",
        "modifications/structuralModifications/createdBy": "remove",
        "modifications/structuralModifications/deprecated": "remove",
        "modifications/structuralModifications/lastEdited": "remove",
        "modifications/structuralModifications/lastEditedBy": "remove",
        "moieties/countAmount/lowLimit": "low_limit",
        "moieties/countAmount/highLimit": "high_limit",
        "moieties/countAmount/nonNumericValue": "non_numeric_value",
        "moieties/countAmount/references": "references",
        "moieties/countAmount/access": "remove",
        "moieties/countAmount/created": "remove",
        "moieties/countAmount/createdBy": "remove",
        "moieties/countAmount/deprecated": "remove",
        "moieties/countAmount/lastEdited": "remove",
        "moieties/countAmount/lastEditedBy": "remove",
        "moieties/references/countAmount": "count_amount",
        "names/nameOrgs/deprecatedDate": "deprecated_date",
        "names/nameOrgs/nameOrg": "name_org",
        "names/nameOrgs/references": "references",
        "names/nameOrgs/access": "remove",
        "names/nameOrgs/created": "remove",
        "names/nameOrgs/createdBy": "remove",
        "names/nameOrgs/deprecated": "remove",
        "names/nameOrgs/lastEdited": "remove",
        "names/nameOrgs/lastEditedBy": "remove",
        "nucleicAcid/linkages/access": "remove",
        "nucleicAcid/linkages/created": "remove",
        "nucleicAcid/linkages/createdBy": "remove",
        "nucleicAcid/linkages/deprecated": "remove",
        "nucleicAcid/linkages/lastEdited": "remove",
        "nucleicAcid/linkages/lastEditedBy": "remove",
        "nucleicAcid/linkages/sitesShorthand": "remove",
        "nucleicAcid/subunits/references": "references",
        "nucleicAcid/subunits/subunitIndex": "subunit_index",
        "nucleicAcid/subunits/access": "remove",
        "nucleicAcid/subunits/created": "remove",
        "nucleicAcid/subunits/createdBy": "remove",
        "nucleicAcid/subunits/deprecated": "remove",
        "nucleicAcid/subunits/lastEdited": "remove",
        "nucleicAcid/subunits/lastEditedBy": "remove",
        "nucleicAcid/subunits/length": "remove",
        "nucleicAcid/sugars/references": "references",
        "nucleicAcid/sugars/access": "remove",
        "nucleicAcid/sugars/created": "remove",
        "nucleicAcid/sugars/createdBy": "remove",
        "nucleicAcid/sugars/deprecated": "remove",
        "nucleicAcid/sugars/lastEdited": "remove",
        "nucleicAcid/sugars/lastEditedBy": "remove",
        "nucleicAcid/sugars/sitesShorthand": "remove",
        "polymer/classification/parentSubstance": "parent_substance",
        "polymer/classification/polymerClass": "polymer_class",
        "polymer/classification/polymerGeometry": "polymer_geometry",
        "polymer/classification/polymerSubclass": "polymer_subclass",
        "polymer/classification/references": "references",
        "polymer/classification/sourceType": "source_type",
        "polymer/classification/access": "remove",
        "polymer/classification/created": "remove",
        "polymer/classification/createdBy": "remove",
        "polymer/classification/deprecated": "remove",
        "polymer/classification/lastEdited": "remove",
        "polymer/classification/lastEditedBy": "remove",
        "polymer/displayStructure/definedStereo": "defined_stereo",
        "polymer/displayStructure/ezCenters": "ez_centers",
        "polymer/displayStructure/mwt": "molecular_weight",
        "polymer/displayStructure/opticalActivity": "optical_activity",
        "polymer/displayStructure/references": "references",
        "polymer/displayStructure/stereoCenters": "stereo_centers",
        "polymer/displayStructure/access": "remove",
        "polymer/displayStructure/created": "remove",
        "polymer/displayStructure/createdBy": "remove",
        "polymer/displayStructure/deprecated": "remove",
        "polymer/displayStructure/digest": "remove",
        "polymer/displayStructure/formula": "remove",
        "polymer/displayStructure/lastEdited": "remove",
        "polymer/displayStructure/lastEditedBy": "remove",
        "polymer/displayStructure/self": "remove",
        "polymer/displayStructure/smiles": "remove",
        "polymer/idealizedStructure/definedStereo": "defined_stereo",
        "polymer/idealizedStructure/ezCenters": "ez_centers",
        "polymer/idealizedStructure/mwt": "molecular_weight",
        "polymer/idealizedStructure/opticalActivity": "optical_activity",
        "polymer/idealizedStructure/stereoCenters": "stereo_centers",
        "polymer/idealizedStructure/access": "remove",
        "polymer/idealizedStructure/created": "remove",
        "polymer/idealizedStructure/createdBy": "remove",
        "polymer/idealizedStructure/deprecated": "remove",
        "polymer/idealizedStructure/digest": "remove",
        "polymer/idealizedStructure/formula": "remove",
        "polymer/idealizedStructure/lastEdited": "remove",
        "polymer/idealizedStructure/lastEditedBy": "remove",
        "polymer/idealizedStructure/self": "remove",
        "polymer/idealizedStructure/smiles": "remove",
        "polymer/monomers/monomerSubstance": "monomer_substance",
        "polymer/monomers/references": "references",
        "polymer/monomers/access": "remove",
        "polymer/monomers/created": "remove",
        "polymer/monomers/createdBy": "remove",
        "polymer/monomers/deprecated": "remove",
        "polymer/monomers/lastEdited": "remove",
        "polymer/monomers/lastEditedBy": "remove",
        "polymer/structuralUnits/attachmentCount": "attachment_count",
        "polymer/structuralUnits/attachmentMap": "attachment_map",
        "polymer/structuralUnits/access": "remove",
        "polymer/structuralUnits/created": "remove",
        "polymer/structuralUnits/createdBy": "remove",
        "polymer/structuralUnits/deprecated": "remove",
        "polymer/structuralUnits/lastEdited": "remove",
        "polymer/structuralUnits/lastEditedBy": "remove",
        "properties/parameters/access": "remove",
        "properties/parameters/created": "remove",
        "properties/parameters/createdBy": "remove",
        "properties/parameters/deprecated": "remove",
        "properties/parameters/lastEdited": "remove",
        "properties/parameters/lastEditedBy": "remove",
        "properties/value/highLimit": "high_limit",
        "properties/value/lowLimit": "low_limit",
        "properties/value/nonNumericValue": "non_numeric_value",
        "properties/value/references": "references",
        "properties/value/access": "remove",
        "properties/value/created": "remove",
        "properties/value/createdBy": "remove",
        "properties/value/deprecated": "remove",
        "properties/value/lastEdited": "remove",
        "properties/value/lastEditedBy": "remove",
        "protein/disulfideLinks/sitesShorthand": "remove",
        "protein/glycosylation/glycosylationType": "glycosylation_type",
        "protein/glycosylation/CGlycosylationSites": "c_glycosylation_sites",
        "protein/glycosylation/NGlycosylationSites": "n_glycosylation_sites",
        "protein/glycosylation/OGlycosylationSites": "o_glycosylation_sites",
        "protein/glycosylation/references": "references",
        "protein/glycosylation/access": "remove",
        "protein/glycosylation/created": "remove",
        "protein/glycosylation/createdBy": "remove",
        "protein/glycosylation/deprecated": "remove",
        "protein/glycosylation/lastEdited": "remove",
        "protein/glycosylation/lastEditedBy": "remove",
        "protein/otherLinks/linkageType": "linkage_type",
        "protein/otherLinks/references": "references",
        "protein/otherLinks/access": "remove",
        "protein/otherLinks/created": "remove",
        "protein/otherLinks/createdBy": "remove",
        "protein/otherLinks/deprecated": "remove",
        "protein/otherLinks/lastEdited": "remove",
        "protein/otherLinks/lastEditedBy": "remove",
        "protein/subunits/references": "references",
        "protein/subunits/subunitIndex": "subunit_index",
        "protein/subunits/access": "remove",
        "protein/subunits/created": "remove",
        "protein/subunits/createdBy": "remove",
        "protein/subunits/deprecated": "remove",
        "protein/subunits/lastEdited": "remove",
        "protein/subunits/lastEditedBy": "remove",
        "protein/subunits/length": "remove",
        "relationships/amount/highLimit": "high_limit",
        "relationships/amount/lowLimit": "low_limit",
        "relationships/amount/nonNumericValue": "non_numeric_value",
        "relationships/amount/references": "references",
        "relationships/amount/access": "remove",
        "relationships/amount/created": "remove",
        "relationships/amount/createdBy": "remove",
        "relationships/amount/deprecated": "remove",
        "relationships/amount/lastEdited": "remove",
        "relationships/amount/lastEditedBy": "remove",
        "relationships/relatedSubstance/approvalID": "unii",
        "relationships/relatedSubstance/linkingID": "linking_id",
        "relationships/relatedSubstance/references": "references",
        "relationships/relatedSubstance/refPname": "ref_pname",
        "relationships/relatedSubstance/substanceClass": "substance_class",
        "relationships/relatedSubstance/access": "remove",
        "relationships/relatedSubstance/created": "remove",
        "relationships/relatedSubstance/createdBy": "remove",
        "relationships/relatedSubstance/deprecated": "remove",
        "relationships/relatedSubstance/lastEdited": "remove",
        "relationships/relatedSubstance/lastEditedBy": "remove",
        "relationships/mediatorSubstance/approvalID": "unii",
        "relationships/mediatorSubstance/linkingID": "linking_id",
        "relationships/mediatorSubstance/references": "references",
        "relationships/mediatorSubstance/refPname": "ref_pname",
        "relationships/mediatorSubstance/substanceClass": "substance_class",
        "relationships/mediatorSubstance/access": "remove",
        "relationships/mediatorSubstance/created": "remove",
        "relationships/mediatorSubstance/createdBy": "remove",
        "relationships/mediatorSubstance/deprecated": "remove",
        "relationships/mediatorSubstance/lastEdited": "remove",
        "relationships/mediatorSubstance/lastEditedBy": "remove",
        "structurallyDiverse/hybridSpeciesMaternalOrganism/approvalID": "unii",
        "structurallyDiverse/hybridSpeciesMaternalOrganism/linkingID": "linking_id",
        "structurallyDiverse/hybridSpeciesMaternalOrganism/refPname": "ref_pname",
        "structurallyDiverse/hybridSpeciesMaternalOrganism/substanceClass": "substance_class",
        "structurallyDiverse/hybridSpeciesMaternalOrganism/access": "remove",
        "structurallyDiverse/hybridSpeciesMaternalOrganism/created": "remove",
        "structurallyDiverse/hybridSpeciesMaternalOrganism/createdBy": "remove",
        "structurallyDiverse/hybridSpeciesMaternalOrganism/deprecated": "remove",
        "structurallyDiverse/hybridSpeciesMaternalOrganism/lastEdited": "remove",
        "structurallyDiverse/hybridSpeciesMaternalOrganism/lastEditedBy": "remove",
        "structurallyDiverse/hybridSpeciesPaternalOrganism/approvalID": "unii",
        "structurallyDiverse/hybridSpeciesPaternalOrganism/linkingID": "linking_id",
        "structurallyDiverse/hybridSpeciesPaternalOrganism/refPname": "ref_pname",
        "structurallyDiverse/hybridSpeciesPaternalOrganism/substanceClass": "substance_class",
        "structurallyDiverse/hybridSpeciesPaternalOrganism/access": "remove",
        "structurallyDiverse/hybridSpeciesPaternalOrganism/created": "remove",
        "structurallyDiverse/hybridSpeciesPaternalOrganism/createdBy": "remove",
        "structurallyDiverse/hybridSpeciesPaternalOrganism/deprecated": "remove",
        "structurallyDiverse/hybridSpeciesPaternalOrganism/lastEdited": "remove",
        "structurallyDiverse/hybridSpeciesPaternalOrganism/lastEditedBy": "remove",
        "structurallyDiverse/parentSubstance/approvalID": "unii",
        "structurallyDiverse/parentSubstance/linkingID": "linking_id",
        "structurallyDiverse/parentSubstance/references": "references",
        "structurallyDiverse/parentSubstance/refPname": "ref_pname",
        "structurallyDiverse/parentSubstance/substanceClass": "substance_class",
        "structurallyDiverse/parentSubstance/access": "remove",
        "structurallyDiverse/parentSubstance/created": "remove",
        "structurallyDiverse/parentSubstance/createdBy": "remove",
        "structurallyDiverse/parentSubstance/deprecated": "remove",
        "structurallyDiverse/parentSubstance/lastEdited": "remove",
        "structurallyDiverse/parentSubstance/lastEditedBy": "remove",
    }

    rename_map_2nd_level_fields = {
        "codes/codeSystem": "code_system",
        "codes/references": "references",
        "codes/access": "remove",
        "codes/created": "remove",
        "codes/createdBy": "remove",
        "codes/deprecated": "remove",
        "codes/lastEdited": "remove",
        "codes/lastEditedBy": "remove",
        "mixture/parentSubstance": "parent_substance",
        "mixture/access": "remove",
        "mixture/created": "remove",
        "mixture/createdBy": "remove",
        "mixture/deprecated": "remove",
        "mixture/lastEdited": "remove",
        "mixture/lastEditedBy": "remove",
        "modifications/agentModifications": "agent_modifications",
        "modifications/physicalModifications": "physical_modifications",
        "modifications/references": "references",
        "modifications/structuralModifications": "structural_modifications",
        "modifications/access": "remove",
        "modifications/created": "remove",
        "modifications/createdBy": "remove",
        "modifications/deprecated": "remove",
        "modifications/lastEdited": "remove",
        "modifications/lastEditedBy": "remove",
        "moieties/countAmount": "count_amount",
        "moieties/definedStereo": "defined_stereo",
        "moieties/ezCenters": "ez_centers",
        "moieties/mwt": "molecular_weight",
        "moieties/opticalActivity": "optical_activity",
        "moieties/references": "references",
        "moieties/stereoCenters": "stereo_centers",
        "moieties/stereoComments": "stereo_comments",
        "moieties/access": "remove",
        "moieties/created": "remove",
        "moieties/createdBy": "remove",
        "moieties/deprecated": "remove",
        "moieties/digest": "remove",
        "moieties/hash": "remove",
        "moieties/lastEdited": "remove",
        "moieties/lastEditedBy": "remove",
        "moieties/self": "remove",
        "names/displayName": "display_name",
        "names/domains": "domains",
        "names/nameJurisdiction": "name_jurisdiction",
        "names/nameOrgs": "name_orgs",
        "names/access": "remove",
        "names/created": "remove",
        "names/createdBy": "remove",
        "names/deprecated": "remove",
        "names/lastEdited": "remove",
        "names/lastEditedBy": "remove",
        "notes/access": "remove",
        "notes/created": "remove",
        "notes/createdBy": "remove",
        "notes/deprecated": "remove",
        "notes/lastEdited": "remove",
        "notes/lastEditedBy": "remove",
        "nucleicAcid/nucleicAcidSubType": "nucleic_acid_sub_type",
        "nucleicAcid/nucleicAcidType": "nucleic_acid_type",
        "nucleicAcid/sequenceOrigin": "sequence_origin",
        "nucleicAcid/sequenceType": "sequence_type",
        "nucleicAcid/access": "remove",
        "nucleicAcid/created": "remove",
        "nucleicAcid/createdBy": "remove",
        "nucleicAcid/deprecated": "remove",
        "nucleicAcid/lastEdited": "remove",
        "nucleicAcid/lastEditedBy": "remove",
        "polymer/displayStructure": "display_structure",
        "polymer/idealizedStructure": "idealized_structure",
        "polymer/structuralUnits": "structural_units",
        "polymer/access": "remove",
        "polymer/created": "remove",
        "polymer/createdBy": "remove",
        "polymer/deprecated": "remove",
        "polymer/lastEdited": "remove",
        "polymer/lastEditedBy": "remove",
        "properties/parameters": "parameters",
        "properties/propertyType": "property_type",
        "properties/references": "references",
        "properties/access": "remove",
        "properties/created": "remove",
        "properties/createdBy": "remove",
        "properties/deprecated": "remove",
        "properties/lastEdited": "remove",
        "properties/lastEditedBy": "remove",
        "protein/disulfideLinks": "disulfide_links",
        "protein/otherLinks": "other_links",
        "protein/proteinSubType": "protein_sub_type",
        "protein/proteinType": "protein_type",
        "protein/sequenceOrigin": "sequence_origin",
        "protein/sequenceType": "sequence_type",
        "protein/access": "remove",
        "protein/created": "remove",
        "protein/createdBy": "remove",
        "protein/deprecated": "remove",
        "protein/lastEdited": "remove",
        "protein/lastEditedBy": "remove",
        "references/nameJurisdiction": "name_jurisdiction",
        "references/nameOrgs": "name_orgs",
        "references/displayName": "display_name",
        "references/docType": "doc_type",
        "references/publicDomain": "public_domain",
        "references/documentDate": "document_date",
        "references/stereoComments": "stereo_comments",
        "references/tags": "tags",
        "references/access": "remove",
        "references/created": "remove",
        "references/createdBy": "remove",
        "references/deprecated": "remove",
        "references/lastEdited": "remove",
        "references/lastEditedBy": "remove",
        "references/uploadedFile": "remove",
        "relationships/interactionType": "interaction_type",
        "relationships/mediatorSubstance": "mediator_substance",
        "relationships/references": "references",
        "relationships/relatedSubstance": "related_substance",
        "relationships/access": "remove",
        "relationships/created": "remove",
        "relationships/createdBy": "remove",
        "relationships/deprecated": "remove",
        "relationships/lastEdited": "remove",
        "relationships/lastEditedBy": "remove",
        "relationships/originatorUuid": "remove",
        "structurallyDiverse/developmentalStage": "developmental_stage",
        "structurallyDiverse/fractionMaterialType": "fraction_material_type",
        "structurallyDiverse/fractionName": "fraction_name",
        "structurallyDiverse/hybridSpeciesMaternalOrganism": "hybrid_species_maternal_organism",
        "structurallyDiverse/hybridSpeciesPaternalOrganism": "hybrid_species_paternal_organism",
        "structurallyDiverse/infraSpecificName": "infra_specific_name",
        "structurallyDiverse/infraSpecificType": "infra_specific_type",
        "structurallyDiverse/organismAuthor": "organism_author",
        "structurallyDiverse/organismFamily": "organism_family",
        "structurallyDiverse/organismGenus": "organism_genus",
        "structurallyDiverse/organismSpecies": "organism_species",
        "structurallyDiverse/parentSubstance": "parent_substance",
        "structurallyDiverse/part": "part",
        "structurallyDiverse/partLocation": "part_location",
        "structurallyDiverse/references": "references",
        "structurallyDiverse/sourceMaterialClass": "source_material_class",
        "structurallyDiverse/sourceMaterialState": "source_material_state",
        "structurallyDiverse/sourceMaterialType": "source_material_type",
        "structurallyDiverse/access": "remove",
        "structurallyDiverse/created": "remove",
        "structurallyDiverse/createdBy": "remove",
        "structurallyDiverse/deprecated": "remove",
        "structurallyDiverse/lastEdited": "remove",
        "structurallyDiverse/lastEditedBy": "remove",
        "structure/definedStereo": "defined_stereo",
        "structure/ezCenters": "ez_centers",
        "structure/mwt": "molecular_weight",
        "structure/opticalActivity": "optical_activity",
        "structure/references": "references",
        "structure/stereoCenters": "stereo_centers",
        "structure/stereoComments": "stereo_comments",
        "structure/access": "remove",
        "structure/created": "remove",
        "structure/createdBy": "remove",
        "structure/deprecated": "remove",
        "structure/digest": "remove",
        "structure/hash": "remove",
        "structure/lastEdited": "remove",
        "structure/lastEditedBy": "remove",
        "structure/self": "remove",
    }

    rename_map_1st_level_fields = {
        "approvalID": "unii",
        "codes": "codes",
        "definitionLevel": "definition_level",
        "definitionType": "definition_type",
        "names": "names",
        "notes": "notes",
        "nucleicAcid": "nucleic_acid",
        "properties": "properties",
        "references": "references",
        "relationships": "relationships",
        "structurallyDiverse": "structurally_diverse",
        "substanceClass": "substance_class",
        "tags": "tags",
        "access": "remove",
        "approvedBy": "remove",
        "changeReason": "remove",
        "created": "remove",
        "createdBy": "remove",
        "deprecated": "remove",
        "lastEdited": "remove",
        "lastEditedBy": "remove",
        "status": "remove",
    }

    def map(self, key, value, output):
        def change_key(obj, entry, rename_map):
            child_entry_path = entry
            parent_entry_obj = obj

            # If entry has nested path find the immediate parent
            if entry.find("/") != -1:
                parent_entry_path = entry.rsplit("/", 1)[0]
                child_entry_path = entry.rsplit("/", 1)[1]
                parent_entry_obj = iterate_dictionary(obj, parent_entry_path)

            if parent_entry_obj != None:
                # Check for list
                if type(parent_entry_obj) == list:
                    for o in parent_entry_obj:
                        if iterate_dictionary(o, child_entry_path) is not None:
                            # Pop fields with empty array value
                            if rename_map[entry] == "remove":
                                o.pop(child_entry_path)
                            elif o.get(child_entry_path) == []:
                                o.pop(child_entry_path)
                            else:
                                # Rename the key
                                o[rename_map[entry]] = o.pop(child_entry_path)
                        elif child_entry_path in o:
                            o.pop(child_entry_path)
                else:
                    if parent_entry_obj.get(child_entry_path) is not None:
                        # Pop fields with empty array value
                        if rename_map[entry] == "remove":
                            parent_entry_obj.pop(child_entry_path)
                        elif parent_entry_obj.get(child_entry_path) == []:
                            parent_entry_obj.pop(child_entry_path)
                        else:
                            # Rename the key
                            parent_entry_obj[rename_map[entry]] = parent_entry_obj.pop(
                                child_entry_path
                            )

        # Change key names from the leaf nodes back up to the top nodes
        for entry in self.rename_map_5th_level_fields:
            change_key(value, entry, self.rename_map_5th_level_fields)

        for entry in self.rename_map_4th_level_fields:
            change_key(value, entry, self.rename_map_4th_level_fields)

        for entry in self.rename_map_3rd_level_fields:
            change_key(value, entry, self.rename_map_3rd_level_fields)

        for entry in self.rename_map_2nd_level_fields:
            change_key(value, entry, self.rename_map_2nd_level_fields)

        for entry in self.rename_map_1st_level_fields:
            change_key(value, entry, self.rename_map_1st_level_fields)

        output.add(key, value)


class SubstanceData2JSON(luigi.Task):
    def requires(self):
        return ExtractZip()

    def output(self):
        return luigi.LocalTarget(config.data_dir(SUBSTANCE_DATA_EXTRACT_DB))

    def run(self):
        parallel.mapreduce(
            parallel.Collection.from_glob(self.input().path, parallel.JSONLineInput()),
            mapper=SubstanceData2JSONMapper(),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
        )


class LoadJSON(index_util.LoadJSONBase):
    index_name = "substancedata"
    mapping_file = "./schemas/substancedata_mapping.json"
    data_source = SubstanceData2JSON()
    use_checksum = False
    optimize_index = True
    last_update_date = lambda _: first_file_timestamp(RAW_DIR)


if __name__ == "__main__":
    luigi.run()
