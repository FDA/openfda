#!/usr/bin/python

""" Device pipeline for downloading, transforming to JSON and loading device
    recall data into Elasticsearch.
"""
import csv
import glob
import os
import re
from os.path import dirname, join
from urllib.parse import quote

import arrow
import luigi
import numpy as np
import pandas as pd
import usaddress

from openfda import common, config, index_util, parallel
from openfda.common import soup_with_retry
from openfda.device_harmonization.pipeline import (
    Harmonized2OpenFDA,
    DeviceAnnotateMapper,
)
from openfda.tasks import AlwaysRunTask, NoopTask

DEVICE_RECALL_DATA_DIR = config.data_dir("device_recall")
SUMMARY_DOWNLOAD_URL = (
    "https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfRes/res.cfm?"
    "start_search=1&event_id=&productdescriptiontxt=&productcode=&IVDProducts=&rootCauseText=&recallstatus=&centerclassificationtypetext=&recallnumber=&"
    "postdatefrom=%s&postdateto=%s"
    "&productshortreasontxt=&firmlegalnam=&PMA_510K_Num=&pnumber=&knumber=&PAGENUM=500"
)

RECALL_RECORD_DOWNLOAD_URL = (
    "https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfRes/res.cfm?id=%s"
)

"""
Even though the supply of the CSV recall file has stopped (reasons unknown), we are still making use of the most recent
file in S3 because the Web page we scrape does not have FEI numbers for manufacturers, and we can still get a good deal
of FEI numbers from the most recent CSV file (albeit not a 100% complete list).
"""
DEVICE_RECALL_BUCKET = "s3://openfda-device-recalls/"
DEVICE_RECALL_LOCAL_DIR = config.data_dir("device_recall/s3_sync")
DEVICE_RECALL_FILTERED_DIR = config.data_dir("device_recall/filtered")
DEVICE_CSV_TO_JSON_DIR = config.data_dir("device_recall/csv2json.db")


def batch_dir(batch):
    return (
        arrow.get(batch[0]).strftime("%Y%m%d")
        + "_"
        + arrow.get(batch[1]).strftime("%Y%m%d")
    )


class SyncS3DeviceRecall(luigi.Task):
    bucket = DEVICE_RECALL_BUCKET
    local_dir = DEVICE_RECALL_LOCAL_DIR

    def output(self):
        return luigi.LocalTarget(DEVICE_RECALL_LOCAL_DIR)

    def run(self):
        common.cmd(["mkdir", "-p", self.local_dir])
        common.cmd(
            [
                "aws",
                "--profile=" + config.aws_profile(),
                "s3",
                "sync",
                self.bucket,
                self.local_dir,
            ]
        )


class RemoveDuplicates(luigi.Task):
    def requires(self):
        return SyncS3DeviceRecall()

    def output(self):
        return luigi.LocalTarget(join(DEVICE_RECALL_FILTERED_DIR, "filtered.csv"))

    def run(self):
        common.cmd(["mkdir", "-p", DEVICE_RECALL_FILTERED_DIR])
        unfiltered = pd.read_csv(
            glob.glob(DEVICE_RECALL_LOCAL_DIR + "/*.csv")[0],
            index_col=False,
            encoding="utf-8",
            dtype=np.unicode,
            keep_default_na=False,
        )
        filtered = unfiltered.drop_duplicates()
        filtered.to_csv(
            self.output().path, encoding="utf-8", index=False, quoting=csv.QUOTE_ALL
        )


class CSV2JSON(parallel.MRTask):
    input_dir = DEVICE_RECALL_LOCAL_DIR

    def map(self, key, value, output):

        RENAME_MAP = {
            "rcl products res number": "product_res_number",
            "rcl firm fei number": "firm_fei_number",
            "rcl products submission numbers": "product_submission_numbers",
        }

        def _cleaner(k, v):
            """A helper function that is used for renaming dictionary keys and
            formatting dates.
            """

            def is_other(data):
                if (
                    not common.get_p_number(data)
                    and not common.get_k_number(data)
                    and not data.startswith("G")
                ):
                    return True
                return False

            k = k.lower()
            if k in RENAME_MAP:
                k = RENAME_MAP[k]
            else:
                return None

            if k == "product_submission_numbers":
                # We take this single key, split it on space and then return three new
                # keys and not the original.
                submissions = [s for s in v.split(" ") if s]
                k_numbers = [d for d in submissions if common.get_k_number(d)]
                pma_numbers = [d for d in submissions if common.get_p_number(d)]
                other = " ".join([d.strip() for d in submissions if is_other(d)])
                return [
                    ("k_numbers", k_numbers),
                    ("pma_numbers", pma_numbers),
                    ("other_submission_description", other),
                ]

            if v != None:
                return (k, v.strip())
            return (k, v)

        new_value = common.transform_dict(value, _cleaner)
        output.add(new_value["product_res_number"], new_value)

    def requires(self):
        return RemoveDuplicates()

    def output(self):
        return luigi.LocalTarget(DEVICE_CSV_TO_JSON_DIR)

    def mapreduce_inputs(self):
        input_files = glob.glob(dirname(self.requires().output().path) + "/*.csv")
        return parallel.Collection.from_glob(input_files, parallel.CSVDictLineInput())


class DownloadWeeklyRecallSummary(luigi.Task):
    batch = luigi.TupleParameter()

    def requires(self):
        return []

    def output(self):
        file_name = "%s/recalls.csv" % batch_dir(self.batch)
        return luigi.LocalTarget(os.path.join(DEVICE_RECALL_DATA_DIR, file_name))

    def run(self):
        output_dir = dirname(self.output().path)
        common.shell_cmd("mkdir -p %s", output_dir)

        end = arrow.get(self.batch[1])
        start = arrow.get(self.batch[0])

        id_list = self._fetch_ids(start, end)
        if len(id_list) >= 500:
            # We have tried to get the entire list of weekly results in one shot, but apparently there are more than 500
            # hits and CDRH recall search does not support more than 500 results per search and does not support paging either.
            # This occurrence is rare, but when it does happen we need to re-retrieve the results day by day.
            for r in arrow.Arrow.range("day", start, end):
                id_list = id_list + self._fetch_ids(r, r)

        df = pd.DataFrame(data=list(set(id_list)), columns=["id"])
        df.to_csv(self.output().path, index=False)

    def _fetch_ids(self, start, end):
        id_list = []
        url = SUMMARY_DOWNLOAD_URL % (
            quote(start.format("MM/DD/YYYY"), ""),
            quote(end.format("MM/DD/YYYY"), ""),
        )
        soup = soup_with_retry(url)
        regex = re.compile(r"\./res\.cfm\?id=(\d+)")
        for a in soup.find_all(href=regex):
            id_list.append(int(regex.fullmatch(a["href"]).group(1)))
        return id_list


class RecallDownloaderAndMapper(parallel.Mapper):
    TERMINATED_RE = re.compile(r"Terminated\s+on\s+(.+)")

    def __init__(self, csv2json_db):
        parallel.Mapper.__init__(self)
        self.csv2json_db = csv2json_db

    def field_val_str(self, soup, field_key):
        for th in soup.find_all("th"):
            if th.text == field_key:
                td = next(x for x in list(th.next_siblings) if x.name == "td")
                if td.a and td.a.sup:
                    td.a.decompose()
                for child in list(td.children):
                    if child.name == "br":
                        child.replace_with("\n")
                return td.text.strip()
        return None

    def field_val_array(self, soup, field_key):
        str_val = self.field_val_str(soup, field_key)
        return str_val.split() if str_val is not None else None

    def field_val_date(self, soup, field_key):
        str_val = self.field_val_str(soup, field_key)
        return self.reformat_date(str_val)

    def reformat_date(self, str_val):
        return (
            arrow.get(str_val, "MMMM DD, YYYY").format("YYYY-MM-DD")
            if str_val is not None
            else None
        )

    def map(self, key, value, output):

        def _cleaner(k, v):
            if v == None:
                return None

            if k == "recall_status":
                fullmatch = self.TERMINATED_RE.fullmatch(v)
                if fullmatch:
                    return [
                        ("recall_status", "Terminated"),
                        (
                            "event_date_terminated",
                            self.reformat_date(fullmatch.group(1)),
                        ),
                    ]

            if k == "recalling_firm":
                return parse_address(v)

            return (k, v)

        def parse_address(addr):
            """
            Attempt to parse the Recalling Firm/Manufacturer piece into firm name and address components using
            usaddress library. If unable to parse, stuff everything back into the 'recalling_firm' element.
            """

            def _concat(addr, part_keys):
                parts = []
                for key in part_keys:
                    if addr.get(key) is not None:
                        parts.append(addr.get(key))
                return " ".join(parts)

            if len(addr.splitlines()) >= 2:
                try:
                    tagged = usaddress.tag(addr.replace("\n", " "))
                    return [
                        x
                        for x in [
                            ("recalling_firm", tagged[0].get("Recipient")),
                            (
                                "address_1",
                                _concat(
                                    tagged[0],
                                    [
                                        "AddressNumberPrefix",
                                        "AddressNumber",
                                        "AddressNumberSuffix",
                                        "StreetNamePreModifier",
                                        "StreetNamePreDirectional",
                                        "StreetNamePreType",
                                        "StreetName",
                                        "StreetNamePostType",
                                        "StreetNamePostDirectional",
                                        "USPSBoxType",
                                        "USPSBoxID",
                                        "USPSBoxGroupType",
                                        "USPSBoxGroupID",
                                    ],
                                ),
                            ),
                            (
                                "address_2",
                                _concat(
                                    tagged[0],
                                    [
                                        "SubaddressType",
                                        "SubaddressIdentifier",
                                        "BuildingName",
                                        "OccupancyType",
                                        "OccupancyIdentifier",
                                        "CornerOf",
                                        "LandmarkName",
                                        "IntersectionSeparator",
                                        "NotAddress",
                                    ],
                                ),
                            ),
                            ("city", tagged[0].get("PlaceName")),
                            ("state", tagged[0].get("StateName")),
                            ("postal_code", tagged[0].get("ZipCode")),
                            ("country", tagged[0].get("CountryName")),
                        ]
                        if x[1] is not None and x[1] != ""
                    ]
                except usaddress.RepeatedLabelError:
                    pass

            return "recalling_firm", addr

        id = value["id"]
        recall = {}
        url = RECALL_RECORD_DOWNLOAD_URL % (id)
        soup = soup_with_retry(url)

        recall["@id"] = id
        recall["cfres_id"] = id
        recall_number = self.field_val_str(soup, "Recall Number")
        recall["product_res_number"] = recall_number
        recall["event_date_initiated"] = self.field_val_date(
            soup, "Date Initiated by Firm"
        )
        recall["event_date_created"] = self.field_val_date(soup, "Create Date")
        recall["event_date_posted"] = self.field_val_date(soup, "Date Posted")
        recall["recall_status"] = self.field_val_str(soup, "Recall Status1")
        recall["res_event_number"] = self.field_val_str(soup, "Recall Event ID")

        product_code = self.field_val_str(soup, "Product Classification")
        if product_code:
            recall["product_code"] = (
                re.search(r"Product Code\s+(\S+)", product_code)
                .group(1)
                .replace("ooo", "N/A")
            )

        recall["k_numbers"] = self.field_val_array(soup, "510(K)Number")
        recall["pma_numbers"] = self.field_val_array(soup, "PMA Number")
        recall["product_description"] = self.field_val_str(soup, "Product")
        recall["code_info"] = self.field_val_str(soup, "Code Information")
        recall["recalling_firm"] = self.field_val_str(
            soup, "Recalling Firm/Manufacturer"
        )
        recall["additional_info_contact"] = self.field_val_str(
            soup, "For Additional Information Contact"
        )
        recall["reason_for_recall"] = self.field_val_str(
            soup, "Manufacturer Reasonfor Recall"
        )
        recall["root_cause_description"] = self.field_val_str(
            soup, "FDA DeterminedCause 2"
        )
        recall["action"] = self.field_val_str(soup, "Action")
        recall["product_quantity"] = self.field_val_str(soup, "Quantity in Commerce")
        recall["distribution_pattern"] = self.field_val_str(soup, "Distribution")

        # Add data from the "old" CSV file.
        csv_record = self.csv2json_db.get(recall_number, None)
        if csv_record is not None:
            if len(csv_record["firm_fei_number"]) > 1:
                recall["firm_fei_number"] = csv_record["firm_fei_number"]
            if len(csv_record["other_submission_description"]) > 0:
                recall["other_submission_description"] = csv_record[
                    "other_submission_description"
                ]
            if recall.get("k_numbers") is None and len(csv_record.get("k_numbers")) > 0:
                recall["k_numbers"] = csv_record["k_numbers"]
            if (
                recall.get("pma_numbers") is None
                and len(csv_record.get("pma_numbers")) > 0
            ):
                recall["pma_numbers"] = csv_record["pma_numbers"]

        xformed_recall = common.transform_dict(recall, _cleaner)
        output.add(key, xformed_recall)


class ProcessWeeklyBatch(luigi.Task):
    batch = luigi.TupleParameter()

    def requires(self):
        return [DownloadWeeklyRecallSummary(self.batch), CSV2JSON()]

    def output(self):
        file_name = "%s/json.db" % batch_dir(self.batch)
        return luigi.LocalTarget(os.path.join(DEVICE_RECALL_DATA_DIR, file_name))

    def run(self):
        csv2json_db = parallel.ShardedDB.open(self.input()[1].path).as_dict()
        parallel.mapreduce(
            parallel.Collection.from_glob(
                self.input()[0].path, parallel.CSVDictLineInput()
            ),
            mapper=RecallDownloaderAndMapper(csv2json_db=csv2json_db),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
            map_workers=10,
            num_shards=10,
        )  # Do not hit fda.gov too hard here.


class DeviceRecallAnnotateMapper(DeviceAnnotateMapper):

    def filter(self, data, lookup=None):
        """This is the standard filter copied from Device Harmonization with a small difference in that
        it does not fail for missing product codes, which is the case for a small number of recall records.
        """
        product_code = data.get("product_code")
        if product_code is not None:
            harmonized = self.harmonized_db.get(product_code, None)
            if harmonized:
                if self.table in harmonized:
                    del harmonized[self.table]
                return harmonized
        return None


class AnnotateWeeklyBatch(luigi.Task):
    batch = luigi.TupleParameter()

    def requires(self):
        return [Harmonized2OpenFDA(), ProcessWeeklyBatch(self.batch)]

    def output(self):
        file_name = "%s/annotate.db" % batch_dir(self.batch)
        return luigi.LocalTarget(os.path.join(DEVICE_RECALL_DATA_DIR, file_name))

    def run(self):
        harmonized_db = parallel.ShardedDB.open(self.input()[0].path).as_dict()

        parallel.mapreduce(
            parallel.Collection.from_sharded(self.input()[1].path),
            mapper=DeviceRecallAnnotateMapper(harmonized_db=harmonized_db),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
        )


class LoadJSON(index_util.LoadJSONBase):
    batch = luigi.TupleParameter()
    last_update_date = luigi.Parameter()
    index_name = "devicerecall"
    mapping_file = "./schemas/device_recall_mapping.json"
    use_checksum = True
    optimize_index = False
    docid_key = "product_res_number"

    def _data(self):
        return AnnotateWeeklyBatch(self.batch)


class RunWeeklyProcess(AlwaysRunTask):
    """Generates a date object that is passed through the pipeline tasks in order
    to generate and load JSON documents for the weekly device recall batches.
    """

    def requires(self):
        run_date = arrow.utcnow()
        start = arrow.get(2002, 12, 8)
        end = arrow.get(run_date.year, run_date.month, run_date.day)
        previous_task = NoopTask()

        for batch in arrow.Arrow.span_range(
            "week", start, end, bounds="[]", exact=True
        ):
            task = LoadJSON(
                batch=(batch[0].format(), batch[1].format()),
                last_update_date=end.format("YYYY-MM-DD"),
                previous_task=previous_task,
            )
            previous_task = task
            yield task

    def _run(self):
        index_util.optimize_index("devicerecall", wait_for_merge=0)


if __name__ == "__main__":
    luigi.run()
