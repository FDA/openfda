#!/usr/local/bin/python

"""
Pipeline for converting CSV RES data in JSON and importing into Elasticsearch.

The data is available since 20 June 2012. The data actually goes back much
longer than that, but it is in an unparseable form or at least would require a
handful of parsers. This date represents the centers switch to a structured
data format.
"""
import csv
import glob
import hashlib
import logging
import os
import sys
from os.path import join, dirname, basename

import arrow
import luigi
import pandas as pd
import simplejson as json

from openfda import common, config, parallel, index_util
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.common import download_to_file_with_retry
from openfda.res import annotate, extract
from openfda.tasks import AlwaysRunTask, NoopTask

# Exceed default field_size limit, need to set to sys.maxsize
csv.field_size_limit(sys.maxsize)

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))

DATE_KEYS = [
    "recall-initiation-date",
    "center-classification-date",
    "termination-date",
    "report-date",
]

# Fields to be used to derive a Document ID for Elasticsearch
ID_FIELDS = ["product-type", "recall-number"]

# Enforcement data changed from XML to CSV the week of 2016-03-10
# Keeping names in line with the former XML style so that all of the pipeline
# logic can remain in place. All `-` elements get converted to `_` in the end.
RENAME_MAP = {
    "Product Type": "product-type",
    "Event ID": "event-id",
    "Status": "status",
    "Recalling Firm": "recalling-firm",
    "Address1": "address-1",
    "Address2": "address-2",
    "City": "city",
    "State/Province": "state",
    "Postal Code": "postal-code",
    "Country": "country",
    "Voluntary/Mandated": "voluntary-mandated",
    "Initial Firm Notification of Consignee or Public": "initial-firm-notification",
    "Distribution Pattern": "distribution-pattern",
    "Recall Number": "recall-number",
    "Classification": "classification",
    "Product Description": "product-description",
    "Product Quantity": "product-quantity",
    "Reason for Recall": "reason-for-recall",
    "Recall Initiation Date": "recall-initiation-date",
    "Center Classification Date": "center-classification-date",
    "Termination Date": "termination-date",
    "Report Date": "report-date",
    "Code Info": "code-info",
    "More Code Info": "more-code-info",
}

DOWNLOAD_URL = (
    "https://www.accessdata.fda.gov/scripts/ires/index.cfm?action="
    "export.getWeeklyExportCSVResult&"
    "monthVal=--M--&dayVal=--D--&yearVal=--Y--"
)


class DownloadCSVReports(luigi.Task):
    batch = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        file_name = "res/batches/%s/enforcement.csv" % arrow.get(self.batch).strftime(
            "%Y%m%d"
        )
        return luigi.LocalTarget(config.data_dir(file_name))

    def run(self):
        output_dir = dirname(self.output().path)
        common.shell_cmd("mkdir -p %s", output_dir)
        dt = arrow.get(self.batch)
        url = DOWNLOAD_URL.replace("--Y--", str(dt.year))
        url = url.replace("--M--", str(dt.month))
        url = url.replace("--D--", str(dt.day))
        download_to_file_with_retry(url, self.output().path)


class CleanCSV(luigi.Task):
    batch = luigi.Parameter()

    def requires(self):
        return DownloadCSVReports(self.batch)

    def output(self):
        file_name = basename(self.input().path)
        dir_name = dirname(self.input().path)
        file_name = "clean-" + file_name
        return luigi.LocalTarget(join(dir_name, file_name))

    def run(self):
        cmd = "iconv -f %s -t %s -c %s > %s" % (
            "ISO-8859-1//TRANSLIT",
            "UTF-8",
            self.input().path,
            self.output().path,
        )
        common.shell_cmd_quiet(cmd)

        # CSV exported by FDA iRes is often malformed because it can contain multiple columns
        # with the same name: More Code Info. Most likely iRes does this when the code information is
        # deemed too large to fit into a single column; but in any case the columns should have been named
        # distinctly, e.g. "More Code Info 01", "More Code Info 02" etc.
        # We handle this case here with Pandas and give the columns distinct names.
        df = pd.read_csv(
            self.output().path, index_col=False, encoding="utf-8", dtype=str
        )
        code_info_columns = list(
            filter(lambda col: col.startswith("More Code Info"), list(df.columns))
        )
        if len(code_info_columns) > 1:
            df["Code Info All"] = df[code_info_columns].apply(
                lambda row: " ".join(
                    list(filter(lambda v: not pd.isna(v), list(row.values)))
                ).strip(),
                axis=1,
            )
            df.drop(code_info_columns, axis=1, inplace=True)
            df.rename(columns={"Code Info All": "More Code Info"}, inplace=True)
            df.to_csv(
                self.output().path, encoding="utf-8", index=False, quoting=csv.QUOTE_ALL
            )


class CSV2JSONMapper(parallel.Mapper):
    """Mapper for the CSV2JSON map-reduction. There is some special logic in here
    to generate a hash() from the top level key/value pairs for the id in
    Elasticsearch.
    Also, upc and ndc are extracted and added to reports that are of drug
    type.
    """

    def _generate_doc_id(self, res_report):
        """Hash function used to create unique IDs for the reports"""
        key_fields = {}
        for field in ID_FIELDS:
            key_fields[field] = res_report.get(field)
        json_str = json.dumps(key_fields, sort_keys=True)
        hasher = hashlib.sha256()
        hasher.update(json_str.encode("utf-8"))
        return hasher.hexdigest()

    def map(self, key, value, output):
        def cleaner(k, v):
            if k not in RENAME_MAP:
                return None

            k = RENAME_MAP[k]

            if k in DATE_KEYS:
                if not v:
                    return None
                v = arrow.get(v, "MM/DD/YYYY").format("YYYYMMDD")

            if isinstance(v, str):
                v = v.strip()

            if v is None:
                v = ""

            return (k, v)

        val = common.transform_dict(value, cleaner)

        # These keys must exist in the JSON records for the annotation logic to work
        logic_keys = ["code-info", "report-date", "product-description"]

        if val.get("product-type") == "Drugs":
            val["upc"] = extract.extract_upc_from_recall(val)
            val["ndc"] = extract.extract_ndc_from_recall(val)

        # There is not a decent ID for the report, so we need to make one
        doc_id = self._generate_doc_id(val)
        val["@id"] = doc_id
        val["@version"] = 1

        # Only write out vals that have required keys and a meaningful date
        if set(logic_keys).issubset(val) and val["report-date"] is not None:
            output.add(doc_id, val)
        else:
            logging.warning(
                "Document is missing required fields. %s",
                json.dumps(val, indent=2, sort_keys=True),
            )


class CSV2JSON(luigi.Task):
    batch = luigi.Parameter()

    def requires(self):
        return CleanCSV(self.batch)

    def output(self):
        dir_name = dirname(self.input().path)
        return luigi.LocalTarget(join(dir_name, "json.db"))

    def run(self):
        input_dir = dirname(self.input().path)
        for csv_filename in glob.glob("%(input_dir)s/clean-*.csv" % locals()):
            parallel.mapreduce(
                parallel.Collection.from_glob(
                    csv_filename, parallel.CSVDictLineInput()
                ),
                mapper=CSV2JSONMapper(),
                reducer=parallel.IdentityReducer(),
                output_prefix=self.output().path,
                num_shards=1,
                map_workers=8,
            )


class AnnotateJSON(luigi.Task):
    batch = luigi.Parameter()

    def requires(self):
        return [CSV2JSON(self.batch), CombineHarmonization()]

    def output(self):
        output_dir = self.input()[0].path.replace("json.db", "annotated.db")
        return luigi.LocalTarget(output_dir)

    def run(self):
        input_db = self.input()[0].path
        harmonized_file = self.input()[1].path

        parallel.mapreduce(
            parallel.Collection.from_sharded(input_db),
            mapper=annotate.AnnotateMapper(harmonized_file),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
            num_shards=1,
            map_workers=1,
        )


class LoadJSON(index_util.LoadJSONBase):
    batch = luigi.Parameter()
    last_update_date = luigi.Parameter()
    index_name = "recall"
    mapping_file = "./schemas/res_mapping.json"
    use_checksum = True
    optimize_index = False
    docid_key = "@id"

    def _data(self):
        return AnnotateJSON(self.batch)


class RunWeeklyProcess(AlwaysRunTask):
    """Generates a date object that is passed through the pipeline tasks in order
    to generate and load JSON documents for the weekly enforcement reports.
    """

    def requires(self):
        run_date = arrow.utcnow()
        dow = run_date.isoweekday()

        # Always run for the most recent wednesday (iso 3) that has passed
        # Reminder: iso day of weeks are Monday (1) through Sunday (7)
        end_delta = 3 - dow if dow >= 3 else (-1 * (4 + dow))
        end_date = run_date.shift(days=end_delta)

        # arrow.Arrow.range() likes the dates in particular datetime format
        start = arrow.get(2012, 0o6, 20)
        end = arrow.get(end_date.year, end_date.month, end_date.day)
        previous_task = NoopTask()

        for batch in arrow.Arrow.range("week", start, end):
            task = LoadJSON(
                batch=batch.format(),
                last_update_date=end.format("YYYY-MM-DD"),
                previous_task=previous_task,
            )
            previous_task = task
            yield task

    def _run(self):
        index_util.optimize_index("recall", wait_for_merge=0)


if __name__ == "__main__":
    luigi.run()
