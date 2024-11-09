import csv
import datetime
import glob
import logging
import os
import re
from urllib.parse import urljoin
from urllib.request import urlopen
from os.path import join, dirname

import luigi
from bs4 import BeautifulSoup

from openfda import common, config, index_util, parallel
from openfda.common import convert_unicode, newest_file_timestamp

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
BASE_DIR = config.data_dir("caers")
DOWNLOAD_DIR = config.data_dir("caers/raw")
common.shell_cmd("mkdir -p %s", BASE_DIR)

CAERS_DOWNLOAD_PAGE_URL = "https://www.fda.gov/food/compliance-enforcement-food/cfsan-adverse-event-reporting-system-caers"

RENAME_MAP = {
    "report id": "report_number",
    "caers created date": "date_created",
    "date of event": "date_started",
    "product type": "role",
    "product": "name_brand",
    "product code": "industry_code",
    "description": "industry_name",
    "patient age": "age",
    "age units": "age_unit",
    "sex": "gender",
    "outcomes": "outcomes",
    "medra preferred terms": "reactions",
    "meddra preferred terms": "reactions",
}

# Lists of keys used by the cleaner function in the CSV2JSONMapper() and the
# _transform() in CSV2JSONReducer()
CONSUMER = ["age", "age_unit", "gender"]
PRODUCTS = ["role", "name_brand", "industry_code", "industry_name"]
DATES = ["date_created", "date_started"]


class DownloadCAERS(luigi.Task):
    local_dir = DOWNLOAD_DIR

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(self.local_dir)

    def run(self):
        common.shell_cmd("mkdir -p %s", self.local_dir)
        soup = BeautifulSoup(urlopen(CAERS_DOWNLOAD_PAGE_URL).read(), "lxml")
        for a in soup.find_all(title=re.compile("CAERS ASCII.*")):
            if "Download CAERS ASCII" in re.sub(r"\s", " ", a.text):
                fileURL = urljoin("https://www.fda.gov", a["href"])
                common.download(
                    fileURL, join(self.output().path, a.attrs["title"] + ".csv")
                )


class CSV2JSONMapper(parallel.Mapper):
    @staticmethod
    def cleaner(k, v):
        """Callback function passed into transform_dict. Takes a key/value tuple
        and either passes them through, does a transformation either or drops
        both (by returning None).

        In this case: renaming all fields, returning None on empty keys to
          avoid blowing up downstream transforms, formatting dates.
        """
        if k.lower() in RENAME_MAP:
            k = RENAME_MAP[k.lower()]

        if v is None:
            return (k, None)

        if isinstance(v, str):
            v = convert_unicode(v).strip()

        if k in DATES:
            if v:
                try:
                    v = datetime.datetime.strptime(v, "%m/%d/%Y").strftime("%Y%m%d")
                except ValueError:
                    logging.warning("Unparseable date: " + v)
            else:
                return None

        return (k, v)

    def map(self, key, value, output):
        new_value = common.transform_dict(value, self.cleaner)
        new_key = new_value["report_number"]

        output.add(new_key, new_value)


class CSV2JSONReducer(parallel.Reducer):
    def merge_two_dicts(self, x, y):
        z = x.copy()  # start with x's keys and values
        z.update(y)  # modifies z with y's keys and values & returns None
        return z

    def _transform(self, value):
        """Takes several rows for the same report_number and merges them into
        a single report object, which is the final JSON representation,
        barring any annotation steps that may follow.
        """
        result = {
            "report_number": None,
            "date_created": None,
            "date_started": None,
            "consumer": {},
            "products": [],
            "reactions": {},  # reactions and outcomes get pivoted to arrays of keys
            "outcomes": {},
        }

        track_consumer = []
        for row in value:
            product = {k: v for k, v in row.items() if k in PRODUCTS}
            consumer = {k: v for k, v in row.items() if k in CONSUMER and v}
            reactions = row.get("reactions", "").split(",")
            outcomes = row.get("outcomes", "").split(",")

            # Setting the results
            result["report_number"] = row["report_number"]
            result["date_created"] = row["date_created"]
            if "date_started" in row:
                result["date_started"] = row["date_started"]

            result["products"].append(product)
            result["consumer"] = self.merge_two_dicts(
                result.get("consumer", {}), consumer
            )

            # Eliminating duplicate reactions
            for reaction in reactions:
                reaction = reaction.strip()
                if reaction:
                    result["reactions"][reaction] = True

            # Eliminating duplicate outcomes
            for outcome in outcomes:
                outcome = outcome.strip()
                if outcome:
                    result["outcomes"][outcome] = True

        # Now that each list is unique, revert to list of strings
        result["reactions"] = list(result["reactions"].keys())
        result["outcomes"] = list(result["outcomes"].keys())
        return result

    def reduce(self, key, values, output):
        output.put(key, self._transform(values))


class CSV2JSON(luigi.Task):
    def requires(self):
        return DownloadCAERS()

    def output(self):
        return luigi.LocalTarget(join(BASE_DIR, "json.db"))

    def run(self):
        parallel.mapreduce(
            parallel.Collection.from_glob(
                glob.glob(join(self.input().path, "*.csv")),
                parallel.CSVDictLineInput(
                    delimiter=",", quoting=csv.QUOTE_MINIMAL, strip_str="\ufeff"
                ),
            ),
            mapper=CSV2JSONMapper(),
            reducer=CSV2JSONReducer(),
            output_prefix=self.output().path,
        )


class LoadJSON(index_util.LoadJSONBase):
    index_name = "foodevent"
    mapping_file = "./schemas/foodevent_mapping.json"
    data_source = CSV2JSON()
    use_checksum = False
    optimize_index = True
    last_update_date = lambda _: newest_file_timestamp(DOWNLOAD_DIR)


if __name__ == "__main__":
    luigi.run()
