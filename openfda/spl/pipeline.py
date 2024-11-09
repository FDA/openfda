#!/usr/bin/python

""" A pipeline for loading SPL data into ElasticSearch
"""
import gzip
import glob
import logging
import multiprocessing
import os
import re
import shutil
import sys
from os.path import join, dirname
import traceback
import arrow
import luigi
import simplejson as json
from bs4 import BeautifulSoup
from lxml import etree
from lxml.etree import XMLSyntaxError

from openfda import config, common, index_util, parallel
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.common import ProcessException
from openfda.parallel import NullOutput
from openfda.spl import annotate
import urllib.request

RUN_DIR = dirname(dirname(os.path.abspath(__file__)))
SPL_JS = join(RUN_DIR, "spl/spl_to_json.js")
LOINC = join(RUN_DIR, "spl/data/sections.csv")

SPL_S3_BUCKET = "s3://openfda-data-spl/"
SPL_STAGING_S3_BUCKET = "s3://openfda-data-spl-staging/"
SPL_S3_LOCAL_DIR = config.data_dir("spl/s3_sync")
SPL_INDEX_DIR = config.data_dir("spl/index.db")
SPL_JSON_DIR = config.data_dir("spl/json.db")
SPL_ANNOTATED_DIR = config.data_dir("spl/annotated.db")

DAILY_MED_DIR = config.data_dir("spl/dailymed")
DAILY_MED_DOWNLOADS_DIR = config.data_dir("spl/dailymed/raw")
DAILY_MED_EXTRACT_DIR = config.data_dir("spl/dailymed/extract")
DAILY_MED_FLATTEN_DIR = config.data_dir("spl/dailymed/flatten")
DAILY_MED_DOWNLOADS_PAGE = (
    "https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm"
)


common.shell_cmd_quiet("mkdir -p %s", SPL_S3_LOCAL_DIR)
common.shell_cmd_quiet("mkdir -p %s", DAILY_MED_DIR)


class DownloadDailyMedSPL(luigi.Task):
    local_dir = DAILY_MED_DOWNLOADS_DIR

    def output(self):
        return luigi.LocalTarget(self.local_dir)

    def run(self):
        common.shell_cmd_quiet("mkdir -p %s", self.local_dir)
        soup = BeautifulSoup(
            urllib.request.urlopen(DAILY_MED_DOWNLOADS_PAGE).read(), "lxml"
        )
        for a in soup.find_all(href=re.compile(".*.zip")):
            if "_human_" in a.text:
                try:
                    common.download(
                        a["href"], join(self.local_dir, a["href"].split("/")[-1])
                    )
                except ProcessException as e:
                    logging.error(
                        "Could not download a DailyMed SPL archive: {0}: {1}".format(
                            a["href"], e
                        )
                    )


class ExtractDailyMedSPL(luigi.Task):
    local_dir = DAILY_MED_EXTRACT_DIR

    def requires(self):
        return DownloadDailyMedSPL()

    def output(self):
        return luigi.LocalTarget(self.local_dir)

    def run(self):
        src_dir = self.input().path
        os.system('mkdir -p "%s"' % self.output().path)
        pattern = join(src_dir, "*.zip")
        zip_files = glob.glob(pattern)

        if len(zip_files) == 0:
            logging.warning("Expected to find one or more daily med SPL files")

        extract_dir = self.output().path
        for zip_file in zip_files:
            common.shell_cmd_quiet(
                "unzip -oq -d %(extract_dir)s %(zip_file)s" % locals()
            )


class FlattenDailyMedSPL(parallel.MRTask):
    local_dir = DAILY_MED_FLATTEN_DIR

    def map(self, zip_file, value, output):
        cmd = "zipinfo -1 %(zip_file)s" % locals()
        xml_file_name = None
        zip_contents = common.shell_cmd_quiet(cmd)
        xml_match = re.search(
            "^([0-9a-f-]{36})\.xml$", zip_contents.decode(), re.I | re.M
        )
        if xml_match:
            xml_file_name = xml_match.group()
            spl_dir_name = os.path.join(self.output().path, xml_match.group(1))
            os.system('mkdir -p "%s"' % spl_dir_name)
            common.shell_cmd_quiet(
                "unzip -oq %(zip_file)s -d %(spl_dir_name)s" % locals()
            )
            output.add(xml_file_name, zip_file)

    def requires(self):
        return ExtractDailyMedSPL()

    def output(self):
        return luigi.LocalTarget(self.local_dir)

    def mapreduce_inputs(self):
        return parallel.Collection.from_glob(os.path.join(self.input().path, "*/*.zip"))

    def output_format(self):
        return NullOutput()


class AddInDailyMedSPL(luigi.Task):
    local_dir = DAILY_MED_DIR

    def requires(self):
        return FlattenDailyMedSPL()

    def flag_file(self):
        return os.path.join(self.local_dir, ".last_sync_time")

    def complete(self):
        # Only run daily med once per day.
        if config.disable_downloads():
            return True

        return os.path.exists(self.flag_file()) and (
            arrow.get(os.path.getmtime(self.flag_file())) > arrow.now().floor("day")
        )

    def run(self):
        src = self.input().path
        dest = SPL_S3_LOCAL_DIR
        os.system("cp -nr %(src)s/. %(dest)s/" % locals())

        with open(self.flag_file(), "w") as out_f:
            out_f.write("")


class SyncS3SPL(luigi.Task):
    # bucket = SPL_S3_BUCKET
    staging_bucket = SPL_STAGING_S3_BUCKET
    local_dir = SPL_S3_LOCAL_DIR

    def flag_file(self):
        return os.path.join(self.local_dir, ".last_sync_time")

    def complete(self):
        # Only run S3 sync once per day.
        if config.disable_downloads():
            return True

        return os.path.exists(self.flag_file()) and (
            arrow.get(os.path.getmtime(self.flag_file())) > arrow.now().floor("day")
        )

    def run(self):
        # Copying from the staging bucket to the archival bucket is now being done via S3 replication.
        """
        common.quiet_cmd(['aws',
                    '--cli-read-timeout=3600',
                    '--profile=' + config.aws_profile(),
                    's3', 'sync',
                    self.staging_bucket,
                    self.bucket])
        """
        common.quiet_cmd(
            [
                "aws",
                "--cli-read-timeout=3600",
                "--profile=" + config.aws_profile(),
                "s3",
                "sync",
                self.staging_bucket,
                self.local_dir,
            ]
        )

        with open(self.flag_file(), "w") as out_f:
            out_f.write("")


class DetermineSPLToIndex(parallel.MRTask):
    NS = {"ns": "urn:hl7-org:v3"}

    def requires(self):
        return SyncS3SPL()  # , AddInDailyMedSPL()]

    def output(self):
        return luigi.LocalTarget(SPL_INDEX_DIR)

    def mapreduce_inputs(self):
        return parallel.Collection.from_glob(join(SPL_S3_LOCAL_DIR, "*/*.xml"))

    def map(self, xml_file, value, output):
        if os.path.getsize(xml_file) > 0:

            # Oddly enough, some SPL XML files arrive from FDA gzipped, which requires us to take an additional
            # uncompressing step.
            filetype = common.shell_cmd_quiet("file %(xml_file)s" % locals())
            if (
                "gzip compressed data" in filetype.decode()
                or "DOS/MBR boot sector" in filetype.decode()
            ):
                # logging.warning("SPL XML is gzipped: " + xml_file)
                gz_file = xml_file + ".gz"
                os.rename(xml_file, gz_file)
                with gzip.open(gz_file, "rb") as f_in, open(xml_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            p = etree.XMLParser(huge_tree=True)
            try:
                tree = etree.parse(open(xml_file), parser=p)
                code = next(
                    iter(
                        tree.xpath(
                            "//ns:document/ns:code[@codeSystem='2.16.840.1.113883.6.1']/@displayName",
                            namespaces=self.NS,
                        )
                    ),
                    "",
                )
                if code.lower().find("human") != -1:
                    spl_id = tree.xpath(
                        "//ns:document/ns:id/@root", namespaces=self.NS
                    )[0].lower()
                    spl_set_id = tree.xpath(
                        "//ns:document/ns:setId/@root", namespaces=self.NS
                    )[0].lower()
                    version = tree.xpath(
                        "//ns:document/ns:versionNumber/@value", namespaces=self.NS
                    )[0]
                    output.add(spl_set_id, {"spl_id": spl_id, "version": version})
                elif len(code) == 0:
                    logging.warning("Not a drug label SPL file: " + xml_file)
            except XMLSyntaxError as e:
                logging.warning("Invalid SPL file: " + xml_file)
                logging.warning(e)
            except:
                logging.error("Error processing SPL file: " + xml_file)
                traceback.print_exc()
                raise
        else:
            logging.warning("Zero length SPL file: " + xml_file)

    def reduce(self, key, values, output):
        values.sort(key=lambda spl: int(spl["version"]))
        output.put(key, values[-1])

    def map_workers(self):
        return multiprocessing.cpu_count() * 2


class SPL2JSON(parallel.MRTask):
    spl_path = SPL_S3_LOCAL_DIR

    def map(self, spl_set_id, value, output):
        value = value["spl_id"].strip()
        xml_file = join(self.spl_path, value, value + ".xml")
        if not os.path.exists(xml_file):
            # logging.error('File does not exist, skipping %s', xml_file)
            return
        spl_js = SPL_JS
        loinc = LOINC
        cmd = "node %(spl_js)s %(xml_file)s %(loinc)s" % locals()
        json_str = ""
        try:
            json_str = common.shell_cmd_quiet(cmd)
            json_obj = json.loads(json_str)
            if not json_obj.get("set_id"):
                raise RuntimeError("SPL file has no set_id: %s", xml_file)
            else:
                output.add(xml_file, json_obj)
        except:
            logging.error("Unable to convert SPL XML to JSON: %s", xml_file)
            logging.error("cmd: %s", cmd)
            logging.error("json: %s", json_str)
            logging.error(sys.exc_info()[0])
            raise

    def requires(self):
        return DetermineSPLToIndex()

    def output(self):
        return luigi.LocalTarget(SPL_JSON_DIR)

    def mapreduce_inputs(self):
        return parallel.Collection.from_sharded(self.input().path)


class AnnotateJSON(luigi.Task):

    def requires(self):
        return [SPL2JSON(), CombineHarmonization()]

    def output(self):
        return luigi.LocalTarget(SPL_ANNOTATED_DIR)

    def run(self):
        input_db = self.input()[0].path
        harmonized_file = self.input()[1].path

        parallel.mapreduce(
            parallel.Collection.from_sharded(input_db),
            mapper=annotate.AnnotateMapper(harmonized_file),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
        )


class LoadJSON(index_util.LoadJSONBase):
    index_name = "druglabel"
    mapping_file = "./schemas/spl_mapping.json"
    use_checksum = True
    docid_key = "set_id"
    optimize_index = True

    def _data(self):
        return AnnotateJSON()


if __name__ == "__main__":
    luigi.run()
