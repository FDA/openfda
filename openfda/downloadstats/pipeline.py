#!/usr/bin/python

""" Pipeline for parsing all the access log files (S3 and CloudFront) for dataset downloads and calculating totals.
"""

import glob
import gzip
import io
import json
import os
import re
from os.path import join, dirname

import arrow
import luigi
import pandas as pd

from openfda import common, config, index_util, parallel
from openfda.tasks import AlwaysRunTask

RUN_DIR = dirname(os.path.abspath(__file__))

S3_ACCESS_LOGS_BUCKET = "s3://openfda-logs/download/"
S3_ACCESS_LOGS_DIR = config.data_dir("downloadstats/s3_logs_raw")
S3_STATS_DB_DIR = config.data_dir("downloadstats/s3_stats.db")
S3_ACCESS_LOGS_CUTOFF = arrow.get("2017-03-01")

CF_ACCESS_LOGS_BUCKET = "s3://openfda-splash-logs/download-cf-logs/"
CF_ACCESS_LOGS_DIR = config.data_dir("downloadstats/cf_logs_raw")
CF_STATS_DB_DIR = config.data_dir("downloadstats/cf_stats.db")

TOTAL_STATS_DB_DIR = config.data_dir("downloadstats/total_stats.db")

ENDPOINT_INDEX_MAP = {
    "animalandveterinary/event": "animalandveterinarydrugevent",
    "drug/event": "drugevent",
    "drug/label": "druglabel",
    "drug/enforcement": "drugenforcement",
    "drug/ndc": "ndc",
    "drug/drugsfda": "drugsfda",
    "device/enforcement": "deviceenforcement",
    "food/enforcement": "foodenforcement",
    "food/event": "foodevent",
    "device/event": "deviceevent",
    "device/classification": "deviceclass",
    "device/510k": "deviceclearance",
    "device/pma": "devicepma",
    "device/recall": "devicerecall",
    "device/registrationlisting": "devicereglist",
    "device/udi": "deviceudi",
    "device/covid19serology": "covid19serology",
    "other/nsde": "othernsde",
    "other/substance": "othersubstance",
    "tobacco/problem": "tobaccoproblem",
}

BOT_USER_AGENTS = map(
    lambda o: o["pattern"],
    json.loads(open(join(RUN_DIR, "crawler-user-agents.json"), "r").read()),
)


def isBot(ua):
    return next((p for p in BOT_USER_AGENTS if re.search(p, ua)), False) != False


class SyncS3AccessLogs(AlwaysRunTask):

    def _run(self):
        common.cmd(["mkdir", "-p", S3_ACCESS_LOGS_DIR])
        common.cmd(
            [
                "aws",
                "--profile=" + config.aws_profile(),
                "s3",
                "sync",
                S3_ACCESS_LOGS_BUCKET,
                S3_ACCESS_LOGS_DIR,
            ]
        )

        # Cut off S3 access logs at the point when CF logs became available
        for file in glob.glob(join(S3_ACCESS_LOGS_DIR, "*")):
            if arrow.get(os.path.split(file)[1][:10]) > S3_ACCESS_LOGS_CUTOFF:
                os.remove(file)

    def output(self):
        return luigi.LocalTarget(S3_ACCESS_LOGS_DIR)


class SyncCFAccessLogs(AlwaysRunTask):

    def _run(self):
        common.cmd(["mkdir", "-p", CF_ACCESS_LOGS_DIR])
        common.cmd(
            [
                "aws",
                "--profile=" + config.aws_profile(),
                "s3",
                "sync",
                CF_ACCESS_LOGS_BUCKET,
                CF_ACCESS_LOGS_DIR,
            ]
        )

    def output(self):
        return luigi.LocalTarget(CF_ACCESS_LOGS_DIR)


class CFAccessLogsStats(parallel.MRTask):
    agg_stats = {}

    def requires(self):
        return SyncCFAccessLogs()

    def output(self):
        return luigi.LocalTarget(CF_STATS_DB_DIR)

    def mapreduce_inputs(self):
        return parallel.Collection.from_glob(join(self.input().path, "*"))

    def map(self, log_file, value, output):
        stats = {}
        with gzip.open(log_file, "rt", encoding="utf-8") as file:
            df = pd.read_csv(
                io.StringIO(file.read()),
                sep="\t",
                skiprows=(0, 1),
                names=[
                    "date",
                    "time",
                    "edge",
                    "bytes",
                    "ip",
                    "method",
                    "host",
                    "uri",
                    "status",
                    "referer",
                    "ua",
                ],
                usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                low_memory=False,
                na_values=[],
                keep_default_na=False,
                index_col=False,
                engine="c",
                memory_map=False,
            )
            for row in df.itertuples():
                if (
                    row.method == "GET"
                    and (200 <= int(row.status) <= 299)
                    and not isBot(row.ua)
                ):
                    for path, endpoint in ENDPOINT_INDEX_MAP.items():
                        if row.uri.startswith("/" + path):
                            if endpoint in stats:
                                stats[endpoint] = stats[endpoint] + 1
                            else:
                                stats[endpoint] = 1

        if len(stats) > 0:
            output.add("stats", stats)

    def reduce(self, key, values, output):
        assert key == "stats"
        for value in values:
            for endpoint, count in value.items():
                if endpoint in self.agg_stats:
                    self.agg_stats[endpoint] = self.agg_stats[endpoint] + count
                else:
                    self.agg_stats[endpoint] = count
        output.put(key, self.agg_stats)


class S3AccessLogsStats(parallel.MRTask):
    agg_stats = {}

    def requires(self):
        return SyncS3AccessLogs()

    def output(self):
        return luigi.LocalTarget(S3_STATS_DB_DIR)

    def mapreduce_inputs(self):
        return parallel.Collection.from_glob(join(self.input().path, "*"))

    def map(self, log_file, value, output):
        stats = {}
        df = pd.read_csv(
            log_file,
            sep=" ",
            names=[
                "Owner",
                "Bucket",
                "Time",
                "Tz",
                "IP",
                "Requester",
                "RequestID",
                "Operation",
                "Key",
                "URI",
                "Status",
                "ErrorCode",
                "BytesSent",
                "ObjectSize",
                "TotalTime",
                "TurnAroundTime",
                "Referrer",
                "UserAgent",
                "VersionId",
                "HostId",
            ],
            low_memory=False,
            na_values=[],
            keep_default_na=False,
            index_col=False,
            engine="c",
            memory_map=True,
        )
        for row in df.itertuples():
            if (
                row.Operation == "REST.GET.OBJECT"
                and isinstance(row.Status, int)
                and (200 <= row.Status <= 299)
                and row.ErrorCode == "-"
                and not isBot(row.UserAgent)
            ):
                for path, endpoint in ENDPOINT_INDEX_MAP.items():
                    if row.Key.startswith(path):
                        if endpoint in stats:
                            stats[endpoint] = stats[endpoint] + 1
                        else:
                            stats[endpoint] = 1
        if len(stats) > 0:
            output.add("stats", stats)

    def reduce(self, key, values, output):
        assert key == "stats"
        for value in values:
            for endpoint, count in value.items():
                if endpoint in self.agg_stats:
                    self.agg_stats[endpoint] = self.agg_stats[endpoint] + count
                else:
                    self.agg_stats[endpoint] = count
        output.put(key, self.agg_stats)


class TotalStats(parallel.MRTask):
    agg_stats = {}

    def requires(self):
        return [S3AccessLogsStats(), CFAccessLogsStats()]

    def output(self):
        return luigi.LocalTarget(TOTAL_STATS_DB_DIR)

    def mapreduce_inputs(self):
        return parallel.Collection.from_sharded_list(
            [path.path for path in self.input()]
        )

    def reduce(self, key, values, output):
        assert key == "stats"
        for value in values:
            for endpoint, count in value.items():
                if endpoint in self.agg_stats:
                    self.agg_stats[endpoint] = self.agg_stats[endpoint] + count
                else:
                    self.agg_stats[endpoint] = count
        output.put(key, self.agg_stats)


class LoadJSON(index_util.LoadJSONBase):
    index_name = "downloadstats"
    mapping_file = "./schemas/downloadstats_mapping.json"
    data_source = TotalStats()
    use_checksum = False
    optimize_index = True


if __name__ == "__main__":
    luigi.run()
