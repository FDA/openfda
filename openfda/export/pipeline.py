import collections
import shutil
from itertools import tee
import glob
import os.path
from os.path import dirname, join, basename
import arrow
import elasticsearch
import luigi
import simplejson as json
from openfda import common, config, index_util, elasticsearch_requests, parallel
from openfda.tasks import AlwaysRunTask
import logging


RUN_DIR = dirname(dirname(dirname(os.path.abspath(__file__))))
BASE_DIR = os.path.abspath(join(RUN_DIR, "./data/export/"))
FILES_DIR = os.path.abspath(join(BASE_DIR, "./files/"))
SCHEMA_DIR = os.path.abspath(join(RUN_DIR, "./schemas/"))


# Export output is spoken in the language of the API, meaning that the directory
# structure needs to follow the API. As such, we need to map the API to the
# index. Please note that some indices serve more than one endpoint.
ENDPOINT_INDEX_MAP = {
    "/animalandveterinary/event": "animalandveterinarydrugevent",
    "/drug/event": "drugevent",
    "/drug/label": "druglabel",
    "/drug/enforcement": "recall",
    "/drug/ndc": "ndc",
    "/drug/drugsfda": "drugsfda",
    "/device/enforcement": "recall",
    "/food/enforcement": "recall",
    "/food/event": "foodevent",
    "/device/event": "deviceevent",
    "/device/classification": "deviceclass",
    "/device/510k": "deviceclearance",
    "/device/pma": "devicepma",
    "/device/recall": "devicerecall",
    "/device/registrationlisting": "devicereglist",
    "/device/udi": "deviceudi",
    "/device/covid19serology": "covid19serology",
    "/other/nsde": "othernsde",
    "/other/substance": "substancedata",
    "/tobacco/problem": "tobaccoproblem",
}

# A data structure that helps generate a distinct dataset from a shared index.
# This structure tells us which key in the index and which value to query by in
# order to get a distinct export for an endpoint.
FILTERED_ENPOINT_MAP = {
    "/drug/enforcement": {"key": "product_type", "term": "drugs"},
    "/device/enforcement": {"key": "product_type", "term": "devices"},
    "/food/enforcement": {"key": "product_type", "term": "food"},
}

# For the larger endpoints, we want to separate the output by quarter, this
# structure tells us the data range, the key to query as well as the chunk size
# to use for a larger payload.
RANGE_ENDPOINT_MAP = {
    # The end_date should be set to the date of the latest data availability
    # If you do not set this correctly, too much data collects in the `All other data` zip files
    # Check here for drug event:
    # https://www.fda.gov/drugs/guidancecomplianceregulatoryinformation/surveillance/adversedrugeffects/ucm082193.htm
    "/drug/event": {
        "date_key": "@timestamp",
        "start_date": "2004-01-01",
        "end_date": "2021-10-01",
    },
    # Check here for device event:
    # https://www.fda.gov/MedicalDevices/DeviceRegulationandGuidance/PostmarketRequirements/ReportingAdverseEvents/ucm127891.htm
    "/device/event": {
        "date_key": "date_received",
        "start_date": "1991-10-01",
        "end_date": "2022-04-01",
    },
    "/animalandveterinary/event": {
        "date_key": "original_receive_date",
        "start_date": "1987-01-01",
        "end_date": "2021-10-01",
    },
}

DEFAULT_CHUNKS = 250000
CUSTOM_CHUNKS = {
    "/drug/label": 20000,
    "/drug/event": 12000,
    "/device/event": 100000,
    "/device/udi": 100000,
}


class EndpointExport(object):
    """Object that holds the data required to do a query based export of an
    endpoint. Also exposes some helper functions to assist in generating
    both date range and term filter based queries.
    """

    def __init__(self, endpoint, chunks=250000, partition="", query=None):
        self.chunks = chunks
        self.endpoint = endpoint
        self.index_name = ENDPOINT_INDEX_MAP[self.endpoint]
        self.partition = partition
        self.query = query
        self.quarter_map = {1: "q1", 4: "q2", 7: "q3", 10: "q4"}

    @staticmethod
    def build_term_filter(key, term):
        return {
            "query": {
                "bool": {"must": {"match_all": {}}, "filter": {"term": {key: term}}}
            }
        }

    @staticmethod
    def build_date_range_query(key, start, end, negate=False):
        query = {
            "query": {
                "bool": {
                    "must": {"match_all": {}},
                    "filter": {"range": {key: {"gte": start, "lt": end}}},
                }
            }
        }

        # If negated, wrap condition with a `not` dictionary
        if negate:
            query["query"]["bool"]["must_not"] = query["query"]["bool"].pop("filter")

        return dict(query)

    def build_quarters(self, start_date, end_date):
        date_range = arrow.Arrow.range("month", start_date, end_date)

        return [d for d in date_range if d.month in self.quarter_map]


def pairwise(iterable):
    """Helper function taken from python docs to turn a list into a list of
    pairwise tuples.

    [s0, s1, s2, s3]  -> [(s0,s1), (s1,s2), (s2, s3)]
    """
    a, b = tee(iterable)
    next(b, None)

    return zip(a, b)


def walk_glob(file_pattern, crawl_dir):
    # The directory layout is variable, depending upon the existence of
    # partitions, so we have to walk everything from this point down.
    results = []

    for dirpath, dirs, filenames in os.walk(crawl_dir):
        for name in filenames:
            if name.startswith(file_pattern):
                results.append(join(dirpath, name))

    return results


def _make_date_range_endpoint_batch(endpoint, params):
    """Helper function to make the export quarters code more readable. This
    function does two things: exports all data that is NOT in between two
    dates (putting it in the all_other partition), and it exports every
    quarter between two dates as a separate partition.

    Creates a list of EndpointExport objects, which get fed into the
    exported.
    """

    batch = []
    base = EndpointExport(endpoint)
    quarter_map = base.quarter_map

    fmt = "YYYY-MM-DD"
    start_date = arrow.get(params["start_date"], fmt)
    end_date = arrow.get(params["end_date"], fmt)
    date_key = params["date_key"]
    chunks = params["chunks"]
    quarters = base.build_quarters(start_date, end_date)

    # First grab everything that is outside the range and put it in an `all_other`
    # partition.
    query = base.build_date_range_query(
        date_key, start_date.format(fmt), end_date.format(fmt), negate=True
    )

    batch.append(
        EndpointExport(endpoint, query=query, chunks=chunks, partition="all_other")
    )

    # Now iterate over each quarter as a range tuple.
    # [(q1, q2), (q2, q3), ...]
    for date_pair in pairwise(quarters):
        start, end = date_pair
        partition = str(start.year) + quarter_map[start.month]
        query = base.build_date_range_query(
            date_key, start.format(fmt), end.format(fmt)
        )
        batch.append(
            EndpointExport(endpoint, query=query, chunks=chunks, partition=partition)
        )

    return batch


def basic_cleaner(k, v):
    """Cleaning function so that the output of Elasticsearch mimics the API.
    That is, remove internal keys like `@field` and `field_exact`.
    """
    ignore = [
        "baseline_510_k_exempt_flag",
        "baseline_510_k_flag",
        "baseline_510_k_number",
        "baseline_brand_name",
        "baseline_catalog_number",
        "baseline_date_ceased_marketing",
        "baseline_date_first_marketed",
        "baseline_device_family",
        "baseline_generic_name",
        "baseline_model_number",
        "baseline_other_id_number",
        "baseline_pma_flag",
        "baseline_pma_number",
        "baseline_preamendment_flag",
        "baseline_shelf_life_contained",
        "baseline_shelf_life_in_months",
        "baseline_transitional_flag",
    ]

    if k in ignore:
        return None

    if k.startswith("@"):
        return None

    if k.endswith("_exact"):
        return None

    return (k, v)


def omit_internal_keys(data):
    """Cleaner function to pass to the dump_index command and is used as a
    json.load(..., object_hook=omit_internal_keys).
    """
    return common.transform_dict(data, basic_cleaner)


class MakeExportBatches(AlwaysRunTask):

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(join(BASE_DIR, "batches"))

    def _run(self):
        shutil.rmtree(self.output().path, ignore_errors=True)
        os.makedirs(self.output().path)
        # Get all of the endpoints served by this index
        # Create an `EndpointExport` object for each endpoint in order to export
        # each endpoint properly.
        #
        # Endpoint exports can be:
        #   date range based (quarterly output)
        #   filter based (index serves many endpoints)
        #   vanilla (endpoint is 1 to 1 with index and it is exported all at once)
        for endpoint, index_name in ENDPOINT_INDEX_MAP.items():
            endpoint_batches = []
            chunks = CUSTOM_CHUNKS.get(endpoint, DEFAULT_CHUNKS)
            if endpoint in RANGE_ENDPOINT_MAP:
                params = RANGE_ENDPOINT_MAP[endpoint]
                params["chunks"] = chunks
                endpoint_batches = _make_date_range_endpoint_batch(endpoint, params)
            elif endpoint in FILTERED_ENPOINT_MAP:
                params = FILTERED_ENPOINT_MAP[endpoint]
                query = EndpointExport.build_term_filter(**params)
                endpoint_batches.append(
                    EndpointExport(endpoint, query=query, chunks=chunks)
                )
            else:
                endpoint_batches.append(EndpointExport(endpoint, chunks=chunks))

            # This is a hack to overcome the shortcoming of the parallel library of
            # only having one mapper process for a tiny, single file input. Since we
            # want to execute these endpoint batches in parallel, we write each task
            # to its own file. It will create a mapper for each file.
            for ep in endpoint_batches:
                partition = ep.partition if ep.partition else "all"

                if "enforcement" in ep.endpoint:
                    partition = ep.endpoint.replace("enforcement", "").replace("/", "")
                elif "label" in ep.endpoint:
                    partition = ep.endpoint.replace("label", "").replace("/", "")

                output_dir = join(self.output().path, index_name)
                common.shell_cmd("mkdir -p %s", output_dir)
                file_name = join(output_dir, partition + ".json")

                with open(file_name, "w") as json_out:
                    json_dict = json.dumps(ep.__dict__)
                    json_out.write(json_dict + "\n")


class ParallelExportMapper(parallel.Mapper):
    def __init__(self, output_dir):
        self.output_dir = output_dir

    def map(self, key, value, output):
        es_client = elasticsearch.Elasticsearch(config.es_host(), timeout=120)
        ep = common.ObjectDict(value)
        schema_file = join(SCHEMA_DIR, ep.index_name + "_schema.json")
        endpoint_dir = join(self.output_dir, ep.endpoint[1:])
        target_dir = join(endpoint_dir, ep.partition)
        common.shell_cmd("mkdir -p %s", target_dir)
        if self.index_changed_since_last_export(es_client, ep.index_name, target_dir):
            index_util.dump_index(
                es_client,
                ep.index_name,
                ep.endpoint,
                target_dir,
                cleaner=omit_internal_keys,
                query=ep.query,
                chunks=ep.chunks,
            )
        # Copy the current JSON schema to the zip location so that it is included
        # in the sync to s3. flock is required to avoid a race condition when copying the schema file.
        common.shell_cmd_quiet(
            "flock --verbose %s cp %s %s", schema_file, schema_file, endpoint_dir
        )

    def index_changed_since_last_export(self, es_client, index_name, target_dir):
        manifest_path = join(target_dir, "manifest.json")
        if os.path.isfile(manifest_path):
            with open(manifest_path, "r") as manifest_file:
                manifest = json.load(manifest_file)
                if manifest.get("index_stamp", ""):
                    stamp = manifest["index_stamp"]
                    if stamp == index_util.get_stamp(es_client, index_name):
                        logging.info(
                            "Index %s has not changed since last export; skipping %s",
                            index_name,
                            target_dir,
                        )
                        return False
        return True


class ParallelExport(AlwaysRunTask):

    def requires(self):
        return MakeExportBatches()

    def output(self):
        return luigi.LocalTarget(FILES_DIR)

    def _run(self):
        files = glob.glob(self.input().path + "/*/*.json")
        parallel.mapreduce(
            parallel.Collection.from_glob(files, parallel.JSONLineInput()),
            mapper=ParallelExportMapper(output_dir=self.output().path),
            reducer=parallel.NullReducer(),
            output_prefix=join(BASE_DIR, "tmp"),
            output_format=parallel.NullOutput(),
            map_workers=12,
        )


class CopyIndexToS3(AlwaysRunTask):
    date_str = luigi.Parameter()
    download_bucket = luigi.Parameter()

    def requires(self):
        return ParallelExport()

    def _run(self):
        sync_path = FILES_DIR
        target_bucket = "s3://%s/%s/" % (self.download_bucket, self.date_str)
        s3_cmd = [
            "aws",
            "--profile",
            config.aws_profile(),
            "s3",
            "sync",
            sync_path,
            target_bucket,
            '--exclude "*"',
            '--include "*.zip"',
            '--include "*schema.json"',
        ]

        common.shell_cmd_quiet(" ".join(s3_cmd))


class CombineManifests(index_util.AlwaysRunTask):
    date_str = luigi.Parameter()
    download_bucket = luigi.Parameter()

    def requires(self):
        return CopyIndexToS3(
            date_str=self.date_str, download_bucket=self.download_bucket
        )

    def output(self):
        target_dir = join(FILES_DIR, "manifest/final_manifest.json")
        return luigi.LocalTarget(target_dir)

    def _run(self):
        crawl_dir = dirname(dirname(self.output().path))
        common.shell_cmd("mkdir -p %s", dirname(self.output().path))

        manifests = walk_glob("manifest.json", crawl_dir)

        records = []
        for file_name in manifests:
            records.append(json.load(open(file_name)))

        # Default data structure that creates the appropriate structure on the
        # first put so that we can blindly use `+=` when appropriate.
        combined = collections.defaultdict(
            lambda: collections.defaultdict(
                lambda: {"export_date": None, "partitions": [], "total_records": 0}
            )
        )

        # Walk over all of the manifests and create a single dictionary
        for row in records:
            row.pop("index_stamp", None)
            for domain, value in row.items():
                for sub, val in value.items():
                    combined[domain][sub]["export_date"] = val.get("export_date", "")
                    combined[domain][sub]["partitions"] += val.get("partitions", [])
                    combined[domain][sub]["total_records"] += val.get(
                        "total_records", 0
                    )

        with open(join(self.output().path), "w") as json_out:
            json.dump(combined, json_out, indent=2)


class LoadDownloadJSON(index_util.LoadJSONBase):
    date_str = luigi.Parameter()
    download_bucket = luigi.Parameter(default="download.open.fda.gov")

    index_name = "openfdadata"
    mapping_file = "./schemas/downloads_mapping.json"
    use_checksum = True
    delete_index = False

    def _data(self):
        if not self.data_source:
            self.data_source = CombineManifests(
                date_str=self.date_str, download_bucket=self.download_bucket
            )
        return self.data_source

    def _run(self):
        json_data = json.load(open(self.input()["data"].path))
        date_str = basename(dirname(dirname(self.input()["data"].path)))
        index_dict = {
            "index": self.index_name,
            "doc_type": self.type_name,
            "id": date_str,
            "body": json_data,
        }

        es_client = elasticsearch.Elasticsearch(config.es_host(), timeout=60)

        # We put the same document into the index twice.
        # Once with a key as its date
        # Once with a key of `current`
        # The API will serve the current key as its default response.
        # How we serve archive records is TBD, but the data is preserved this way.
        es_client.index(**index_dict)
        index_dict["id"] = "current"
        es_client.index(**index_dict)

        elasticsearch_requests.update_process_datetime(
            config.es_client(),
            self.index_name,
            arrow.utcnow().format("YYYY-MM-DD"),
            arrow.utcnow().format("YYYY-MM-DD"),
        )


if __name__ == "__main__":
    luigi.run()
