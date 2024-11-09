"""
There are a number of configuration options that are common across all of
the OpenFDA pipelines.

Previously, these were defined individually in each pipeline, which made it
annoying to specify defaults and easy to make errors.

These are now grouped under the FDAConfig 'psuedo-task' and shared across all
pipelines.  The settings can be adjusted using command-line flags or the luigi
config file.

"""

import luigi
from luigi import Parameter

import os.path


class FDAConfig(luigi.WrapperTask):
    data_dir = Parameter(default="./data")
    tmp_dir = Parameter(default="./data/openfda-tmp")
    es_host = Parameter(default="localhost:9200")
    aws_profile = luigi.Parameter(default="openfda")
    disable_downloads = luigi.BoolParameter(default=False)

    snapshot_path = luigi.Parameter(default="elasticsearch-snapshots/opensearch")
    snapshot_bucket = luigi.Parameter(default="openfda-prod")
    role_arn = luigi.Parameter(
        default="arn:aws:iam::806972138196:role/OpenSearchS3Access"
    )


class Context(object):
    def __init__(self, path=""):
        self._path = path

    def path(self, path=None):
        return os.path.join(FDAConfig().data_dir, self._path, path)


def snapshot_path():
    return FDAConfig().snapshot_path


def snapshot_bucket():
    return FDAConfig().snapshot_bucket


def es_host():
    return FDAConfig().es_host


def role_arn():
    return FDAConfig().role_arn


def disable_downloads():
    return FDAConfig().disable_downloads


def es_client(host=None, port=9200):
    import elasticsearch

    if host is None:
        host = es_host()

    return elasticsearch.Elasticsearch(host, timeout=180, port=port)


def data_dir(*subdirs):
    return os.path.join(FDAConfig().data_dir, *subdirs)


def tmp_dir(*subdirs):
    return os.path.join(FDAConfig().tmp_dir, *subdirs)


def aws_profile():
    return FDAConfig().aws_profile
