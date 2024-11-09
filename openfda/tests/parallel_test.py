import csv
import json
import os
import shutil
import unittest
from os.path import dirname

from openfda import parallel, app


class KeyMapper(parallel.Mapper):
    def map(self, key, value, output):
        output.add(key, key)


class BadMapper(parallel.Mapper):
    def map(self, key, value, output):
        raise Exception("I crashed!")


class CsvMapper(parallel.Mapper):
    def map(self, key, value, output):
        output.add("code", int(value[0]))


class ParallelTest(unittest.TestCase):
    def make_files(self, input_prefix, data, input_format):
        os.system('mkdir -p "%s"' % input_prefix)
        source_files = []
        for i, val in enumerate(data):
            filename = os.path.join(input_prefix, "%d.txt" % i)
            source_files.append(filename)
            with open(filename, "w") as f:
                f.write(val)

        return parallel.Collection(source_files, input_format)

    def run_mr(
        self,
        prefix,
        input_data,
        input_format=parallel.LineInput(),
        mapper=parallel.IdentityMapper(),
        reducer=parallel.IdentityReducer(),
        output_format=parallel.LevelDBOutput(),
        num_shards=5,
    ):
        os.system('rm -rf "%s"' % prefix)
        source = self.make_files(
            os.path.join(prefix, "input"), input_data, input_format
        )
        output_prefix = os.path.join(prefix, "output")

        parallel.mapreduce(
            source,
            mapper=mapper,
            reducer=reducer,
            map_workers=1,
            output_format=output_format,
            output_prefix=output_prefix,
            num_shards=num_shards,
        )

        if isinstance(output_format, parallel.LevelDBOutput):
            return sorted(list(parallel.ShardedDB.open(output_prefix)))

        if isinstance(output_format, parallel.JSONOutput):
            return json.load(open(output_prefix))

        if isinstance(output_format, parallel.JSONLineOutput):
            result = []
            with open(output_prefix, "r") as input_f:
                for line in input_f:
                    result.append(json.loads(line))
            return result

    def test_identity(self):
        results = self.run_mr(
            "/tmp/test-identity/",
            ["hello" for i in range(10)],
            input_format=parallel.FilenameInput(),
        )

        for i in range(10):
            key, value = results[i]
            assert key.decode() == "/tmp/test-identity/input/%d.txt" % i, (
                i,
                results[i],
            )
            assert value == ""

    def test_json_output(self):
        results = self.run_mr(
            "/tmp/test-json-output/",
            ["hello" for i in range(10)],
            input_format=parallel.FilenameInput(),
            output_format=parallel.JSONOutput(indent=2, sort_keys=True),
            num_shards=1,
        )

        for i in range(10):
            key = "/tmp/test-json-output/input/%d.txt" % i
            assert key in results, key
            assert results[key] == "", {"key": key, "value": results[key]}

    def test_jsonline_output(self):
        results = self.run_mr(
            "/tmp/test-jsonline-output/",
            ["hello" for i in range(10)],
            mapper=KeyMapper(),
            input_format=parallel.FilenameInput(),
            output_format=parallel.JSONLineOutput(),
        )

        for i in range(10):
            key = "/tmp/test-jsonline-output/input/%d.txt" % i
            assert key in results

    def test_lines_input(self):
        input_data = []
        for i in range(10):
            input_data.append("\n".join(["test-line"] * 10))

        results = self.run_mr("/tmp/test-lineinput", input_data)

        for i in range(10):
            key, value = results[i]
            assert value == "test-line", (i, results[i])

    def test_sum(self):
        # 10 files with 100 lines each
        results = self.run_mr(
            "/tmp/test-sum",
            ["\n".join([str(i) for i in range(100)]) for i in range(10)],
            reducer=parallel.SumReducer(),
        )

        results = set(
            dict(list(map(lambda res: (res[0].decode(), res[1]), results))).values()
        )
        for i in range(100):
            assert i * 10 in results
            results.remove(i * 10)
        assert len(results) == 0, "Unexpected output: %s" % results

    def test_exception(self):
        with self.assertRaises(parallel.MRException) as ctx:
            self.run_mr(
                "/tmp/test-bad-mapper", ["hello" for i in range(10)], mapper=BadMapper()
            )

    def test_csv_line_split(self):
        dir = "/tmp/openfda-test-csv-line-split"
        level_db_prefix = os.path.join(dir, "leveldb")
        file = "csv_split.csv"

        os.system('rm -rf "%s"' % level_db_prefix)
        os.makedirs(dir, exist_ok=True)
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "data/%s" % file),
            os.path.join(dir, file),
        )

        col = parallel.Collection.from_glob(
            os.path.join(dir, file),
            parallel.CSVSplitLineInput(
                quoting=csv.QUOTE_NONE, delimiter="|", fixed_splits=3
            ),
        )
        splits = list(col)
        assert len(splits) == 3
        # Check every split's start and end. Start must be at the beginning of a line, end must be after carriage return or EOF.
        assert (splits[0].start_pos) == 0
        assert (splits[0].end_pos) == 81
        assert (splits[1].start_pos) == 81
        assert (splits[1].end_pos) == 169
        assert (splits[2].start_pos) == 169
        assert (splits[2].end_pos) == 196

        # Run M/R with these splits and ensure the result is as expected
        parallel.mapreduce(
            col,
            mapper=CsvMapper(),
            reducer=parallel.SumReducer(),
            map_workers=len(splits),
            output_format=parallel.LevelDBOutput(),
            output_prefix=level_db_prefix,
            num_shards=len(splits),
        )

        result = sorted(list(parallel.ShardedDB.open(level_db_prefix)))
        print(result)
        # if we got the sum right we know all the splits have been processed correctly
        assert (result[0][1]) == 401 + 402 + 403 + 404 + 405 + 407 + 408 + 409 + 410


def main(argv):
    unittest.main(argv=argv)


if __name__ == "__main__":
    app.run()
