#!/bin/bash

./_python-env/bin/python openfda/annotation_table/pipeline.py --local-scheduler CombineHarmonization 2>&1 |tee ./logs/annotation.log
