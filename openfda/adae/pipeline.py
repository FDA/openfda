#!/usr/bin/python

""" Animal Drug Adverse Event pipeline.
"""

import logging
import os
import re
import sys
import traceback

import arrow
import luigi
import lxml
from lxml import etree

from openfda import common, config, index_util, parallel
from openfda.adae import annotate
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.common import newest_file_timestamp
from openfda.parallel import NullOutput

ADAE_BUCKET = 's3://openfda-data-adae'
ADAE_LOCAL_DIR = config.data_dir('adae/s3_sync')
BATCH = arrow.utcnow().floor('day').format('YYYYMMDD')
AWS_CLI = 'aws'

# TODO: move to an external file once the list grows unmanageable.
NULLIFIED = ['US-FDACVM-2018-US-045311.xml', 'US-FDACVM-2018-US-048571.xml', 'US-FDACVM-2018-US-046672.xml',
             'US-FDACVM-2017-US-042492.xml', 'US-FDACVM-2018-US-044065.xml', 'US-FDACVM-2017-US-070108.xml',
             'US-FDACVM-2017-US-002864.xml', 'CN-FDACVM-2023-CN-000001.xml',
             'US-FDACVM-2017-US-002866.xml', 'US-FDACVM-2017-US-052458.xml', 'US-FDACVM-2017-US-055193.xml',
             'US-FDACVM-2017-US-043931.xml', 'US-FDACVM-2018-US-002321.xml',
             'US-FDACVM-2018-US-063536.xml', 'US-FDACVM-2015-US-221044.xml', 'US-FDACVM2019-US-016263.xml',
             'US-FDACVM-2016-US-062923.xml', 'US-FDACVM-2017-US-001483.xml', 'US-FDACVM-2017-US-009155.xml',
             'US-FDACVM-2017-US-028125.xml', 'US-FDACVM-2017-US-033030.xml',
             'US-FDACVM-2019-US-007584.xml', 'US-FDACVM-2019-US-000655.xml', 'US-FDACVM-2019-US-009469.xml',
             'US-FDACVM-2019-US-021451.xml', 'US-FDACVM-2012-US-036039.xml',
             'US-FDACVM-2019-US-021383.xml', 'US-FDACVM-2019-US-036949.xml', 'US-FDACVM-2019-US-056874.xml',
             'US-FDACVM-2020-US-000506.xml', 'US-FDACVM-2020-US-012144.xml',
             'US-FDACVM-2019-US-008020.xml', 'US-FDACVM-2020-US-055134.xml', 'US-FDACVM-2020-US-042720.xml',
             'US-FDACVM-2020-US-045814.xml', 'US-FDACVM-2020-US-045816.xml',
             'US-FDACVM-2020-US-045820.xml', 'US-FDACVM-2020-US-045824.xml', 'US-FDACVM-2016-US-054799.xml',
             'US-FDACVM-2017-US-060740.xml', 'CA-FDACVM-2024-CA-000079.xml',
             'US-FDACVM-2016-US-062923.xml', 'US-FDACVM-2017-US-001483.xml', 'US-FDACVM-2017-US-009155.xml',
             'US-FDACVM-2017-US-028125.xml', 'US-FDACVM-2017-US-033030.xml', 'US-FDACVM-2012-US-031040.xml',
             'US-FDACVM-2012-US-031016.xml',
             'US-FDACVM-2014-US-011762.xml',
             'US-FDACVM-2012-US-045510.xml',
             'US-FDACVM-2012-US-031080.xml',
             'US-FDACVM-2012-US-030987.xml',
             'US-FDACVM-2012-US-031012.xml',
             'US-FDACVM-2018-US-026106.xml',
             'US-FDACVM-2019-US-063566.xml',
             'US-FDACVM-2020-US-035313.xml',
             'US-FDACVM-2021-US-024914.xml',
             'US-FDACVM-2017-US-003772.xml', 'US-FDACVM-2019-GB-000037.xml',
             'US-FDACVM-2018-US-066176.xml',
             'US-FDACVM-2020-US-037305.xml',
             'US-FDACVM-2021-US-059599.xml',
             'US-FDACVM-2020-US-041760.xml', 'US-FDACVM-2021-US-058330.xml',
             'US-FDACVM-2022-US-017391.xml', 'US-FDACVM-2021-US-056187.xml', 'US-FDACVM-2021-US-056191.xml',
             'US-FDACVM-2021-US-056192.xml', 'US-FDACVM-2019-US-061743.xml', 'US-FDACVM-2021-US-010181.xml',
             'US-FDACVM-2022-US-009418.xml', 'US-FDACVM-2020-US-031776.xml', 'US-FDACVM-2020-US-040198.xml',
             'US-FDACVM-2022-US-002771.xml', 'US-FDACVM-2022-US-003805.xml', 'US-FDACVM-2021-US-048336.xml',
             'US-FDACVM-2021-US-016039.xml', 'US-FDACVM-2022-US-020622.xml', 'US-FDACVM-2022-US-019999.xml',
             'US-FDACVM-2022-US-020273.xml', 'US-FDACVM-2022-US-020000.xml', 'US-FDACVM-2022-US-019240.xml',
             'US-FDACVM-2022-US-020621.xml', 'US-FDACVM-2021-US-062660.xml', 'US-FDACVM-2021-US-062692.xml',
             'US-FDACVM-2021-US-062641.xml', 'US-FDACVM-2021-US-062630.xml', 'US-FDACVM-2021-US-062664.xml',
             'US-FDACVM-2021-US-062629.xml', 'US-FDACVM-2021-US-062689.xml', 'US-FDACVM-2021-US-062640.xml',
             'US-FDACVM-2021-US-062662.xml', 'US-FDACVM-2021-US-062642.xml', 'US-FDACVM-2021-US-062638.xml',
             'US-FDACVM-2021-US-062628.xml', 'US-FDACVM-2021-US-062639.xml', 'US-FDACVM-2021-US-062691.xml',
             'US-FDACVM-2021-US-062644.xml', 'US-FDACVM-2021-US-062661.xml', 'US-FDACVM-2021-US-062693.xml',
             'US-FDACVM-2021-US-062665.xml', 'US-FDACVM-2021-US-062663.xml', 'US-FDACVM-2021-US-062690.xml',
             'US-FDACVM-2021-US-062694.xml', 'US-FDACVM-2022-US-026545.xml', 'US-FDACVM-2022-US-021891.xml',
             'US-FDACVM-2022-US-020784.xml', 'US-FDACVM-2022-US-022294.xml', 'US-FDACVM-2022-US-024610.xml',
             'US-FDACVM-2022-US-026289.xml', 'US-FDACVM-2022-US-021877.xml', 'US-FDACVM-2022-US-022249.xml',
             'US-FDACVM-2022-US-019946.xml', 'US-FDACVM-2022-US-022031.xml', 'US-FDACVM-2022-US-023591.xml',
             'US-FDACVM-2022-US-025153.xml', 'US-FDACVM-2022-US-026469.xml', 'US-FDACVM-2022-US-022332.xml',
             'US-FDACVM-2022-US-022323.xml', 'US-FDACVM-2022-US-022010.xml', 'US-FDACVM-2022-US-021038.xml',
             'US-FDACVM-2022-US-024835.xml', 'USA-FDACVM-2022-US-022224.xml', 'US-FDACVM-2022-US-025982.xml',
             'US-FDACVM-2022-US-023169.xml', 'US-FDACVM-2022-US-022072.xml', 'US-FDACVM-2022-US-026072.xml',
             'US-FDACVM-2022-US-021889.xml', 'US-FDACVM-2022-US-023725.xml', 'US-FDACVM-2022-US-026159.xml',
             'US-FDACVM-2022-US-019782.xml', 'US-FDACVM-2022-US-020775.xml', 'US-FDACVM-2022-US-020994.xml',
             'US-FDACVM-2022-US-019415.xml', 'US-FDACVM-2022-US-024578.xml', 'US-FDACVM-2022-US-019228.xml',
             'US-FDACVM-2022-US-024497.xml', 'US-FDACVM-2022-US-023461.xml', 'US-FDACVM-2022-US-023177.xml',
             'US-FDACVM-2022-US-023118.xml', 'US-FDACVM-2022-US-020954.xml', 'US-FDACVM-2022-US-023412.xml',
             'US-FDACVM-2022-US-023093.xml', 'US-FDACVM-2022-US-019306.xml', 'US-FDACVM-2022-US-019625.xml',
             'US-FDACVM-2022-US-022341.xml', 'US-FDACVM-2022-US-026467.xml', 'US-FDACVM-2022-US-019831.xml',
             'US-FDA-2022-US-020807.xml', 'US-FDA-2022-US-020759.xml', 'US-FDACVM-2022-US-020885.xml',
             'US-FDACVM-2022-US-019258.xml', 'US-FDACVM-2022-US-019925.xml', 'US-FDACVM-2022-US-019361.xml',
             'US-FDACVM-2022-US-019851.xml', 'US-FDACVM-2022-US-019667.xml', 'US-FDACVM-2022-US-026021.xml',
             'US-FDACVM-2022-US-022123.xml', 'US-FDACVM-2022-US-026147.xml', 'US-FDACVM-2022-US-019251.xml',
             'US-FDACVM-2022-US-026234.xml', 'US-FDACVM-2022-US-022268.xml', 'US-FDACVM-2022-US-021950.xml',
             'US-FDACVM-2022-US-023438.xml', 'US-FDACVM-2022-US-021976.xml', 'US-FDACVM-2022-US-019359.xml',
             'US-FDACVM-2022-US-026532.xml', 'US-FDACVM-2022-US-019753.xml', 'US-FDACVM-2022-US-022348.xml',
             'US-FDACVM-2022-US-019754.xml', 'US-FDACVM-2022-US-023486.xml', 'US-FDACVM-2022-US-023550.xml',
             'US-FDACVM-2022-US-022053.xml', 'US-FDACVM-2022-US-022025.xml', 'US-FDACVM-2022-US-022186.xml',
             'US-FDACVM-2022-US-023163.xml', 'US-FDACVM-2022-US-019917.xml', 'US-FDACVM-2022-US-019474.xml',
             'US-FDACVM-2022-US-019732.xml', 'US-FDACVM-2022-US-019817.xml', 'US-FDACVM-2022-US-019506.xml',
             'US-FDACVM-2022-US-026161.xml', 'US-FDACVM-2022-US-019447.xml', '2022-US-020903.xml',
             'US-FDACVM-2022-US-020022.xml', 'US-FDACVM-2022-US-023570.xml', 'US-FDACVM-2022-US-023710.xml',
             'US-FDACVM-2022-US-019480.xml', 'US-FDACVM-2022-US-022065.xml', 'US-FDACVM-2022-US-023268.xml',
             'US-FDACVM-2022-US-023178.xml', 'US-FDACVM-2022-US-019441.xml', 'US-FDACVM-2022-US-019268.xml',
             'US-FDACVM-2022-US-019684.xml', 'US-FDACVM-2022-US-023359.xml', 'US-FDACVM-2022-US-022127.xml',
             'US-FDACVM-2022-US-019244.xml', 'US-FDACVM-2022-US-019321.xml', 'US-FDACVM-2022-US-024894.xml',
             'US-FDACVM-2022-US-022228.xml', 'US-FDACVM-2022-US-019464.xml', 'US-FDACVM-2022-US-023189.xml',
             'US-FDACVM-2022-US-023660.xml', 'US-FDACVM-2022-US-021895.xml', 'US-FDACVM-2022-US-019808.xml',
             'US-FDACVM-2022-US-020841.xml', 'US-FDACVM-2022-US-022243.xml', 'US-FDACVM-2022-US-019267.xml',
             'US-FDACVM-2022-US-020853.xml', 'US-FDACVM-2022-US-024547.xml', 'US-FDACVM-2022-US-026200.xml',
             'US-FDACVM-2022-US-022129.xml', 'US-FDACVM-2022-US-019837.xml', 'US-FDACVM-2022-US-019803.xml',
             'US-FDACVM-2022-US-021015.xml', 'US-FDACVM-2022-US-024556.xml', 'US-FDACVM-2022-US-020971.xml',
             'US-FDACVM-2022-US-021928.xml', 'US-FDACVM-2022-US-026257.xml', 'US-FDACVM-2022-US-024612.xml',
             'US-FDACVM-2022-US-019668.xml', 'US-FDACVM-2022-US-023352.xml', 'US-FDACVM-2022-US-026255.xml',
             'US-FDACVM-2022-US-022182.xml', 'US-FDACVM-2022-US-019380.xml', 'US-FDACVM-2022-US-021987.xml',
             'US-FDACVM-2022-US-020668.xml', 'US-FDACVM-2022-VU-000001.xml', 'US-FDACVM-2022-US-022058.xml',
             'US-FDACVM-2022-US-020833.xml', 'US-FDACVM-2022-US-019798.xml', 'US-FDACVM-2022-US-022250.xml',
             'US-FDACVM-2022-US-020828.xml', 'US-FDACVM-2022-US-022246.xml', 'US-FDACVM-2022-US-022170.xml',
             'US-FDACVM-2022-US-020980.xml', 'US-FDACVM-2022-US-026460.xml', 'US-FDACVM-2022-US-020908.xml',
             'US-FDACVM-2022-US-019511.xml', 'US-FDACVM-2022-US-026548.xml', 'US-FDACVM-2022-US-022240.xml',
             'US-FDACVM-2022-US-023358.xml', 'US-FDACVM-2022-US-024954.xml', 'US-FDACVM-2022-US-026508.xml',
             'US-FDACVM-2022-US-021898.xml', 'US-FDACVM-2022-US-019487.xml', 'US-FDACVM-2022-US-023220.xml',
             'US-FDACVM-2022-US-019795.xml', 'US-FDACVM-2022-US-019687.xml', 'US-FDACVM-2022-US-023451.xml',
             'US-FDACVM-2022-US-026471.xml', 'US-FDACVM-2022-US-023422.xml', 'US-FDACVM-2022-US-021020.xml',
             'US-FDACVM-2022-US-022122.xml', 'US-FDACVM-2022-US-026521.xml', 'US-FDACVM-2022-US-020691.xml',
             'US-FDACVM-2022-US-023269.xml', 'US-FDACVM-2022-US-022225.xml', 'US-FDACVM-2022-US-023779.xml',
             'US-FDACVM-2022-US-023758.xml', 'US-FDACVM-2022-US-020906.xml', 'US-FDACVM-2022-US-023740.xml',
             'US-FDACVM-2022-US-026254.xml', 'US-FDACVM-2022-US-026500.xml', 'US-FDACVM-2022-US-022101.xml',
             'US-FDACVM-2022-US-019936.xml', 'US-FDACVM-2022-US-020665.xml', 'US-FDACVM-2022-US-019248.xml',
             'US-FDACVM-2022-US-024550.xml', 'US-FDACVM-2022-US-023609.xml', 'US-FDACVM-2022-US-024817.xml',
             'US-FDACVM-2022-US-023704.xml', 'US-FDACVM-2022-US-024795.xml', 'US-FDACVM-2022-US-024907.xml',
             'US-FDACVM-2022-US-023086.xml', 'US-FDACVM-2022-US-023334.xml', 'US-FDACVM-2022-US-023372.xml',
             'US-FDACVM-2022-US-023465.xml', 'US-FDACVM-2022-US-019846.xml', 'US-FDACVM-2022-US-019456.xml',
             'US-FDACVM-2022-US-021913.xml', 'US-FDACVM-2022-US-022213.xml', 'US-FDACVM-2022-US-024886.xml',
             'US-FDACVM-2022-US-023287.xml', 'US-FDACVM-2022-US-023183.xml', 'US-FDACVM-2022-US-019898.xml',
             'US-FDACVM-2022-US-024759.xml', 'US-FDACVM-2022-US-023454.xml', 'US-FDACVM-2022-US-023395.xml',
             'US-FDACVM-2022-US-027373.xml', 'US-FDACVM-2022-US-019468.xml', 'US-FDACVM-2022-US-024685.xml',
             'US-FDACVM-2022-US-024813.xml', 'US-FDACVM-2022-US-019639.xml', 'US-FDACVM-2022-US-019388.xml',
             'US-FDACVM-2022-US-024614.xml', 'US-FDACVM-2022-US-022060.xml', 'US-FDACVM-2022-US-019755.xml',
             'US-FDACVM-2022-US-023464.xml', 'US-FDACVM-2022-US-019922.xml', 'US-FDACVM-2022-US-019593.xml',
             'US-FDACVM-2022-US-019902.xml', 'US-FDACVM-2022-US-019610.xml', 'US-FDACVM-2022-US-024871.xml',
             'US-FDACVM-2022-US-024905.xml', 'US-FDACVM-2022-US-024646.xml', 'US-FDACVM-2022-US-024552.xml',
             'US-FDACVM-2022-US-024681.xml', 'US-FDACVM-2022-US-024656.xml', 'US-FDACVM-2022-US-023307.xml',
             'US-FDACVM-2022-US-025093.xml', 'US-FDACVM-2022-US-019679.xml', 'US-FDACVM-2022-US-024765.xml',
             'US-FDACVM-2022-US-019365.xml', 'US-FDACVM-2022-US-023467.xml', 'US-FDACVM-2022-US-020637.xml',
             'US-FDACVM-2022-US-023614.xml', 'US-FDACVM-2022-US-024735.xml', 'US-FDACVM-2022-US-019743.xml',
             'US-FDACVM-2022-US-022211.xml', 'US-FDACVM-2022-US-019614.xml', 'US-FDACVM-2022-US-020736.xml',
             'US-FDACVM-2022-US-024857.xml', 'US-FDACVM-2022-US-020826.xml', 'US-FDACVM-2022-US-026248.xml',
             'US-FDACVM-2022-US-019814.xml', 'US-FDACVM-2022-US-019764.xml', 'US-FDACVM-2022-US-023399.xml',
             'US-FDACVM-2022-US-022287.xml', 'US-FDACVM-2022-US-019307.xml', 'US-FDACVM-2022-US-019290.xml',
             'US-FDACVM-2022-US-019975.xml', 'US-FDACVM-2022-US-021912.xml', 'US-FDACVM-2022-US-019627.xml',
             'US-FDACVM-2022-US-023292.xml', 'US-FDACVM-2022-US-019461.xml', 'US-FDACVM-2022-US-021023.xml',
             'US-FDACVM-2022-US-020940.xml', 'US-FDACVM-2022-US-024584.xml', 'US-FDACVM-2022-US-026088.xml',
             'US-FDACVM-2022-US-019453.xml', 'US-FDACVM-2022-US-026303.xml', 'US-FDACVM-2022-US-020864.xml',
             'US-FDACVM-2022-US-019963.xml', 'US-FDACVM-2022-US-019313.xml', 'US-FDACVM-2022-US-026115.xml',
             'US-FDACVM-2022-US-019822.xml', 'US-FDACVM-2022-US-024520.xml', 'US-FDACVM-2022-US-020670.xml',
             'US-FDACVM-2022-US-026387.xml', 'US-FDACVM-2022-US-019671.xml', 'US-FDACVM-2022-US-019514.xml',
             'US-FDACVM-2022-US-026000.xml', 'US-FDACVM-2022-US-022328.xml', 'US-FDACVM-2022-US-024617.xml',
             'US-FDACVM-2022-US-024910.xml', 'US-FDACVM-2022-US-024546.xml', 'US-FDACVM-2022-US-019264.xml',
             'US-FDACVM-2022-US-019417.xml', 'US-FDACVM-2022-US-026209.xml', 'US-FDACVM-2022-US-021942.xml',
             'US-FDACVM-2022-US-022325.xml', 'US-FDACVM-2022-US-019352.xml', 'US-FDACVM-2022-US-020989.xml',
             'US-FDACVM-2022-US-024639.xml', 'US-FDACVM-2022-US-022173.xml', 'US-FDACVM-2022-US-019758.xml',
             'US-FDACVM-2022-US-022311.xml', 'US-FDACVM-2022-US-019689.xml', 'US-FDACVM-2022-US-024942.xml',
             'US-FDACVM-2022-US-023605.xml', 'US-FDACVM-2022-US-020640.xml', 'US-FDACVM-2022-US-024616.xml',
             'US-FDACVM-2022-US-019654.xml', 'US-FDACVM-2022-US-023711.xml', 'US-FDACVM-2022-US-023712.xml',
             'US-FDACVM-2022-US-019381.xml', 'US-FDACVM-2022-US-020859.xml', 'US-FDACVM-2022-US-019793.xml',
             'US-FDACVM-2022-US-022236.xml', 'US-FDACVM-2022-US-019294.xml', 'US-FDACVM-2022-US-019459.xml',
             'US-FDACVM-2022-US-019579.xml', 'US-FDACVM-2022-US-019906.xml', 'US-FDACVM-2022-US-026174.xml',
             'US-FDACVM-2022-US-019427.xml', 'US-FDACVM-2022-US-024548.xml', 'US-FDACVM-2022-US-020780.xml',
             'US-FDACVM-2022-US-019393.xml', 'US-FDACVM-2022-US-023291.xml', 'US-FDACVM-2022-US-019578.xml',
             'US-FDACVM-2022-US-024613.xml', 'US-FDACVM-2022-US-024851.xml', 'US-FDACVM-2022-US-026267.xml',
             'US-FDACVM-2022-US-019927.xml', 'US-FDACVM-2022-US-026028.xml', 'US-FDACVM-2022-US-019223.xml',
             'US-FDACVM-2022-US-019312.xml', 'US-FDACVM-2022-US-019938.xml', 'US-FDACVM-2022-US-019246.xml',
             'US-FDACVM-2022-US-019619.xml', 'US-FDACVM-2022-US-021026.xml', 'US-FDACVM-2022-US-019857.xml',
             'US-FDACVM-2022-US-019607.xml', 'US-FDACVM-2022-US-023338.xml', 'US-FDACVM-2018-US-015240.xml',
             'US-FDACVM-2022-US-027146.xml', 'US-FDACVM-2022-US-048307.xml', 'US-FDACVM-2022-US-056558.xml',
             'US-FDACVM-2022-US-060179.xml', 'US-FDACVM-2021-US-058907.xml', 'US-FDACVM-2022-US-007923.xml',
             'US-FDACVM-2022-US-007923.xml', 'US-FDACVM-2022-US-045960.xml', 'US-FDACVM-2022-US-050596.xml',
             'US-FDACVM-2022-US-052615.xml', 'US-FDACVM-2022-US-052990.xml', 'US-FDACVM-2022-US-055744.xml',
             'US-FDACVM-2022-US-055881.xml', 'US-FDACVM-2022-US-057877.xml', 'US-FDACVM-2022-US-058031.xml',
             'US-FDACVM-2022-US-059508.xml', 'US-FDACVM-2022-US-059509.xml', 'US-FDACVM-2022-US-059520.xml',
             'US-FDACVM-2022-US-059521.xml', 'US-FDACVM-2022-US-059523.xml', 'US-FDACVM-2022-US-059524.xml',
             'US-FDACVM-2022-US-059525.xml', 'US-FDACVM-2022-US-059526.xml', 'US-FDACVM-2022-US-059527.xml',
             'US-FDACVM-2022-US-059528.xml', 'US-FDACVM-2022-US-059529.xml', 'US-FDACVM-2022-US-059531.xml',
             'US-FDACVM-2022-US-059532.xml', 'US-FDACVM-2022-US-059533.xml', 'US-FDACVM-2022-US-059534.xml',
             'US-FDACVM-2022-US-059535.xml', 'US-FDACVM-2022-US-059537.xml', 'US-FDACVM-2022-US-059538.xml',
             'US-FDACVM-2022-US-059539.xml', 'US-FDACVM-2022-US-059541.xml', 'US-FDACVM-2022-US-059542.xml',
             'US-FDACVM-2022-US-059543.xml', 'US-FDACVM-2022-US-059544.xml', 'US-FDACVM-2022-US-059545.xml',
             'US-FDACVM-2022-US-059546.xml', 'US-FDACVM-2022-US-059550.xml', 'US-FDACVM-2022-US-059551.xml',
             'US-FDACVM-2022-US-059552.xml', 'US-FDACVM-2022-US-059553.xml', 'US-FDACVM-2022-US-059554.xml',
             'US-FDACVM-2022-US-059555.xml', 'US-FDACVM-2022-US-059556.xml', 'US-FDACVM-2022-US-059557.xml',
             'US-FDACVM-2022-US-059558.xml', 'US-FDACVM-2022-US-059560.xml', 'US-FDACVM-2022-US-059562.xml',
             'US-FDACVM-2022-US-059563.xml', 'US-FDACVM-2022-US-059566.xml', 'US-FDACVM-2022-US-059567.xml',
             'US-FDACVM-2022-US-059568.xml', 'US-FDACVM-2022-US-059569.xml', 'US-FDACVM-2022-US-059570.xml',
             'US-FDACVM-2022-US-059571.xml', 'US-FDACVM-2022-US-059573.xml', 'US-FDACVM-2022-US-059643.xml',
             'US-FDACVM-2022-US-059644.xml', 'US-FDACVM-2022-US-059645.xml', 'US-FDACVM-2022-US-059648.xml',
             'US-FDACVM-2022-US-059649.xml', 'US-FDACVM-2022-US-059650.xml', 'US-FDACVM-2022-US-059652.xml',
             'US-FDACVM-2022-US-059654.xml', 'US-FDACVM-2022-US-059657.xml', 'US-FDACVM-2022-US-059658.xml',
             'US-FDACVM-2022-US-059659.xml', 'US-FDACVM-2022-US-059660.xml', 'US-FDACVM-2022-US-059661.xml',
             'US-FDACVM-2022-US-059662.xml', 'US-FDACVM-2022-US-059663.xml', 'US-FDACVM-2022-US-059665.xml',
             'US-FDACVM-2022-US-059666.xml', 'US-FDACVM-2022-US-059667.xml', 'US-FDACVM-2022-US-059671.xml',
             'US-FDACVM-2022-US-059673.xml', 'US-FDACVM-2022-US-059674.xml', 'US-FDACVM-2022-US-059678.xml',
             'US-FDACVM-2022-US-059679.xml', 'US-FDACVM-2022-US-059680.xml', 'US-FDACVM-2022-US-059681.xml',
             'US-FDACVM-2022-US-059683.xml', 'US-FDACVM-2022-US-059686.xml', 'US-FDACVM-2022-US-059687.xml',
             'US-FDACVM-2022-US-059688.xml', 'US-FDACVM-2022-US-059689.xml', 'US-FDACVM-2022-US-059690.xml',
             'US-FDACVM-2022-US-059691.xml', 'US-FDACVM-2022-US-059693.xml', 'US-FDACVM-2022-US-059695.xml',
             'US-FDACVM-2022-US-059696.xml', 'US-FDACVM-2022-US-059697.xml', 'US-FDACVM-2022-US-059698.xml',
             'US-FDACVM-2022-US-059700.xml', 'US-FDACVM-2022-US-059701.xml', 'US-FDACVM-2022-US-059702.xml',
             'US-FDACVM-2022-US-059736.xml', 'US-FDACVM-2022-US-059737.xml', 'US-FDACVM-2022-US-059738.xml',
             'US-FDACVM-2022-US-059739.xml', 'US-FDACVM-2022-US-059740.xml', 'US-FDACVM-2022-US-059741.xml',
             'US-FDACVM-2022-US-059742.xml', 'US-FDACVM-2022-US-059743.xml', 'US-FDACVM-2022-US-059744.xml',
             'US-FDACVM-2022-US-059745.xml', 'US-FDACVM-2022-US-059746.xml', 'US-FDACVM-2022-US-059747.xml',
             'US-FDACVM-2022-US-059748.xml', 'US-FDACVM-2022-US-059749.xml', 'US-FDACVM-2022-US-059750.xml',
             'US-FDACVM-2022-US-059751.xml', 'US-FDACVM-2022-US-059752.xml', 'US-FDACVM-2022-US-059753.xml',
             'US-FDACVM-2022-US-059755.xml', 'US-FDACVM-2022-US-059756.xml', 'US-FDACVM-2022-US-059757.xml',
             'US-FDACVM-2022-US-059758.xml', 'US-FDACVM-2022-US-059759.xml', 'US-FDACVM-2022-US-059760.xml',
             'US-FDACVM-2022-US-059761.xml', 'US-FDACVM-2022-US-059762.xml', 'US-FDACVM-2022-US-059763.xml',
             'US-FDACVM-2022-US-059764.xml', 'US-FDACVM-2022-US-059766.xml', 'US-FDACVM-2022-US-059767.xml',
             'US-FDACVM-2022-US-059768.xml', 'US-FDACVM-2022-US-059769.xml', 'US-FDACVM-2022-US-059770.xml',
             'US-FDACVM-2022-US-059771.xml', 'US-FDACVM-2022-US-059772.xml', 'US-FDACVM-2022-US-059773.xml',
             'US-FDACVM-2022-US-059774.xml', 'US-FDACVM-2022-US-059775.xml', 'US-FDACVM-2022-US-059776.xml',
             'US-FDACVM-2022-US-059777.xml', 'US-FDACVM-2022-US-059778.xml', 'US-FDACVM-2022-US-059779.xml',
             'US-FDACVM-2022-US-059780.xml', 'US-FDACVM-2022-US-059782.xml', 'US-FDACVM-2022-US-059784.xml',
             'US-FDACVM-2022-US-059786.xml', 'US-FDACVM-2022-US-059788.xml', 'US-FDACVM-2022-US-059789.xml',
             'US-FDACVM-2022-US-059790.xml', 'US-FDACVM-2022-US-059791.xml', 'US-FDACVM-2022-US-059792.xml',
             'US-FDACVM-2022-US-059836.xml', 'US-FDACVM-2022-US-059837.xml', 'US-FDACVM-2022-US-059838.xml',
             'US-FDACVM-2022-US-059840.xml', 'US-FDACVM-2022-US-059842.xml', 'US-FDACVM-2022-US-059844.xml',
             'US-FDACVM-2022-US-059846.xml', 'US-FDACVM-2022-US-059847.xml', 'US-FDACVM-2022-US-059848.xml',
             'US-FDACVM-2022-US-059850.xml', 'US-FDACVM-2022-US-059851.xml', 'US-FDACVM-2022-US-059852.xml',
             'US-FDACVM-2022-US-059853.xml', 'US-FDACVM-2022-US-059854.xml', 'US-FDACVM-2022-US-059855.xml',
             'US-FDACVM-2022-US-059863.xml', 'US-FDACVM-2022-US-059864.xml', 'US-FDACVM-2022-US-059865.xml',
             'US-FDACVM-2022-US-059867.xml', 'US-FDACVM-2022-US-059869.xml', 'US-FDACVM-2022-US-059881.xml',
             'US-FDACVM-2022-US-059882.xml', 'US-FDACVM-2022-US-059883.xml', 'US-FDACVM-2022-US-059884.xml',
             'US-FDACVM-2022-US-059885.xml', 'US-FDACVM-2022-US-059886.xml', 'US-FDACVM-2022-US-059887.xml',
             'US-FDACVM-2022-US-059889.xml', 'US-FDACVM-2022-US-059891.xml', 'US-FDACVM-2022-US-059892.xml',
             'US-FDACVM-2022-US-059893.xml', 'US-FDACVM-2022-US-059894.xml', 'US-FDACVM-2022-US-059896.xml',
             'US-FDACVM-2022-US-059898.xml', 'US-FDACVM-2022-US-059900.xml', 'US-FDACVM-2022-US-059901.xml',
             'US-FDACVM-2022-US-059902.xml', 'US-FDACVM-2022-US-059903.xml', 'US-FDACVM-2022-US-059904.xml',
             'US-FDACVM-2022-US-059905.xml', 'US-FDACVM-2022-US-059908.xml', 'US-FDACVM-2022-US-059909.xml',
             'US-FDACVM-2022-US-059910.xml', 'US-FDACVM-2022-US-059940.xml', 'US-FDACVM-2022-US-059942.xml',
             'US-FDACVM-2022-US-059943.xml', 'US-FDACVM-2022-US-059944.xml', 'US-FDACVM-2022-US-059945.xml',
             'US-FDACVM-2022-US-059946.xml', 'US-FDACVM-2022-US-059947.xml', 'US-FDACVM-2022-US-059948.xml',
             'US-FDACVM-2022-US-059949.xml', 'US-FDACVM-2022-US-059950.xml', 'US-FDACVM-2022-US-059951.xml',
             'US-FDACVM-2022-US-059953.xml', 'US-FDACVM-2022-US-059954.xml', 'US-FDACVM-2022-US-059955.xml',
             'US-FDACVM-2022-US-059956.xml', 'US-FDACVM-2022-US-059959.xml', 'US-FDACVM-2022-US-059960.xml',
             'US-FDACVM-2022-US-059961.xml', 'US-FDACVM-2022-US-059962.xml', 'US-FDACVM-2022-US-059963.xml',
             'US-FDACVM-2022-US-059964.xml', 'US-FDACVM-2022-US-059965.xml', 'US-FDACVM-2022-US-059966.xml',
             'US-FDACVM-2022-US-059967.xml', 'US-FDACVM-2022-US-059968.xml', 'US-FDACVM-2022-US-059969.xml',
             'US-FDACVM-2022-US-059972.xml', 'US-FDACVM-2022-US-059973.xml', 'US-FDACVM-2022-US-059975.xml',
             'US-FDACVM-2022-US-059976.xml', 'US-FDACVM-2022-US-059977.xml', 'US-FDACVM-2022-US-059978.xml',
             'US-FDACVM-2022-US-060303.xml', 'US-FDACVM-2022-US-060304.xml', 'US-FDACVM-2022-US-060307.xml',
             'US-FDACVM-2022-US-060308.xml', 'US-FDACVM-2022-US-060309.xml', 'US-FDACVM-2022-US-060310.xml',
             'US-FDACVM-2022-US-060311.xml', 'US-FDACVM-2022-US-060313.xml', 'US-FDACVM-2022-US-060314.xml',
             'US-FDACVM-2022-US-060316.xml', 'US-FDACVM-2022-US-060317.xml', 'US-FDACVM-2022-US-060318.xml',
             'US-FDACVM-2022-US-060321.xml', 'US-FDACVM-2022-US-060324.xml', 'US-FDACVM-2022-US-060325.xml',
             'US-FDACVM-2022-US-060327.xml', 'US-FDACVM-2022-US-060328.xml', 'US-FDACVM-2022-US-060329.xml',
             'US-FDACVM-2022-US-060330.xml', 'US-FDACVM-2022-US-060331.xml', 'US-FDACVM-2022-US-060332.xml',
             'US-FDACVM-2022-US-060333.xml', 'US-FDACVM-2022-US-060334.xml', 'US-FDACVM-2022-US-060337.xml',
             'US-FDACVM-2022-US-060338.xml', 'US-FDACVM-2022-US-060339.xml', 'US-FDACVM-2022-US-060376.xml',
             'US-FDACVM-2022-US-060385.xml', 'US-FDACVM-2022-US-060386.xml', 'US-FDACVM-2022-US-060387.xml',
             'US-FDACVM-2022-US-060389.xml', 'US-FDACVM-2022-US-060393.xml', 'US-FDACVM-2022-US-060396.xml',
             'US-FDACVM-2022-US-060397.xml', 'US-FDACVM-2022-US-060399.xml', 'US-FDACVM-2022-US-060401.xml',
             'US-FDACVM-2022-US-060402.xml', 'US-FDACVM-2022-US-060403.xml', 'US-FDACVM-2022-US-060405.xml',
             'US-FDACVM-2022-US-060406.xml', 'US-FDACVM-2022-US-060407.xml', 'US-FDACVM-2022-US-060408.xml',
             'US-FDACVM-2022-US-060410.xml', 'US-FDACVM-2022-US-060411.xml', 'US-FDACVM-2022-US-060415.xml',
             'US-FDACVM-2022-US-060418.xml', 'US-FDACVM-2022-US-060420.xml', 'US-FDACVM-2022-US-060430.xml',
             'US-FDACVM-2022-US-060431.xml', 'US-FDACVM-2022-US-060432.xml', 'US-FDACVM-2022-US-060440.xml',
             'US-FDACVM-2022-US-060441.xml', 'US-FDACVM-2022-US-060446.xml', 'US-FDACVM-2022-US-060449.xml',
             'US-FDACVM-2022-US-060450.xml', 'US-FDACVM-2022-US-060453.xml', 'US-FDACVM-2022-US-060454.xml',
             'US-FDACVM-2022-US-060456.xml', 'US-FDACVM-2022-US-060457.xml', 'US-FDACVM-2022-US-060490.xml',
             'US-FDACVM-2022-US-060499.xml', 'US-FDACVM-2022-US-060500.xml', 'US-FDACVM-2022-US-060502.xml',
             'US-FDACVM-2022-US-060504.xml', 'US-FDACVM-2022-US-060505.xml', 'US-FDACVM-2022-US-060506.xml',
             'US-FDACVM-2022-US-060508.xml', 'US-FDACVM-2022-US-060509.xml', 'US-FDACVM-2022-US-060512.xml',
             'US-FDACVM-2022-US-060514.xml', 'US-FDACVM-2022-US-060517.xml', 'US-FDACVM-2022-US-060518.xml',
             'US-FDACVM-2022-US-060520.xml', 'US-FDACVM-2022-US-060521.xml', 'US-FDACVM-2022-US-060522.xml',
             'US-FDACVM-2022-US-060524.xml', 'US-FDACVM-2022-US-060528.xml', 'US-FDACVM-2022-US-060533.xml',
             'US-FDACVM-2023-US-000014.xml', 'US-FDACVM-2023-US-001158.xml', 'US-FDACVM-2023-US-001634.xml',
             'US-FDACVM-2023-US-001767.xml', 'US-FDACVM-2023-US-001768.xml', 'US-FDACVM-2023-US-001769.xml',
             'US-FDACVM-2023-US-001770.xml', 'US-FDACVM-2023-US-001771.xml', 'US-FDACVM-2023-US-001772.xml',
             'US-FDACVM-2023-US-001773.xml', 'US-FDACVM-2023-US-001774.xml', 'US-FDACVM-2023-US-001775.xml',
             'US-FDACVM-2023-US-001776.xml', 'US-FDACVM-2023-US-001930.xml', 'US-FDACVM-2023-US-001937.xml',
             'US-FDACVM-2023-US-001987.xml', 'US-FDACVM-2023-US-001996.xml', 'US-FDACVM-2023-US-002004.xml',
             'US-FDACVM-2023-US-002028.xml', 'US-FDACVM-2023-US-002031.xml', 'US-FDACVM-2023-US-002035.xml',
             'US-FDACVM-2023-US-002050.xml', 'US-FDACVM-2023-US-002073.xml', 'US-FDACVM-2023-US-002137.xml',
             'US-FDACVM-2023-US-002143.xml', 'US-FDACVM-2023-US-002146.xml', 'US-FDACVM-2023-US-002152.xml',
             'US-FDACVM-2023-US-002157.xml', 'US-FDACVM-2023-US-002175.xml', 'US-FDACVM-2023-US-002179.xml',
             'US-FDACVM-2023-US-002212.xml', 'US-FDACVM-2023-US-002213.xml', 'US-FDACVM-2023-US-002217.xml',
             'US-FDACVM-2023-US-002221.xml', 'US-FDACVM-2023-US-002229.xml', 'US-FDACVM-2023-US-002239.xml',
             'US-FDACVM-2023-US-002266.xml', 'US-FDACVM-2023-US-002292.xml', 'US-FDACVM-2023-US-002306.xml',
             'US-FDACVM-2023-US-002359.xml', 'US-FDACVM-2023-US-002365.xml', 'US-FDACVM-2023-US-002388.xml',
             'US-FDACVM-2023-US-002401.xml', 'US-FDACVM-2023-US-002557.xml', 'US-FDACVM-2023-US-002748.xml',
             'US-FDACVM-2023-US-002749.xml', 'US-FDACVM-2023-US-002751.xml', 'US-FDACVM-2023-US-002757.xml',
             'US-FDACVM-2023-US-002758.xml', 'US-FDACVM-2023-US-002759.xml', 'US-FDACVM-2023-US-002760.xml',
             'US-FDACVM-2023-US-002761.xml', 'US-FDACVM-2023-US-002762.xml', 'US-FDACVM-2023-US-002763.xml',
             'US-FDACVM-2023-US-002764.xml', 'US-FDACVM-2023-US-002919.xml', 'US-FDACVM-2023-US-002930.xml',
             'US-FDACVM-2023-US-002965.xml', 'US-FDACVM-2023-US-002972.xml', 'US-FDACVM-2023-US-002979.xml',
             'US-FDACVM-2023-US-002989.xml', 'US-FDACVM-2023-US-002992.xml', 'US-FDACVM-2023-US-003005.xml',
             'US-FDACVM-2023-US-003018.xml', 'US-FDACVM-2023-US-003025.xml', 'US-FDACVM-2023-US-003031.xml',
             'US-FDACVM-2023-US-003035.xml', 'US-FDACVM-2023-US-003036.xml', 'US-FDACVM-2023-US-003042.xml',
             'US-FDACVM-2023-US-003104.xml', 'US-FDACVM-2023-US-003106.xml', 'US-FDACVM-2023-US-003110.xml',
             'US-FDACVM-2023-US-003112.xml', 'US-FDACVM-2023-US-003128.xml', 'US-FDACVM-2023-US-003154.xml',
             'US-FDACVM-2023-US-003163.xml', 'US-FDACVM-2023-US-003169.xml', 'US-FDACVM-2023-US-003181.xml',
             'US-FDACVM-2023-US-003186.xml', 'US-FDACVM-2023-US-003202.xml', 'US-FDACVM-2023-US-003204.xml',
             'US-FDACVM-2023-US-003212.xml', 'US-FDACVM-2023-US-003237.xml', 'US-FDACVM-2023-US-003239.xml',
             'US-FDACVM-2023-US-003242.xml', 'US-FDACVM-2023-US-003245.xml', 'US-FDACVM-2023-US-003256.xml',
             'US-FDACVM-2023-US-003259.xml', 'US-FDACVM-2023-US-003261.xml', 'US-FDACVM-2023-US-003270.xml',
             'US-FDACVM-2023-US-003292.xml', 'US-FDACVM-2023-US-003294.xml', 'US-FDACVM-2023-US-003295.xml',
             'US-FDACVM-2023-US-003322.xml', 'US-FDACVM-2023-US-003342.xml', 'US-FDACVM-2023-US-003370.xml',
             'US-FDACVM-2023-US-003376.xml', 'US-FDACVM-2023-US-003384.xml', 'US-FDACVM-2023-US-003389.xml',
             'US-FDACVM-2023-US-003399.xml', 'US-FDACVM-2023-US-003956.xml', 'US-FDACVM-2023-US-003957.xml',
             'US-FDACVM-2023-US-003991.xml', 'US-FDACVM-2023-US-004002.xml', 'US-FDACVM-2023-US-004038.xml',
             'US-FDACVM-2023-US-004043.xml', 'US-FDACVM-2023-US-004044.xml', 'US-FDACVM-2023-US-004048.xml',
             'US-FDACVM-2023-US-004057.xml', 'US-FDACVM-2023-US-004114.xml', 'US-FDACVM-2023-US-004123.xml',
             'US-FDACVM-2023-US-004124.xml', 'US-FDACVM-2023-US-004125.xml', 'US-FDACVM-2023-US-004126.xml',
             'US-FDACVM-2023-US-004130.xml', 'US-FDACVM-2023-US-004131.xml', 'US-FDACVM-2023-US-004132.xml',
             'US-FDACVM-2023-US-004133.xml', 'US-FDACVM-2023-US-004134.xml', 'US-FDACVM-2023-US-004135.xml',
             'US-FDACVM-2023-US-004136.xml', 'US-FDACVM-2023-US-004137.xml', 'US-FDACVM-2023-US-004141.xml',
             'US-FDACVM-2023-US-004178.xml', 'US-FDACVM-2023-US-004180.xml', 'US-FDACVM-2023-US-004185.xml',
             'US-FDACVM-2023-US-004196.xml', 'US-FDACVM-2023-US-004217.xml', 'US-FDACVM-2023-US-004313.xml',
             'US-FDACVM-2023-US-004335.xml', 'US-FDACVM-2023-US-004337.xml', 'US-FDACVM-2023-US-004351.xml',
             'US-FDACVM-2023-US-004364.xml', 'US-FDACVM-2023-US-004390.xml', 'US-FDACVM-2023-US-004391.xml',
             'US-FDACVM-2023-US-004399.xml', 'US-FDACVM-2023-US-004408.xml', 'US-FDACVM-2023-US-004441.xml',
             'US-FDACVM-2023-US-004794.xml', 'US-FDACVM-2023-US-004795.xml', 'US-FDACVM-2023-US-004796.xml',
             'US-FDACVM-2023-US-004797.xml', 'US-FDACVM-2023-US-004798.xml', 'US-FDACVM-2023-US-004799.xml',
             'US-FDACVM-2023-US-004800.xml', 'US-FDACVM-2023-US-004976.xml', 'US-FDACVM-2023-US-004994.xml',
             'US-FDACVM-2023-US-005076.xml', 'US-FDACVM-2023-US-005092.xml', 'US-FDACVM-2023-US-005123.xml',
             'US-FDACVM-2023-US-005165.xml', 'US-FDACVM-2023-US-005197.xml', 'US-FDACVM-2023-US-005218.xml',
             'US-FDACVM-2023-US-005226.xml', 'US-FDACVM-2023-US-005230.xml', 'US-FDACVM-2023-US-005348.xml',
             'US-FDACVM-2023-US-005381.xml', 'US-FDACVM-2023-US-005394.xml', 'US-FDACVM-2023-US-005403.xml',
             'US-FDACVM-2023-US-005418.xml', 'US-FDACVM-2023-US-005445.xml', 'US-FDACVM-2023-US-005461.xml',
             'US-FDACVM-2023-US-005897.xml', 'US-FDACVM-2023-US-005904.xml', 'US-FDACVM-2023-US-006023.xml',
             'US-FDACVM-2023-US-006048.xml', 'US-FDACVM-2023-US-006081.xml', 'US-FDACVM-2023-US-006083.xml',
             'US-FDACVM-2023-US-006093.xml', 'US-FDACVM-2023-US-006126.xml', 'US-FDACVM-2023-US-006159.xml',
             'US-FDACVM-2023-US-006172.xml', 'US-FDACVM-2023-US-006180.xml', 'US-FDACVM-2023-US-006189.xml',
             'US-FDACVM-2023-US-006204.xml', 'US-FDACVM-2023-US-006206.xml', 'US-FDACVM-2023-US-006215.xml',
             'US-FDACVM-2023-US-006218.xml', 'US-FDACVM-2023-US-006234.xml', 'US-FDACVM-2023-US-006305.xml',
             'US-FDACVM-2023-US-006355.xml', 'US-FDACVM-2023-US-006368.xml', 'US-FDACVM-2023-US-006930.xml',
             'US-FDACVM-2023-US-006931.xml', 'US-FDACVM-2023-US-006932.xml', 'US-FDACVM-2023-US-006933.xml',
             'US-FDACVM-2023-US-006934.xml', 'US-FDACVM-2023-US-006935.xml', 'US-FDACVM-2023-US-006936.xml',
             'US-FDACVM-2023-US-006937.xml', 'US-FDACVM-2023-US-006938.xml', 'US-FDACVM-2023-US-007118.xml',
             'US-FDACVM-2023-US-007274.xml', 'US-FDACVM-2023-US-007302.xml', 'US-FDACVM-2023-US-007330.xml',
             'US-FDACVM-2023-US-007367.xml', 'US-FDACVM-2023-US-007371.xml', 'US-FDACVM-2023-US-007376.xml',
             'US-FDACVM-2023-US-007390.xml', 'US-FDACVM-2023-US-007483.xml', 'US-FDACVM-2023-US-007499.xml',
             'US-FDACVM-2023-US-007525.xml', 'US-FDACVM-2023-US-007987.xml', 'US-FDACVM-2023-US-008166.xml',
             'US-FDACVM-2023-US-008179.xml', 'US-FDACVM-2023-US-008262.xml', 'US-FDACVM-2023-US-008339.xml',
             'US-FDACVM-2023-US-008346.xml', 'US-FDACVM-2023-US-008431.xml', 'US-FDACVM-2023-US-008500.xml',
             'US-FDACVM-2023-US-008515.xml', 'US-FDACVM-2023-US-008531.xml', 'US-FDACVM-2023-US-008638.xml',
             'US-FDACVM-2023-US-009286.xml', 'US-FDACVM-2023-US-009287.xml', 'US-FDACVM-2023-US-009288.xml',
             'US-FDACVM-2023-US-009289.xml', 'US-FDACVM-2023-US-009290.xml', 'US-FDACVM-2023-US-009291.xml',
             'US-FDACVM-2023-US-009586.xml', 'US-FDACVM-2023-US-009813.xml', 'US-FDACVM-2023-US-010035.xml',
             'US-FDACVM-2023-US-010254.xml', 'US-FDACVM-2023-US-010255.xml', 'US-FDACVM-2023-US-010256.xml',
             'US-FDACVM-2023-US-010659.xml', 'US-FDACVM-2023-US-010671.xml', 'US-FDACVM-2023-US-010768.xml',
             'US-FDACVM-2023-US-010773.xml', 'US-FDACVM-2023-US-010823.xml', 'US-FDACVM-2023-US-010825.xml',
             'US-FDACVM-2023-US-010886.xml', 'US-FDACVM-2023-US-011578.xml', 'US-FDACVM-2023-US-011774.xml',
             'US-FDACVM-2023-US-012115.xml', 'US-FDACVM-2023-US-012124.xml', 'US-FDACVM-2023-US-032236.xml',
             'US-FDACVM-2023-US-028602.xml', 'US-FDACVM-2022-US-054115.xml', 'US-FDACVM-2023-US-036527.xml',
             'US-FDACVM-2022-CA-000020.xml', 'US-FDACVM-2023-US-038113.xml', 'US-FDACVM-2023-US-038127.xml',
             'US-FDACVM-2023-US-049402.xml', 'US-FDACVM-2024-US-014919.xml', 'US-FDACVM-2024-US-010581.xml',
             'US-FDACVM-2024-US-013907.xml', 'US-FDACVM-2025-US-008523.xml', 'US-FDACVM-2025-US-009147.xml']


class SyncS3(luigi.Task):
  bucket = ADAE_BUCKET
  local_dir = ADAE_LOCAL_DIR
  aws = AWS_CLI

  def output(self):
    return luigi.LocalTarget(ADAE_LOCAL_DIR)

  def run(self):
    common.quiet_cmd([self.aws,
                '--profile=' + config.aws_profile(),
                's3', 'sync',
                self.bucket,
                self.local_dir])


class ExtractXML(parallel.MRTask):
  local_dir = ADAE_LOCAL_DIR

  def requires(self):
    return SyncS3()

  def output(self):
    return luigi.LocalTarget(os.path.join(config.data_dir('adae/extracted'), BATCH))

  def mapreduce_inputs(self):
    return parallel.Collection.from_glob(os.path.join(self.local_dir, '*.zip'))

  def map(self, zip_file, value, output):
    output_dir = self.output().path
    common.shell_cmd_quiet('mkdir -p %s', output_dir)
    common.shell_cmd_quiet('7z x "%s" -aoa -bd -y -o%s', zip_file, output_dir)

  def output_format(self):
    return NullOutput()

class XML2JSONMapper(parallel.Mapper):
  UNIT_OF_MEASUREMENTS_MAP = {
    "a": "Year",
    "s": "Second",
    "min": "Minute",
    "h": "Hour",
    "d": "Day",
    "wk": "Week",
    "mo": "Month",
    "g": "Gram",
    "mg": "Milligram",
    "ug": "Microgram",
    "ng": "Nanogram",
    "kg": "Kilogram"
  }

  VALUE_DECODE_MAP = {
    "unit": UNIT_OF_MEASUREMENTS_MAP,
    "numerator_unit": UNIT_OF_MEASUREMENTS_MAP,
    "denominator_unit": UNIT_OF_MEASUREMENTS_MAP
  }

  NS = {'ns': 'urn:hl7-org:v3'}

  DATE_FIELDS = ['original_receive_date', 'onset_date', 'first_exposure_date', 'last_exposure_date',
                 'manufacturing_date', 'lot_expiration']

  def map_shard(self, map_input, map_output):

    XPATH_MAP = {
      "//ns:investigationEvent/ns:id[@root='1.2.3.4']/@extension": ["@id", "unique_aer_id_number"],
      "//ns:attentionLine/ns:keyWordText[text()='Report Identifier']/following-sibling::ns:value": ["report_id"],
      "//ns:outboundRelationship/ns:priorityNumber[@value='1']/following-sibling::ns:relatedInvestigation/ns:effectiveTime/@value": [
        "original_receive_date"],

      "//ns:investigationEvent//ns:primaryInformationRecipient/ns:assignedEntity/ns:representedOrganization/ns:name":
        ["receiver.organization"],
      "//ns:investigationEvent//ns:primaryInformationRecipient/ns:assignedEntity/ns:addr/ns:streetAddressLine":
        ["receiver.street_address"],
      "//ns:investigationEvent//ns:primaryInformationRecipient/ns:assignedEntity/ns:addr/ns:city":
        ["receiver.city"],
      "//ns:investigationEvent//ns:primaryInformationRecipient/ns:assignedEntity/ns:addr/ns:state":
        ["receiver.state"],
      "//ns:investigationEvent//ns:primaryInformationRecipient/ns:assignedEntity/ns:addr/ns:postalCode":
        ["receiver.postal_code"],
      "//ns:investigationEvent//ns:primaryInformationRecipient/ns:assignedEntity/ns:addr/ns:country":
        ["receiver.country"],

      "//ns:outboundRelationship/ns:priorityNumber[@value='1']/following-sibling::ns:relatedInvestigation//ns:code[@codeSystem='2.16.840.1.113883.13.194']/@displayName": [
        "primary_reporter"],
      "//ns:outboundRelationship/ns:priorityNumber[@value='2']/following-sibling::ns:relatedInvestigation//ns:code[@codeSystem='2.16.840.1.113883.13.194']/@displayName": [
        "secondary_reporter"],
      "//ns:investigationCharacteristic/ns:code[@code='T95004']/following-sibling::ns:value/@displayName": [
        "type_of_information"],
      "//ns:investigationCharacteristic/ns:code[@code='T95022']/following-sibling::ns:value/@value": [
        "serious_ae"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:player2[@classCode='ANM']/ns:quantity/@value": [
        "number_of_animals_treated"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2//ns:code[@code='T95005']/following-sibling::ns:value/@value": [
        "number_of_animals_affected"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:player2[@classCode='ANM']/ns:code[@codeSystem='2.16.840.1.113883.4.341']/@displayName": [
        "animal.species"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:player2[@classCode='ANM']/ns:administrativeGenderCode/@displayName": [
        "animal.gender"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:player2[@classCode='ANM']/ns:genderStatusCode/@displayName": [
        "animal.reproductive_status"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95010']/following-sibling::ns:value/@displayName": [
        "animal.female_animal_physiological_status"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95012']/following-sibling::ns:value/ns:low/@value": [
        "animal.age.min"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95012']/following-sibling::ns:value/ns:high/@value": [
        "animal.age.max"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95012']/following-sibling::ns:value/ns:low/@unit": [
        "animal.age.unit"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95012']/following-sibling::ns:value/following-sibling::ns:methodCode[@codeSystem='2.16.840.1.113883.13.200']/@displayName": [
        "animal.age.qualifier"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95011']/following-sibling::ns:value/ns:low/@value": [
        "animal.weight.min"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95011']/following-sibling::ns:value/ns:high/@value": [
        "animal.weight.max"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95011']/following-sibling::ns:value/ns:low/@unit": [
        "animal.weight.unit"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95011']/following-sibling::ns:value/following-sibling::ns:methodCode[@codeSystem='2.16.840.1.113883.13.200']/@displayName": [
        "animal.weight.qualifier"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95007']/following-sibling::ns:value/@value": [
        "animal.breed.is_crossbred"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95008']/following-sibling::ns:value[@codeSystem='2.16.840.1.113883.4.342']/@displayName": [
        "animal.breed.breed_component"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='C53279' or @code='C82467' or @code='C49495' or @code='C28554' or @code='C21115' or @code='C17998']/following-sibling::ns:value[@value>0]/..":
        self.outcome,
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95006']/following-sibling::ns:value/@displayName": [
        "health_assessment_prior_to_exposure.condition"],
      "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95006']/following-sibling::ns:author//ns:code/@displayName": [
        "health_assessment_prior_to_exposure.assessed_by"],

      "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:substanceAdministration": self.drug,

      "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:observation/ns:code[@code='T95020']/following-sibling::ns:effectiveTime/ns:low/@value": [
        "onset_date"],
      "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:observation/ns:code[@code='T95020']/following-sibling::ns:effectiveTime/ns:width/@value": [
        "duration.value"],
      "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:observation/ns:code[@code='T95020']/following-sibling::ns:effectiveTime/ns:width/@unit": [
        "duration.unit"],

      "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:observation/ns:code[@code='T95020']/following-sibling::ns:value[@codeSystem='2.16.840.1.113883.4.358' or @codeSystem='2.16.840.1.113883.13.226' or @codeSystem='2.16.840.1.113883.13.227']/..":
        self.reaction,

      "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:observation/ns:code[@code='T95021']/following-sibling::ns:value/@displayName":
        ["time_between_exposure_and_onset"],
      "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:observation/ns:code[@code='T95023']/following-sibling::ns:value/@value":
        ["treated_for_ae"]

    }

    ae = {}
    try:
      if os.path.getsize(map_input.filename) > 0:
        tree = etree.parse(open(map_input.filename))
        self.process_xpath_map(XPATH_MAP, tree, ae)
        map_output.add(ae["unique_aer_id_number"], ae)
      else:
        logging.warning("Zero length input file: " + map_input.filename)
    except lxml.etree.XMLSyntaxError as err:
      logging.warning("Malformed input file: " + map_input.filename + ". Error was " + str(err))
    except Exception:
      traceback.print_exc()
      logging.error(sys.exc_info()[0])
      raise

  def process_xpath_map(self, xpath_map, root_node, json):
    for xpath, json_fields in iter(xpath_map.items()):
      nodeset = root_node.xpath(xpath,
                                namespaces=self.NS)
      if type(nodeset) == list and len(nodeset) > 0:
        self.set_value_in_json(nodeset, json_fields, json)

  def set_value_in_json(self, nodeset, json_fields, json):

    if callable(json_fields):
      json_fields(nodeset, json)
      return

    val = []
    for el in nodeset:
      if type(el) == etree._Element:
        val.append(el.text)
      else:
        val.append(str(el))

    val = val if len(val) > 1 else val[0]

    for field in json_fields:
      if type(field) == str:
        self.set_field_dot_notation(val, field, json)

  def set_field_dot_notation(self, val, field, json):
    # Decode value if needed
    if field in self.VALUE_DECODE_MAP:
      if val in self.VALUE_DECODE_MAP[field]:
        val = self.VALUE_DECODE_MAP[field][val]

    parts = field.split('.')
    if (len(parts) > 1):
      first = parts[0]
      if (json.get(first) == None):
        json[first] = {}
      self.set_field_dot_notation(val, '.'.join(parts[1:]), json[first])
    elif val is not None:
      if not (field in self.DATE_FIELDS and not val):
        json[field] = val

  def isdigit(self, s):
    try:
      float(s)
      return True
    except ValueError:
      return False

  def drug(self, nodeset, json):

    XPATH_MAP = {
      "./ns:effectiveTime/ns:comp/ns:low/@value": [
        "first_exposure_date"],
      "./ns:effectiveTime/ns:comp/ns:high/@value": [
        "last_exposure_date"],
      "./ns:effectiveTime/ns:comp/ns:period/@value": [
        "frequency_of_administration.value"],
      "./ns:effectiveTime/ns:comp/ns:period/@unit": [
        "frequency_of_administration.unit"],
      "./ns:performer/ns:assignedEntity/ns:code/@displayName": [
        "administered_by"],
      "./ns:routeCode/@displayName": [
        "route"],
      "./ns:doseCheckQuantity/ns:numerator/@value": [
        "dose.numerator"],
      "./ns:doseCheckQuantity/ns:numerator/ns:translation/@displayName": [
        "dose.numerator_unit"],
      "./ns:doseCheckQuantity/ns:denominator/@value": [
        "dose.denominator"],
      "./ns:doseCheckQuantity/ns:denominator/ns:translation/@displayName": [
        "dose.denominator_unit"],
      ".//ns:code[@code='T95015']/following-sibling::ns:value/@value": [
        "used_according_to_label"],
      ".//ns:code[@code='T95016']/following-sibling::ns:outboundRelationship2//ns:value[@value='true']/preceding-sibling::ns:code/@displayName": [
        "off_label_use"],
      ".//ns:code[@code='T95024']/following-sibling::ns:value/@value": [
        "previous_exposure_to_drug"],
      ".//ns:code[@code='T95025']/following-sibling::ns:value/@value": [
        "previous_ae_to_drug"],
      ".//ns:code[@code='T95026']/following-sibling::ns:value/@value": [
        "ae_abated_after_stopping_drug"],
      ".//ns:code[@code='T95027']/following-sibling::ns:value/@value": [
        "ae_reappeared_after_resuming_drug"],
      "./ns:consumable/ns:instanceOfKind/ns:productInstanceInstance/ns:existenceTime/ns:low/@value": [
        "manufacturing_date"],
      "./ns:consumable/ns:instanceOfKind/ns:productInstanceInstance/ns:lotNumberText": [
        "lot_number"],
      "./ns:consumable/ns:instanceOfKind/ns:productInstanceInstance/ns:lotNumberText/following-sibling::ns:expirationTime/@value": [
        "lot_expiration"],
      "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:code/@code": [
        "product_ndc"],
      "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:code/following-sibling::ns:name": [
        "brand_name"],
      "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:code/following-sibling::ns:formCode/@displayName": [
        "dosage_form"],
      "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:asManufacturedProduct/ns:manufacturerOrganization/ns:name": [
        "manufacturer.name"],
      "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:asManufacturedProduct/ns:subjectOf/ns:approval/ns:id/@extension": [
        "manufacturer.registration_number"],
      "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:instanceOfKind//ns:code[@code='T95017']/following-sibling::ns:value/@value": [
        "number_of_defective_items"],
      "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:instanceOfKind//ns:code[@code='T95018']/following-sibling::ns:value/@value": [
        "number_of_items_returned"],
      "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:asSpecializedKind//ns:code[@code='T95013']/following-sibling::ns:name": [
        "atc_vet_code"],
      "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:ingredient":
        self.ingredient
    }

    json["drug"] = []
    for node in nodeset:
      drug = {}
      self.process_xpath_map(XPATH_MAP, node, drug)
      # Convert yyyymm to yyyy-mm that ES can understand.
      if drug.get('lot_expiration') is not None:
        drug['lot_expiration'] = re.sub(r'^(\d{4})(\d{2})$', r'\1-\2', drug['lot_expiration'])

      # A corner-case scenario (US-FDACVM-2014-US-065247.xml)
      if drug.get('dose') is not None and drug['dose'].get('denominator') is not None and not self.isdigit(drug['dose'][
        'denominator']):
        logging.warning("Non-numeric denominator: " + drug['dose'][
          'denominator'])
        drug['dose']['denominator'] = '0'


      json["drug"].append(drug)

  def ingredient(self, nodeset, json):

    XPATH_MAP = {
      "./ns:ingredientSubstance/ns:name": ["name"],
      "./ns:quantity/ns:numerator/@value": [
        "dose.numerator"],
      "./ns:quantity/ns:numerator/ns:translation/@displayName": [
        "dose.numerator_unit"],
      "./ns:quantity/ns:denominator/@value": [
        "dose.denominator"],
      "./ns:quantity/ns:denominator/ns:translation/@displayName": [
        "dose.denominator_unit"]
    }

    json["active_ingredients"] = []
    for node in nodeset:
      ingredient = {}
      self.process_xpath_map(XPATH_MAP, node, ingredient)
      if "name" in list(ingredient.keys()):
        ingredient["name"] = ingredient["name"].title()
      json["active_ingredients"].append(ingredient)

  def outcome(self, nodeset, json):

    XPATH_MAP = {
      "./ns:code/@displayName": ["medical_status"],
      "./ns:value/@value": ["number_of_animals_affected"]
    }

    json["outcome"] = []
    for node in nodeset:
      outcome = {}
      self.process_xpath_map(XPATH_MAP, node, outcome)
      json["outcome"].append(outcome)

  def reaction(self, nodeset, json):

    XPATH_MAP = {
      "./ns:value[@codeSystem='2.16.840.1.113883.4.358' or @codeSystem='2.16.840.1.113883.13.226' or @codeSystem='2.16.840.1.113883.13.227']/@codeSystemVersion": [
        "veddra_version"],
      "./ns:value[@codeSystem='2.16.840.1.113883.4.358' or @codeSystem='2.16.840.1.113883.13.226' or @codeSystem='2.16.840.1.113883.13.227']/@code": [
        "veddra_term_code"],
      "./ns:value[@codeSystem='2.16.840.1.113883.4.358' or @codeSystem='2.16.840.1.113883.13.226' or @codeSystem='2.16.840.1.113883.13.227']/@displayName": [
        "veddra_term_name"],
      "./ns:referenceRange/ns:observationRange/ns:value/@value": ["number_of_animals_affected"],
      "./ns:referenceRange/ns:observationRange/ns:interpretationCode/@displayName": ["accuracy"]
    }

    json["reaction"] = []
    for node in nodeset:
      reaction = {}
      self.process_xpath_map(XPATH_MAP, node, reaction)
      json["reaction"].append(reaction)


class XML2JSON(luigi.Task):
  def requires(self):
    return ExtractXML()

  def output(self):
    return luigi.LocalTarget(os.path.join(config.data_dir('adae/json.db'), BATCH))

  def run(self):
    input_shards = []
    input_dir = self.input().path
    for subdir, dirs, files in os.walk(input_dir):
      for file in files:
        if file.endswith('.xml'):
          if not file in NULLIFIED:
            input_shards.append(os.path.join(subdir, file))
          else:
            logging.info("Skipping a nullified case: " + file)

    parallel.mapreduce(
      parallel.Collection.from_list(input_shards),
      mapper=XML2JSONMapper(),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path)


# Annotation code left in for posterity. This class is not currently used at the request of CVM.
class AnnotateEvent(luigi.Task):
  def requires(self):
    return [CombineHarmonization(), XML2JSON()]

  def output(self):
    return luigi.LocalTarget(os.path.join(config.data_dir('adae/annotate.db'), BATCH))

  def run(self):
    harmonized_file = self.input()[0].path
    parallel.mapreduce(
      parallel.Collection.from_sharded(self.input()[1].path),
      mapper=annotate.AnnotateMapper(harmonized_dict=annotate.read_harmonized_file(open(harmonized_file))),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path)


class LoadJSON(index_util.LoadJSONBase):
  index_name = 'animalandveterinarydrugevent'
  mapping_file = './schemas/animalandveterinarydrugevent_mapping.json'
  data_source = XML2JSON()
  use_checksum = False
  optimize_index = True
  last_update_date = lambda _: newest_file_timestamp(ADAE_LOCAL_DIR)


if __name__ == '__main__':
  luigi.run()
