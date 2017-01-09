#!/usr/bin/python

""" Device UDI pipeline for downloading, converting to JSON and
    loading into elasticsearch.
"""

import glob
import logging
import os
import sys
import traceback

import arrow
import luigi
import xmltodict
from openfda import common, config, index_util, parallel
from openfda.device_harmonization.pipeline import (Harmonized2OpenFDA,
                                                   DeviceAnnotateMapper)

DEVICE_UDI_BUCKET = 's3://cdrh-data'
DEVICE_UDI_LOCAL_DIR = config.data_dir('device_udi/s3_sync')
BATCH = arrow.utcnow().floor('day').format('YYYYMMDD')
AWS_CLI = 'aws'


class SyncS3DeviceUDI(luigi.Task):
  bucket = DEVICE_UDI_BUCKET
  local_dir = DEVICE_UDI_LOCAL_DIR
  aws = AWS_CLI

  def flag_file(self):
    return os.path.join(self.local_dir, '.last_sync_time')

  def complete(self):
    # Only run S3 sync once per day.
    if config.disable_downloads():
      return True

    return os.path.exists(self.flag_file()) and (
      arrow.get(os.path.getmtime(self.flag_file())) > arrow.now().floor('day'))

  def run(self):
    common.cmd([self.aws,
                '--profile=' + config.aws_profile(),
                's3', 'sync',
                self.bucket,
                self.local_dir])

    with open(self.flag_file(), 'w') as out_f:
      out_f.write('')


class ExtractXML(luigi.Task):
  local_dir = DEVICE_UDI_LOCAL_DIR

  def requires(self):
    return SyncS3DeviceUDI()

  def output(self):
    return luigi.LocalTarget(os.path.join(config.data_dir('device_udi/extracted'), BATCH))

  def run(self):
    output_dir = self.output().path
    common.shell_cmd('mkdir -p %s', output_dir)
    input_dir = self.local_dir
    for zip_filename in glob.glob(input_dir + '/*.zip'):
      common.shell_cmd('unzip -ou %s -d %s', zip_filename, output_dir)


class XML2JSONMapper(parallel.Mapper):
  def map_shard(self, map_input, map_output):

    # Changing names to match the openFDA naming standard
    # key = source name, value = replacement name
    RENAME_MAP = {
      "deviceRecordStatus": "record_status",
      "devicePublishDate": "publish_date",
      "deviceCommDistributionEndDate": "commercial_distribution_end_date",
      "deviceCommDistributionStatus": "commercial_distribution_status",
      "identifiers": "identifiers",
      "identifier": "identifier",
      "brandName": "brand_name",
      "versionModelNumber": "version_or_model_number",
      "catalogNumber": "catalog_number",
      "companyName": "company_name",
      "deviceCount": "device_count_in_base_package",
      "deviceDescription": "device_description",
      "DMExempt": "is_direct_marking_exempt",
      "premarketExempt": "is_pm_exempt",
      "deviceHCTP": "is_hct_p",
      "deviceKit": "is_kit",
      "deviceCombinationProduct": "is_combination_product",
      "singleUse": "is_single_use",
      "lotBatch": "has_lot_or_batch_number",
      "serialNumber": "has_serial_number",
      "manufacturingDate": "has_manufacturing_date",
      "expirationDate": "has_expiration_date",
      "donationIdNumber": "has_donation_id_number",
      "labeledContainsNRL": "is_labeled_as_nrl",
      "labeledNoNRL": "is_labeled_as_no_nrl",
      "MRISafetyStatus": "mri_safety",
      "rx": "is_rx",
      "otc": "is_otc",
      "contacts": "customer_contacts",
      "customerContact": "customer_contact",
      "gmdnTerms": "gmdn_terms",
      "gmdn": "gmdn",
      "productCodes": "product_codes",
      "fdaProductCode": "code",
      "deviceSizes": "device_sizes",
      "deviceSize": "device_size",
      "storageHandling": "storage_handling",
      "deviceSterile": "is_sterile",
      "sterilizationPriorToUse": "is_sterilization_prior_use",
      "methodTypes": "sterilization_methods",
      "deviceId": "id",
      "deviceIdType": "type",
      "deviceIdIssuingAgency": "issuing_agency",
      "containsDINumber": "unit_of_use_id",
      "pkgQuantity": "quantity_per_package",
      "pkgDiscontinueDate": "package_discontinue_date",
      "pkgStatus": "package_status",
      "pkgType": "package_type",
      "phone": "phone",
      "phoneExtension": "ext",
      "email": "email",
      "gmdnPTName": "name",
      "gmdnPTDefinition": "definition",
      "productCode": "code",
      "productCodeName": "name",
      "sizeType": "type",
      "size": "value",
      "sizeText": "text",
      "storageHandlingType": "type",
      "storageHandlingHigh": "high",
      "storageHandlingLow": "low",
      "storageHandlingSpecialConditionText": "special_conditions",
      "sterilizationMethod": "sterilization_method",
      "feis": "fei_number",
      "premarketSubmissions": "pma_submissions",
      "submissionNumber": "submission_number",
      "supplementNumber": "supplement_number",
      "submissionType": "submission_type",
      "environmentalConditions": "storage"
    }

    IGNORE = ['@xmlns']

    FLATTEN = ["customer_contacts", "device_sizes", "gmdn_terms", "identifiers", "pma_submissions",
               "product_codes", "storage", "fei_number", "sterilization_methods"]

    logging.info('Extracting devices from %s...', map_input.filename)

    def transformer_fn(k, v):
      """ A helper function used for removing and renaming dictionary keys.
            """
      if k in IGNORE: return None
      if v is None: return None
      if type(v) == dict and len(v.keys()) == 0:
        return None
      if type(v) == list and len(v) == 1 and v[0] is None:
        return None
      if k in RENAME_MAP:
        k = RENAME_MAP[k]

      if k in FLATTEN:
        v = v[v.keys()[0]]

      if type(v) == list and len(v) == 1 and type(v[0]) == dict and len(v[0].keys()) == 0:
        return None

      return k, v

    def handle_device(_, xml):
      """Transform the dictionary, which follows the structure of UDI XML files, to a new one that
        matches the UDI mapping in ES.
      """

      try:

        device = common.transform_dict(xml, transformer_fn)

        # Additional transformation of the dictionary not handled by the transformer_fn function.
        if "id" in device:
          del device["id"]
        if "device_sizes" in device:
          for size in device["device_sizes"]:
            if "value" in size:
              value_dict = size["value"]
              del size["value"]
              if "value" in value_dict:
                size["value"] = value_dict["value"]
              if "unit" in value_dict:
                size["unit"] = value_dict["unit"]

        # print(json.dumps(device, indent=4 * ' '))

        # @id will be a concatenation of primary identifier's issuer with the ID number itself.
        for identifier, idenList in xml["identifiers"].iteritems():
          if type(idenList) == type([]):
            for iden in idenList:
              if 'deviceIdType' in iden and 'deviceIdIssuingAgency' in iden and 'Primary' == iden['deviceIdType']:
                device["@id"] = (('' if 'deviceIdIssuingAgency' not in iden else iden['deviceIdIssuingAgency'] + "_") + \
                                 iden["deviceId"]).lower()

        map_output.add(device["@id"], device)
        return True
      except Exception:
        logging.error(xml)
        traceback.print_exc()
        logging.error(sys.exc_info()[0])
        raise

    try:
      xmltodict.parse(open(map_input.filename), encoding="utf-8", xml_attribs=True,
                      force_list=("identifier", "customerContact", "gmdn", "fdaProductCode",
                                  "deviceSize", "storageHandling", "fei", "premarketSubmission"),
                      item_depth=2, item_callback=handle_device)
    except Exception:
      traceback.print_exc()
      logging.error(sys.exc_info()[0])
      raise


class XML2JSON(luigi.Task):
  def requires(self):
    return ExtractXML()

  def output(self):
    return luigi.LocalTarget(os.path.join(config.data_dir('device_udi/json.db'), BATCH))

  def run(self):
    input_shards = []
    input_dir = self.input().path
    for xml_filename in glob.glob(input_dir + '/*.xml'):
      input_shards.append(xml_filename)

    parallel.mapreduce(
      parallel.Collection.from_list(input_shards),
      mapper=XML2JSONMapper(),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path,
      num_shards=len(input_shards))


class UDIAnnotateMapper(DeviceAnnotateMapper):
  """ The UDI document has a unique placement requirement, so we need
      to override the `harmonize()` method to place the `openfda` section on
      each product code `product_codes` list within the document.
  """

  def harmonize(self, data):
    result = dict(data)

    product_codes = []
    for row in result.get('product_codes', []):
      d = dict(row)
      harmonized = self.filter(row)
      if harmonized:
        d['openfda'] = self.flatten(harmonized)
      else:
        d['openfda'] = {}
      product_codes.append(d)
    result['product_codes'] = product_codes
    return result

  def filter(self, data):
    product_code = data['code']
    harmonized = self.harmonized_db.get(product_code, None)
    if harmonized:
      # Remove k_number, fei_number and pma_number from the harmonization section because they are contained in the
      # main record.
      if harmonized.get("510k") is not None:
        del harmonized["510k"]
      if harmonized.get("device_pma") is not None:
        del harmonized["device_pma"]
      if harmonized.get("registration") is not None:
        del harmonized["registration"]

      return harmonized
    return None


class AnnotateDevice(luigi.Task):
  def requires(self):
    return [Harmonized2OpenFDA(), XML2JSON()]

  def output(self):
    return luigi.LocalTarget(os.path.join(config.data_dir('device_udi/annotate.db'), BATCH))

  def run(self):
    harmonized_db = parallel.ShardedDB.open(self.input()[0].path).as_dict()

    parallel.mapreduce(
      parallel.Collection.from_sharded(self.input()[1].path),
      mapper=UDIAnnotateMapper(harmonized_db=harmonized_db),
      reducer=parallel.IdentityReducer(),
      output_prefix=self.output().path,
      num_shards=10)


class LoadJSON(index_util.LoadJSONBase):
  index_name = 'deviceudi'
  type_name = 'deviceudi'
  mapping_file = './schemas/deviceudi_mapping.json'
  data_source = AnnotateDevice()
  #docid_key = '@id'
  use_checksum = False
  optimize_index = False


if __name__ == '__main__':
  luigi.run()
