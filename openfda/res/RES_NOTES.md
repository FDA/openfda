Notes (06/14/2014)
======
Links
------
[Report Labeling Definitions](http://www.fda.gov/Safety/Recalls/EnforcementReports/ucm181313.htm)

[Submission Guidelines](http://www.fda.gov/Safety/Recalls/IndustryGuidance/ucm129259.htm)

Download
------
Extract
------
* It is not clear to me that the pipeline is referencing the proper python code
* **ALL FIELDS SHOULD BE WITH _ not - (naming convention from faers)**
* We are going to need the NDC/UPC in the intermediate format. Currently, the extract goes directly to index format.
* Look at `voluntary_mandated` possible values, we may consider making these separate columns and making them booleans. **Consider** `Firm_Initiated`: *True* seems to be a better way of looking at the data
* `initial_firm_notification`: **consider** an array of strings or splitting into multiple fields (single and multi-channel)

  * Letter, Telephone, Press Release, E-Mail, FAX, Other
  * **OR**, Two or more of the following: Email, Fax, Letter, Press Release, Telephone, Visit
* `distribution_pattern`: may consider some fancy extraction logic for an array of locations
  * currently free text, we could tokenize on some key words (like nation-wide) and then on comma
  * BAG OF WORDS otherwise, **consider** making unique
* `code_info`:
  * **consider** an array of strings based upon various tokens
  * BAG OF WORDS, **consider** making unique
* `product_description`: BAG OF WORDS, **consider** making unique
* `product_quantity`: BAG OF WORDS, **consider** making unique
* `reason_for_recall`: BAG OF WORDS, **consider** making unique



Annotation
------
* Join key is going to be NDC or UPC.
* **Need to get UPC added to the harmonized.json** (it will come from the SPL product imager extract)
* Make sure to reconsider the is_original_packager and other fields cast out during event join.


Mapping
------
* `openfda`: Going with the same structure as the event for now. This may change once we do the join.

* `status`: on-going has a - in the documentation, but not in the data we have. If the - is going to exist, then we will need to use a multi-field, if not, then string is ok.

* `product_type`: exists in both the openfda and the res document. **consider** renaming.

* `voluntary_mandated`:
  * "Voluntary: Firm Initiated"
  * Might want to look at the extraction for this, it looks weird
  * Also, need to see if it looks like `voluntary_mandated`: "Forced: Gov Initiated", for example?

* `code_info`:
  * BAG OF WORDS
  * If it is an array of strings, then we **must** add a code_info_exact field like we have in openfda

* `distribution_pattern`: BAG OF WORDS
* `product_description`: BAG OF WORDS
* `product_quantity`: BAG OF WORDS
* `reason_for_recall`: BAG OF WORDS

Mapping Grid
------

Properties|Data Type|Multi-field|Notes
---:|---|---|---
`product_type`|string|Yes|
`event_id`|long|No|
`status `|string|Yes|
`recalling_firm`|string|Yes|
`city`|string|Yes|
`state`|string|Yes|Puerto Rico means we have to multi-field this one|
`country`|string|Yes|
`voluntary_mandated`|string|Yes|
`initial_firm_notification`|string|Yes|
`distribution_pattern`|string|Yes|BAG OF WORDS
`classification`|string|Yes|
`product_description`|string|No|BAG OF WORDS
`code_info`|string|No|BAG OF WORDS
`product_quantity`|string|No|BAG OF WORDS
`reason_for_recall`|string|No|BAG OF WORDS
`recall_initiation_date`|date|No|
`report_date`|date|No|
`timestamp`|date|No|
`openfda.application_number`|string|No|
`openfda.application_number_exact`|string|No|
`openfda.brand_name`|string|No|
`openfda.brand_name_exact`|string|No|
`openfda.substance_name`|string|No|
`openfda.substance_name_exact`|string|No|
`openfda.dosage_form`|string|No|
`openfda.dosage_form_exact`|string|No|
`openfda.generic_name`|string|No|
`openfda.generic_name_exact`|string|No|
`openfda.manufacturer_name`|string|No|
`openfda.manufacturer_name_exact`|string|No|
`openfda.product_ndc`|string|No|
`openfda.product_ndc_exact`|string|No|
`openfda.product_type`|string|No|
`openfda.product_type_exact`|string|No|
`openfda.route`|string|No|
`openfda.route_exact`|string|No|
`openfda.rxcui`|string|No|
`openfda.rxcui_exact`|string|No|
`openfda.rxstring`|string|No|
`openfda.rxstring_exact`|string|No|
`openfda.rxtty`|string|No|
`openfda.rxtty_exact`|string|No|
`openfda.spl_id`|string|No|
`openfda.spl_id_exact`|string|No|
`openfda.package_ndc`|string|No|
`openfda.package_ndc_exact`|string|No|
`openfda.spl_set_id`|string|No|
`openfda.spl_set_id_exact`|string|No|
`openfda.unii`|string|No|
`openfda.unii_exact`|string|No|
`openfda.pharm_class_moa`|string|No|
`openfda.pharm_class_moa_exact`|string|No|
`openfda.pharm_class_pe`|string|No|
`openfda.pharm_class_pe_exact`|string|No|
`openfda.pharm_class_cs`|string|No|
`openfda.pharm_class_cs_exact`|string|No|
`openfda.pharm_class_epc`|string|No|
`openfda.pharm_class_epc_exact`|string|No|
`openfda.nui`|string|No|
`openfda.nui_exact`|string|No|
***NEW------------>***`openfda.upc`|string|No








