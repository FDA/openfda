#!/usr/bin/python
"""
Extract data from the class_indexing file and join it to SPL set_id.


Also, the set_id in the xml is NOT the set_id in the SPL, there is nothing
that relates to the set_id in the XML file!

Inputs:
        1) Directory filled with xml files that were extracted from
           pharmacologic_class_indexing_spl_files.zip
        2) NDC/Product.txt file
        3) JSON to which we dump the extract
"""
import csv
import fnmatch
import multiprocessing
import os
import simplejson as json
from . import extract_unii


def parallel_extract(files, worker):
    manager = multiprocessing.Manager()
    name_queue = manager.Queue()
    pool = multiprocessing.Pool()
    pool.map_async(worker, [(f, name_queue) for f in files])
    pool.close()
    pool.join()

    rows = {}
    while not name_queue.empty():
        harm_row = name_queue.get()
        rows[harm_row["name"].lower()] = harm_row
    return rows


def harmonization_extract_worker(args):
    filename = args[0]
    name_queue = args[1]
    try:
        tree = extract_unii.parse_xml(filename)
        harmonized = {}

        harmonized["unii"] = extract_unii.extract_unii(tree)
        harmonized["set_id"] = extract_unii.extract_set_id(tree)
        harmonized["name"] = extract_unii.extract_unii_name(tree)
        # zipping together two arrays, since they came from the same xpath locations
        # these are the NUI codes and their respective names
        # we might be able to get the names from somewhere else and avoid the zip
        intermediate = list(
            zip(
                extract_unii.extract_unii_other_code(tree),
                extract_unii.extract_unii_other_name(tree),
            )
        )
        # print intermediate
        header = ["number", "name"]
        harmonized["va"] = [dict(list(zip(header, s))) for s in intermediate]
        name_queue.put(harmonized)
    except Exception as inst:
        print(filename + "has a problem")
        print(inst)


def harmonize_unii(out_file, product_file, unii_file, class_index_dir):
    out = open(out_file, "w")
    meta_file = csv.DictReader(
        open(product_file, "rt", encoding="utf-8", errors="replace"), delimiter="\t"
    )

    ndc_dict = {}
    for row in meta_file:

        # Building the ndc_dict which is the out_file data structure of the final loop
        # A little weird because there are duplicate set_id entries in the Product
        # file. Setting the key of the ndc_dict as the substance name and then
        # checking to see if the set_id is already in value list of that key.
        this_spl_id = row["PRODUCTID"].split("_")[1]
        this_substance = row["SUBSTANCENAME"]

        if this_substance.strip() != "":
            if this_spl_id in ndc_dict:
                tmp_substance = [s.lstrip() for s in this_substance.split(";")]
                ndc_dict[this_spl_id] = set(tmp_substance + ndc_dict[this_spl_id])
                ndc_dict[this_spl_id] = list(ndc_dict[this_spl_id])
            else:
                ndc_dict[this_spl_id] = [s.lstrip() for s in this_substance.split(";")]

    pharma_xmls = []
    # Grab all of the xml files
    for root, _, filenames in os.walk(class_index_dir):
        for filename in fnmatch.filter(filenames, "*.xml"):
            pharma_xmls.append(os.path.join(root, filename))

    # call async worker
    pharma_rows = parallel_extract(pharma_xmls, harmonization_extract_worker)

    unii_rows = extract_unii.load_unii_from_csv(unii_file)

    combo = []

    # Loop over ndc_dict, split its key, look for each token as a separate
    # UNII element, if it is one, then add it to the unii_info dict for this
    # loop cycle, once done with all of the tokenized keys, then loop over each
    # set_id in the ndc_dict value list and push a combine record onto the
    # list that will be the output.
    # Loop handles the many-to-many relationship of ingredients to products.
    unii_pivot = {}
    for key, value in iter(ndc_dict.items()):
        for substance_name in value:
            if substance_name.lower() in pharma_rows:
                if key in unii_pivot:
                    unii_pivot[key].append(pharma_rows[substance_name.lower()])
                else:
                    unii_pivot[key] = [pharma_rows[substance_name.lower()]]
            elif substance_name.lower() in unii_rows:
                if key in unii_pivot:
                    unii_pivot[key].append(unii_rows[substance_name.lower()])
                else:
                    unii_pivot[key] = [unii_rows[substance_name.lower()]]

    for key, value in iter(unii_pivot.items()):
        output_dict = {}
        output_dict["spl_id"] = key
        output_dict["unii_indexing"] = value
        combo.append(output_dict)

    for row in combo:
        out.write(json.dumps(row) + "\n")
