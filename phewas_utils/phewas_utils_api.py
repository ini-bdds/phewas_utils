import os
import csv
import logging
import json
import hashlib
import re
from collections import OrderedDict
import phewas_utils as pu
from phewas_utils.converters.freesurfer import fs2csv

logger = logging.getLogger(__name__)


def configure_logging(level=logging.INFO, logpath=None):
    logging.captureWarnings(True)
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    if logpath:
        logging.basicConfig(filename=logpath, level=level, format=log_format)
    else:
        logging.basicConfig(level=level, format=log_format)


def read_config(config_file):
    logger.info("Reading configuration file %s" % config_file)
    with open(config_file) as cf:
        config = cf.read()
        return json.loads(config, object_pairs_hook=OrderedDict)


def process_input_dir(config_file, input_path, output_path, batch_id=None):
    # process config file
    config = read_config(config_file)
    metadata_fields = config.get('metadata_fields', None)
    if metadata_fields:
        pu.METADATA_FIELDS = metadata_fields
    metrics_fields = config.get('metrics_fields', None)
    if metrics_fields:
        pu.METRICS_FIELDS = metrics_fields
    image_fields = config.get('image_fields', None)
    if image_fields:
        pu.IMAGE_FIELDS = image_fields

    input_files = {}
    for entry in config.get('metrics', []):
        if 'input_file' in entry:
            input_files[entry['input_file']] = entry

    image_files = {}
    for image in config.get('images', []):
        if 'image_files' in image:
            image_inputs = list(image['image_files'])
            image_files[image_inputs[0]] = image

    # walk input path and invoke conversion process on configured files
    for dirpath, dirnames, filenames in os.walk(input_path):
        subdirs_count = dirnames.__len__()
        if subdirs_count:
            logger.info("%s subdirectories found in input directory %s %s" %
                        (subdirs_count, input_path, dirnames))
        filenames.sort()
        for fn in filenames:
            if fn in input_files:
                input_file = os.path.join(dirpath, fn)
                logger.debug("Processing input file %s" % input_file)
                process_data_file(input_files[fn], input_file, output_path, batch_id)

            image_path = None
            image_config = None
            if fn in image_files:
                image_path = os.path.join(dirpath, fn)
                image_config = image_files[fn]
            if not image_path:
                for key in image_files.keys():
                    if "*" in key:
                        nkey = key.replace('*', '')
                        filename, ext = os.path.splitext(fn)
                        if nkey.lower() == ext:
                            image_path = os.path.join(dirpath, fn)
                            image_config = image_files[key]
                            break

            if image_path:
                logger.debug("Processing image file %s" % image_path)
                process_image_file(fn, image_path, image_config, input_path, output_path, batch_id)


def process_data_file(config, input_file, output_path, batch_id):
    preproc = config.get('preprocessor', None)
    if preproc == 'freesurfer':
        fs2csv.stats2csv(config, input_file, output_path, batch_id)
    else:
        logger.warn("Unsupported preprocessor type %s" % preproc)


def process_image_file(filename, filepath, config, input_path, output_path, batch_id):
    preproc = config.get('preprocessor', None)
    if preproc == 'freesurfer':
        fs2csv.image2csv(filename, filepath, config, input_path, output_path, batch_id)
    else:
        logger.warn("Unsupported preprocessor type %s" % preproc)


def map_column_from_file(config, column_name, column_value):
    new_value = None
    map_entries = config.get('value_mappings', list())
    for entry in map_entries:
        column = entry.get('column', None)
        source = entry.get('source', None)
        pattern = entry.get('pattern', None)
        dest = entry.get('dest', None)
        mapping_file = entry.get('file', None)
        if column and source and dest and mapping_file and (column_name == column):
            with open(mapping_file, 'r') as mappings:
                reader = csv.DictReader(mappings, delimiter=',')
                for row in reader:
                    if pattern:
                        match = re.search(pattern, column_value)
                        if match:
                            column_value = match.group(0)
                    if row[source] == column_value:
                        new_value = row.get(dest, None)
                        break

    if not new_value:
        logger.warn("Unable to map %s: %s using config %s" % (column_name, column_value, json.dumps(map_entries)))
        return column_value
    else:
        return new_value


def calculate_file_hashes(full_path, hashes):
    f_hashers = dict()
    for alg in hashes:
        try:
            f_hashers[alg] = hashlib.new(alg)
        except ValueError:
            logger.warning("Unable to validate file contents using unknown %s hash algorithm", alg)

    logger.info("Calculating %s checksum(s) for file %s" % (set(f_hashers.keys()), full_path))
    if not os.path.exists(full_path):
        logger.warn("%s does not exist" % full_path)
        return

    try:
        with open(full_path, 'rb') as f:
            while True:
                block = f.read(1048576)
                if not block:
                    break
                for i in f_hashers.values():
                    i.update(block)
    except (IOError, OSError) as e:
        logger.warn("Could not read %s: %s" % (full_path, str(e)))
        raise

    return dict((alg, h.hexdigest()) for alg, h in f_hashers.items())
