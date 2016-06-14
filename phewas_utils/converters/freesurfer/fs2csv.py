import os
import re
import sys
import csv
import logging
import tempfile
from itertools import islice
import phewas_utils
from phewas_utils.converters.freesurfer import *
logger = logging.getLogger(__name__)


def stats2csv(config, input_file, output_path, batch_id, append=True):

    logger.info("Converting FreeSurfer stats data from file %s" % input_file)

    subject = None
    columns = None
    file_id = os.path.basename(input_file)
    header = []
    headerlines = 0

    # First, read the FS header, parse out the columns we need, and then write out the header as metadata.
    with open(input_file, 'r') as inf:
        for line in inf.readlines():
            if line.startswith("#"):
                headerlines += 1
                header.append(line)
                if line.startswith("# subjectname"):
                    subject = line.rpartition(" ")[2].strip()
                    subject = phewas_utils.api.map_column_from_file(config, "subject_id", subject)
                if line.startswith("# ColHeaders"):
                    cols = line.partition("ColHeaders")[-1].strip()
                    columns = cols.split(" ")
        output_file = os.path.join(output_path, str('phewas_metadata_batch_%s.csv' % batch_id))
        metadata_file = open(output_file, 'a' if append else 'w')
        metadata_writer = csv.DictWriter(metadata_file, phewas_utils.METADATA_FIELDS, dialect='simplecsv')
        if not os.path.getsize(output_file):
            metadata_writer.writeheader()
        metadata_writer.writerow(
            {"batch_id": batch_id, "subject_id": subject, "file_id": file_id, "metadata": ''.join(header)})
        metadata_file.close()

    # Next, read the tabular portion of the FS file. The FS files use arbitrary length whitespace as delimiters, so
    # these rows are first split by whitespace and re-written to a temp CSV file, keeping only non-null results.
    with open(input_file, 'r') as inf:
        f = islice(inf.readlines(), headerlines, None)
        temp = tempfile.NamedTemporaryFile(mode="r+", prefix=str('phewas_convert_batch_%s' % batch_id))
        for line in f:
            newline = generate_stats_input_row(columns, line)
            temp.write(newline+'\n')
        temp.file.seek(0)
        reader = csv.DictReader(temp, fieldnames=columns, delimiter=',')
        # Now, map the input columns to the output columns and append each row to the output file.
        output_file = os.path.join(output_path, str('phewas_metrics_batch_%s.csv' % batch_id))
        with open(output_file, 'a' if append else 'w') as metrics_file:
            metrics_writer = csv.DictWriter(metrics_file, phewas_utils.METRICS_FIELDS, dialect='simplecsv')
            if not os.path.getsize(output_file):
                metrics_writer.writeheader()
            for row in reader:
                output_row = generate_stats_output_row(config, row)
                output_row['batch_id'] = batch_id
                output_row['subject_id'] = subject
                output_row['file_id'] = file_id
                output_row['atlas'] = config['atlas_name']
                # output_row['preprocessor'] = config['preprocessor']
                metrics_writer.writerow(output_row)
            metrics_file.flush()
    return


def generate_stats_input_row(src_columns, src_row):
    index = 0
    new_row = list()
    for col in src_row.strip().split(" "):
        if col:
            try:
                val = float(col)
                if val == 0:
                    logger.debug(
                        "Encountered zero value %s in column %s of input data, replacing with null value" %
                        (col, src_columns[index]))
                    col = ''
            except ValueError:
                pass
            new_row.append(col)
            index += 1
    return ','.join(new_row)


def generate_stats_output_row(config, input_row):

    logger.debug("Input row: %s" % input_row)

    column_mappings = config.get('column_mappings', list())
    region_prefix = config.get('region_prefix', None)
    region_column = config.get('region_column', None)
    output_row = dict()
    input_fields = input_row.keys()
    output_fields = column_mappings.keys()
    for output_field in output_fields:
        mapped_col = column_mappings[output_field]
        if mapped_col in input_fields:
            output_value = input_row[mapped_col] if not (region_prefix and region_column == output_field)\
                else ''.join([region_prefix, input_row[mapped_col]])
            output_row[output_field] = output_value

    logger.debug("Output row: %s" % output_row)

    return output_row


def regex_subject_replace(config, subject):
    if config.get('subject_id_regex', None):
        pattern = config['subject_id_regex']['pattern']
        repl = config['subject_id_regex']['replacement']
        if pattern and repl:
            newsub = re.sub(pattern, repl, subject)
            if newsub == subject:
                logger.warn("Transformation of subject id using pattern %s and replacement %s resulted"
                            " in no change to subject id %s." % (pattern, repl, subject))
            else:
                subject = newsub
        else:
            logger.warn("Both a regex pattern and a regex replacement string are required for subject_"
                        "id transformation.  Got pattern %s and replacement %s." % (pattern, repl))
    return subject


def image2csv(filename, filepath, config, input_path, output_path, batch_id):
    checksums = None
    hashes = config.get("checksums", None)
    if hashes:
        checksums = phewas_utils.api.calculate_file_hashes(filepath, hashes)

    subject_id = phewas_utils.api.map_column_from_file(config, "subject_id", filepath)
    file_size = os.path.getsize(filepath)

    output_file = os.path.join(output_path, str('phewas_images_batch_%s.csv' % batch_id))
    with open(output_file, 'a') as images_file:
        images_writer = csv.DictWriter(images_file, phewas_utils.IMAGE_FIELDS, dialect='simplecsv')
        if not os.path.getsize(output_file):
            images_writer.writeheader()
        output_row = dict()
        output_row['batch_id'] = batch_id
        output_row['subject_id'] = subject_id
        output_row['filename'] = filename
        output_row['bytes'] = file_size
        output_row['content_type'] = 'application/octet-stream'

        if 'md5' in checksums:
            output_row['md5sum'] = checksums['md5']
        if 'sha1' in checksums:
            output_row['sha1sum'] = checksums['sha1']
        if 'sha256' in checksums:
            output_row['sha256sum'] = checksums['sha256']
        if 'sha512' in checksums:
            output_row['sha512sum'] = checksums['sha512']

        base_url = config.get('base_url', '')
        base_path = config.get('base_path', '')
        url_path = '/'.join([base_url, base_path])
        path_pattern = config.get('path_pattern', None)
        target_path = os.path.relpath(filepath, input_path)
        if path_pattern:
            match = re.search(path_pattern, filepath)
            if match:
                target_path = match.group(0)
        target_path = target_path.replace('\\', '/')
        output_row['filepath'] = target_path

        full_url = '/'.join([url_path, target_path])
        image_files = config.get('image_files', list())
        if image_files.__len__() > 1:
            preview_urls = list()
            for image_file in image_files:
                dirname = os.path.dirname(target_path)
                preview_urls.append("\"" + '/'.join([url_path, dirname, image_file]) + "\"")
            output_row['preview'] = '{\"preview_urls\":[' + ','.join(preview_urls) + ']}'
        else:
            output_row['preview'] = full_url

        output_row['uri'] = full_url

        images_writer.writerow(output_row)
        images_file.flush()
