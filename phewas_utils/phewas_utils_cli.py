import argparse
import os
import sys
import logging
import uuid
from phewas_utils import phewas_utils_api as pua
from phewas_utils import get_named_exception as gne

logger = logging.getLogger(__name__)


def parse_cli():
    description = 'BDDS Phewas Data Utility'

    parser = argparse.ArgumentParser(
        description=description, epilog="For more information see: http://github.com/ini-bdds/phewas_utils")

    parser.add_argument(
        '--quiet', action="store_true", help="Suppress logging output.")

    parser.add_argument(
        '--debug', action="store_true", help="Enable debug logging output.")

    parser.add_argument(
        '--config-file', metavar="<file>", required=True,
        help="Path to the configuration file to be used for processing input data.")

    parser.add_argument(
        '--input-path', metavar="<dir>", required=True,
        help="Path to the base directory containing the subject input data sub-directories to be processed.")

    parser.add_argument(
        '--output-path', metavar="<file>", required=True,
        help="Path to the directory where output files will be generated.")

    parser.add_argument(
        '--batch-id', metavar="<id>",
        help="A label that will be added to each row of the output data set. "
             "If not specified, it will be automatically generated.")

    args = parser.parse_args()

    pua.configure_logging(level=logging.ERROR if args.quiet else (logging.DEBUG if args.debug else logging.INFO))

    path = os.path.abspath(args.input_path)
    if not os.path.exists(path):
        sys.stderr.write("Error: file or directory not found: %s\n\n" % path)
        sys.exit(2)

    return args


def main():

    result = 0
    error = None
    sys.stderr.write('\n')

    args = parse_cli()
    config_file = os.path.abspath(args.config_file)
    input_path = os.path.abspath(args.input_path)
    output_path = os.path.abspath(args.output_path)
    if args.batch_id:
        batch_id = args.batch_id
        logger.info("Using existing Batch ID: %s" % batch_id)
    else:
        batch_id = uuid.uuid1()
        logger.info("No Batch ID specified, creating new batch: %s" % batch_id)

    try:
        pua.process_input_dir(config_file, input_path, output_path, batch_id)
    except Exception as e:
        result = 1
        error = "Error: %s" % gne(e)
    finally:
        if result != 0:
            sys.stderr.write("\n%s" % error)

    sys.stderr.write('\n')

    return result

if __name__ == '__main__':
    sys.exit(main())

