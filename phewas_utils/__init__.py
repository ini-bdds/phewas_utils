import csv
from phewas_utils import phewas_utils_api as api

METADATA_FIELDS = ["batch_id", "subject_id", "file_id", "metadata"]

METRICS_FIELDS = ["batch_id", "subject_id", "file_id", "atlas", "region", "num_vert", "surf_area", "thick_avg",
                  "thick_std", "mean_curv", "gaus_curv", "fold_ind", "curv_ind", "volume"]

IMAGE_FIELDS = ["id", "batch_id", "subject_id ", "filename", "filepath", "uri", "content_type", "bytes", "preview",
                "md5sum", "sha1sum", "sha256sum", "sha512sum"],


def get_named_exception(e):
    exc = "".join(("[", type(e).__name__, "] "))
    return "".join((exc, str(e)))

csv.register_dialect('simplecsv', delimiter=',', lineterminator='\n', quoting=csv.QUOTE_MINIMAL)