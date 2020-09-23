# Standard imports
from logging.handlers import RotatingFileHandler
import logging
import datetime as dt
import tempfile
from pkg_resources import resource_filename
import gzip
import io

# Misc
MAX_RECENT_PROJECTS = 5
MIN_COMPLETION_LETTERS = 1
DEFAULT_SELECTION_NAME = "variants"


# Classification

CLASSIFICATION = {
    0: "Unclassified",
    1: "Benin",
    2: "Likely benin",
    3: "Variant of uncertain significance",
    4: "Likely pathogen",
    5: "Pathogen",
}

CLASSIFICATION_ICONS = {
    0: 0xF03A1,
    1: 0xF03A4,
    2: 0xF03A7,
    3: 0xF03AA,
    4: 0xF03AD,
    5: 0xF03B1,
}


# Paths
DIR_LOGS = tempfile.gettempdir() + "/"

DIR_ASSETS = resource_filename(__name__, "assets/")  # current package name
DIR_TRANSLATIONS = DIR_ASSETS + "i18n/"
DIR_FONTS = DIR_ASSETS + "fonts/"
DIR_ICONS = DIR_ASSETS + "icons/"
DIR_STYLES = DIR_ASSETS + "styles/"

GENOTYPE_ICONS = {0: 0xF130, 1: 0xFAA0, 2: 0xFAA4, -1: 0xF625}


# Websites and variant query
WEBSITES_URLS = {"varsome": "https://varsome.com/variant/hg19/{chr}-{pos}-{ref}-{alt}"}


# Logging
LOGGER_NAME = "cutevariant"
LOG_LEVEL = "DEBUG"
LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "notset": logging.NOTSET,
}

################################################################################


def logger(name=LOGGER_NAME, logfilename=None):
    """Return logger of given name, without initialize it.

    Equivalent of logging.getLogger() call.
    """
    logger = logging.getLogger(name)
    FORMAT = "[%(filename)s:%(lineno)s:%(funcName)s() ]\n %(message)s"
    logging.basicConfig(format=FORMAT)
    return logger


_logger = logging.getLogger(LOGGER_NAME)
_logger.setLevel(LOG_LEVEL)

# log file
formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: %(message)s")
file_handler = RotatingFileHandler(
    DIR_LOGS
    + LOGGER_NAME
    + "_"
    + dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    + ".log",
    "a",
    100_000_000,
    1,
)
file_handler.setLevel(LOG_LEVEL)
file_handler.setFormatter(formatter)
_logger.addHandler(file_handler)

# terminal log
stream_handler = logging.StreamHandler()
formatter = logging.Formatter("%(levelname)s: %(message)s")
stream_handler.setFormatter(formatter)
stream_handler.setLevel(LOG_LEVEL)
_logger.addHandler(stream_handler)


def log_level(level):
    """Set terminal/file log level to given one.
    .. note:: Don't forget the propagation system of messages:
        From logger to handlers. Handlers receive log messages only if
        the main logger doesn't filter them.
    """
    # Main logger
    _logger.setLevel(level.upper())
    # Handlers
    [
        handler.setLevel(level.upper())
        for handler in _logger.handlers
        if handler.__class__
        in (logging.StreamHandler, logging.handlers.RotatingFileHandler)
    ]


def is_gz_file(filepath):
    """Return a boolean according to the compression state of the file"""
    with open(filepath, "rb") as test_f:
        return test_f.read(3) == b"\x1f\x8b\x08"


def get_uncompressed_size(filepath):
    """Get the size of the given compressed file
    This size is stored in the last 4 bytes of the file.
    """
    with gzip.open(filepath, "rb") as file_obj:
        return file_obj.seek(0, io.SEEK_END)
