#!/usr/bin/env python
# * coding: utf8 *
"""
Run the broadband-data skid as a cloud function.
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import arcgis
import geopandas as gpd
import pandas as pd
from palletjack import extract, load, transform, utils
from supervisor.message_handlers import SendGridHandler
from supervisor.models import MessageDetails, Supervisor

#: This makes it work when calling with just `python <file>`/installing via pip and in the gcf framework, where
#: the relative imports fail because of how it's calling the function.
try:
    from . import config, version
except ImportError:
    import config
    import version


class Skid:
    def __init__(self):
        self.secrets = SimpleNamespace(**self._get_secrets())
        self.tempdir = TemporaryDirectory(ignore_cleanup_errors=True)
        self.tempdir_path = Path(self.tempdir.name)
        self.log_name = f"{config.LOG_FILE_NAME}_{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
        self.log_path = self.tempdir_path / self.log_name
        self._initialize_supervisor()
        self.skid_logger = logging.getLogger(config.SKID_NAME)

    def __del__(self):
        self.tempdir.cleanup()

    @staticmethod
    def _get_secrets():
        """A helper method for loading secrets from either a GCF mount point or the local src/skidname/secrets/secrets.json file

        Raises:
            FileNotFoundError: If the secrets file can't be found.

        Returns:
            dict: The secrets .json loaded as a dictionary
        """

        secret_folder = Path("/secrets")

        #: Try to get the secrets from the Cloud Function mount point
        if secret_folder.exists():
            return json.loads(Path("/secrets/app/secrets.json").read_text(encoding="utf-8"))

        #: Otherwise, try to load a local copy for local development
        secret_folder = Path(__file__).parent / "secrets"
        if secret_folder.exists():
            return json.loads((secret_folder / "secrets.json").read_text(encoding="utf-8"))

        raise FileNotFoundError("Secrets folder not found; secrets not loaded.")

    def _initialize_supervisor(self):
        """A helper method to set up logging and supervisor

        Args:
            log_path (Path): File path for the logfile to be written
            sendgrid_api_key (str): The API key for sendgrid for this particular application

        Returns:
            Supervisor: The supervisor object used for sending messages
        """

        skid_logger = logging.getLogger(config.SKID_NAME)
        skid_logger.setLevel(config.LOG_LEVEL)
        palletjack_logger = logging.getLogger("palletjack")
        palletjack_logger.setLevel(config.LOG_LEVEL)

        cli_handler = logging.StreamHandler(sys.stdout)
        cli_handler.setLevel(config.LOG_LEVEL)
        formatter = logging.Formatter(
            fmt="%(levelname)-7s %(asctime)s %(name)15s:%(lineno)5s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        cli_handler.setFormatter(formatter)

        log_handler = logging.FileHandler(self.log_path, mode="w")
        log_handler.setLevel(config.LOG_LEVEL)
        log_handler.setFormatter(formatter)

        skid_logger.addHandler(cli_handler)
        skid_logger.addHandler(log_handler)
        palletjack_logger.addHandler(cli_handler)
        palletjack_logger.addHandler(log_handler)

        #: Log any warnings at logging.WARNING
        #: Put after everything else to prevent creating a duplicate, default formatter
        #: (all log messages were duplicated if put at beginning)
        logging.captureWarnings(True)

        skid_logger.debug("Creating Supervisor object")
        self.supervisor = Supervisor(handle_errors=False)
        sendgrid_settings = config.SENDGRID_SETTINGS
        sendgrid_settings["api_key"] = self.secrets.SENDGRID_API_KEY
        self.supervisor.add_message_handler(
            SendGridHandler(
                sendgrid_settings=sendgrid_settings, client_name=config.SKID_NAME, client_version=version.__version__
            )
        )

    def _remove_log_file_handlers(self, log_name, loggers):
        """A helper function to remove the file handlers so the tempdir will close correctly

        Args:
            log_name (str): The logfiles filename
            loggers (List<str>): The loggers that are writing to log_name
        """

        for logger in loggers:
            for handler in logger.handlers:
                try:
                    if log_name in handler.stream.name:
                        logger.removeHandler(handler)
                        handler.close()
                except Exception:
                    pass

    def process(self):
        """The main function that does all the work."""

        #: Set up secrets, tempdir, supervisor, and logging
        start = datetime.now()

        #: Get our GIS object via the ArcGIS API for Python
        self.gis = arcgis.GIS(config.AGOL_ORG, self.secrets.AGOL_USER, self.secrets.AGOL_PASSWORD)

        end = datetime.now()

        summary_message = MessageDetails()
        summary_message.subject = f"{config.SKID_NAME} Update Summary"
        summary_rows = [
            f"{config.SKID_NAME} update {start.strftime('%Y-%m-%d')}",
            "=" * 20,
            "",
            f"Start time: {start.strftime('%H:%M:%S')}",
            f"End time: {end.strftime('%H:%M:%S')}",
            f"Duration: {str(end - start)}",
            #: Add other rows here containing summary info captured/calculated during the working portion of the skid,
            #: like the number of rows updated or the number of successful attachment overwrites.
        ]

        summary_message.message = "\n".join(summary_rows)
        summary_message.attachments = self.tempdir_path / self.log_name

        self.supervisor.notify(summary_message)

        #: Remove file handler so the tempdir will close properly
        loggers = [logging.getLogger(config.SKID_NAME), logging.getLogger("palletjack")]
        self._remove_log_file_handlers(log_name, loggers)  #: FIXME


def entry():
    skid = Skid()
    skid.process()


#: Putting this here means you can call the file via `python main.py` and it will run. Useful for pre-GCF testing.
if __name__ == "__main__":
    skid = Skid()
    skid.process()
