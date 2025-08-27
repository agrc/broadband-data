#!/usr/bin/env python
# * coding: utf8 *
"""
Run the broadband-data skid as a cloud function.
"""

import json
import logging
import re
import sys
import time
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Literal

import arcgis
import geopandas as gpd
import pandas as pd
import requests
from palletjack import load
from supervisor.message_handlers import SendGridHandler
from supervisor.models import MessageDetails, Supervisor

#: This makes it work when calling with just `python <file>`/installing via pip and in the gcf framework, where
#: the relative imports fail because of how it's calling the function.
try:
    from . import config, utils, version
except ImportError:
    import config
    import utils
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

        self.skid_logger.info("Extracting BDC data...")
        utah_service_data = self._extract_bdc_data()

        self.skid_logger.info("Loading hexes from AGOL...")
        level_6_hexes = (
            self.gis.content.get(config.HEXES_LEVEL_6_ITEMID).layers[0].query(where="1=1", out_fields="hex_id").sdf
        )
        level_7_hexes = (
            self.gis.content.get(config.HEXES_LEVEL_7_ITEMID).layers[0].query(where="1=1", out_fields="hex_id").sdf
        )
        level_8_hexes = (
            self.gis.content.get(config.HEXES_LEVEL_8_ITEMID).layers[0].query(where="1=1", out_fields="hex_id").sdf
        )

        self.skid_logger.info("Creating service polygons at hex levels 6, 7, and 8...")
        service_level_6 = utils.create_service_polygons_at_hex_level(utah_service_data, 6, level_6_hexes)
        service_level_7 = utils.create_service_polygons_at_hex_level(utah_service_data, 7, level_7_hexes)
        service_level_8 = utils.create_service_polygons_at_hex_level(utah_service_data, 8, level_8_hexes)

        self.skid_logger.info("Creating max service table and hexes...")
        max_service_table = utils.max_service_by_hex_all_providers(utah_service_data)
        max_service_hexes = level_8_hexes[level_8_hexes["hex_id"].isin(max_service_table["h3_res8_id"])]

        self.skid_logger.info("Updating AGOL...")
        service_level_6_count = self._update_agol(service_level_6, config.SERVICE_HEXES_6_ITEMID, "layer", 0)
        service_level_7_count = self._update_agol(service_level_7, config.SERVICE_HEXES_7_ITEMID, "layer", 0)
        service_level_8_count = self._update_agol(service_level_8, config.SERVICE_HEXES_8_ITEMID, "layer", 0)

        #: The service records service has both a table and a layer
        max_service_count = self._update_agol(max_service_table, config.SERVICE_RECORDS_ITEMID, "table", 0)
        max_service_hex_count = self._update_agol(max_service_hexes, config.SERVICE_RECORDS_ITEMID, "layer", 0)

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
            "",
            f"Service areas at hex level 6: {service_level_6_count} features",
            f"Service areas at hex level 7: {service_level_7_count} features",
            f"Service areas at hex level 8: {service_level_8_count} features",
            f"Service record table: {max_service_count} records",
            f"Hexes for service records: {max_service_hex_count} features",
        ]

        summary_message.message = "\n".join(summary_rows)
        summary_message.attachments = self.tempdir_path / self.log_name

        self.supervisor.notify(summary_message)

        #: Remove file handler so the tempdir will close properly
        loggers = [logging.getLogger(config.SKID_NAME), logging.getLogger("palletjack")]
        self._remove_log_file_handlers(self.log_name, loggers)

    def _extract_bdc_data(self) -> pd.DataFrame:
        base_url = "https://bdc.fcc.gov/api/public/map"
        base_headers = {
            # 'user-agent': "vscode-restclient",
            "username": self.secrets.BDC_USERNAME,
            "hash_value": self.secrets.BDC_HASH,
        }

        #: Get the list of available dates
        dates_response = requests.request("GET", f"{base_url}/listAsOfDates", headers=base_headers)
        response_list = dates_response.json()["data"]
        available_dates = [entry["as_of_date"] for entry in response_list if entry["data_type"] == "availability"]
        available_dates.sort(reverse=True)

        #: Use the latest date to get a list of available files for Utah
        params = {
            "category": "State",
            "technology_type": "Fixed Broadband",  #: only doing fixed, not worried about mobile data/voice
        }
        download_list_response = requests.request(
            "GET",
            f"{base_url}/downloads/listAvailabilityData/{available_dates[0]}",
            headers=base_headers,
            params=params,
        )
        available_files_df = pd.DataFrame.from_records(download_list_response.json()["data"])
        utah_files = available_files_df[(available_files_df["state_name"] == "Utah")]

        #: Use the file list to extract Utah provider data into a single dataframe
        all_data_df = self._download_and_concat_provider_files(utah_files, base_url, base_headers)

        #: Add h3, common tech, and category columns
        all_data_df["h3_res6_id"] = all_data_df.apply(lambda row: utils.h3_to_parent(row["h3_res8_id"], 6), axis=1)
        all_data_df["h3_res7_id"] = all_data_df.apply(lambda row: utils.h3_to_parent(row["h3_res8_id"], 7), axis=1)

        all_data_df = utils.classify_common_tech(all_data_df)
        all_data_df = utils.categorize_service(all_data_df)

        return all_data_df

    def _download_and_concat_provider_files(
        self, files_df: pd.DataFrame, base_url: str, base_headers: dict[str, str]
    ) -> pd.DataFrame:
        #: Download, extract, load to dataframe, and concat all the provider data for utah

        #: Extract the file name (w/o trailing .zip), which is in the Content-Disposition header as 'attachment; filename="filename.csv.zip"'
        filename_pattern = re.compile(r"attachment; filename=(?:\"|')(.*)\.zip")

        all_df = pd.DataFrame()

        for i, file_data in enumerate(files_df[["technology_code_desc", "file_id"]].itertuples(index=False)):
            if (i + 1) % 10 == 0:  #: +1 so that we don't sleep on the first iteration
                self.skid_logger.info("Sleeping for 45 seconds to avoid API rate limiting...")
                time.sleep(45)

            technology, file_id = file_data
            self.skid_logger.debug("Downloading file_id: %s (%s)", file_id, technology)
            download_response = requests.request(
                "GET", f"{base_url}/downloads/downloadFile/availability/{file_id}", headers=base_headers
            )

            if download_response.status_code != 200:
                raise ValueError(
                    f"Failed to download {file_id}: {download_response.status_code} - {download_response.text}"
                )

            match = filename_pattern.search(download_response.headers["Content-Disposition"])
            if not match:
                raise ValueError(
                    f"Failed to extract filename from Content-Disposition header: {download_response.headers}"
                )

            filename = match.group(1)  #: regex group is the csv file name w/o .zip
            with zipfile.ZipFile(BytesIO(download_response.content)) as zf:  #: pass the response as a BytesIO object
                with zf.open(filename) as f:  #: Open the csv inside the zip archive
                    df = pd.read_csv(f)  #: Because f is a file-like object, we can read it like an on-disk file
                    df["technology_name"] = technology
            all_df = pd.concat([all_df, df], ignore_index=True)

        return all_df

    def _update_agol(
        self,
        data: pd.DataFrame | gpd.GeoDataFrame,
        layer_itemid: str,
        service_type: Literal["layer", "table"],
        index: int,
    ) -> int:
        loader = load.ServiceUpdater(self.gis, layer_itemid, service_type, index, self.tempdir_path)
        records_loaded = loader.truncate_and_load(data)

        return records_loaded


def entry():
    skid = Skid()
    skid.process()


#: Putting this here means you can call the file via `python main.py` and it will run. Useful for pre-GCF testing.
if __name__ == "__main__":
    skid = Skid()
    skid.process()
