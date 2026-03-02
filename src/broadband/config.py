"""
config.py: Configuration values. Secrets to be handled with Secrets Manager
"""

import logging
import socket
import urllib.request

SKID_NAME = "broadband-data"

try:
    url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
    req = urllib.request.Request(url)
    req.add_header("Metadata-Flavor", "Google")
    with urllib.request.urlopen(req, timeout=15) as response:
        project_id = response.read().decode()
        if not project_id:
            raise ValueError
        HOST_NAME = project_id
except Exception:
    HOST_NAME = socket.gethostname()


AGOL_ORG = "https://utahbroadbandctr.maps.arcgis.com"
SENDGRID_SETTINGS = {  #: Settings for SendGridHandler
    "from_address": "noreply@utah.gov",
    "to_addresses": "ugrc-developers@utah.gov",
    "prefix": f"{SKID_NAME} on {HOST_NAME}: ",
}
LOG_LEVEL = logging.INFO
LOG_FILE_NAME = "log"

#: Hexes from OpenSGID
HEXES_LEVEL_6_LAYER = "indices.h3_hexes_level_6"
HEXES_LEVEL_7_LAYER = "indices.h3_hexes_level_7"
HEXES_LEVEL_8_LAYER = "indices.h3_hexes_level_8"

SERVICE_HEXES_6_ITEMID = "57a781feba684fe4a8410bf1e8e61d4f"
SERVICE_HEXES_7_ITEMID = "834f5cc5d22249f2a6dcf8636a9b8d1b"
SERVICE_HEXES_8_ITEMID = "25c23ead840e492a97dac6e117df72d8"
SERVICE_RECORDS_ITEMID = "a13eebdb3676417e94569b1483f0c6b5"
