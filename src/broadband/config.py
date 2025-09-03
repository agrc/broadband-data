"""
config.py: Configuration values. Secrets to be handled with Secrets Manager
"""

import logging
import socket

SKID_NAME = "broadband-data"

AGOL_ORG = "https://utahbroadbandctr.maps.arcgis.com"
SENDGRID_SETTINGS = {  #: Settings for SendGridHandler
    "from_address": "noreply@utah.gov",
    "to_addresses": "jdadams@utah.gov",
    "prefix": f"{SKID_NAME} on {socket.gethostname()}: ",
}
LOG_LEVEL = logging.DEBUG
LOG_FILE_NAME = "log"

#: Hexes from SGID on AGOL
HEXES_LEVEL_6_ITEMID = "992228ada5cd4016b5f9c4d68751d1ca"
HEXES_LEVEL_7_ITEMID = "3dcd072755cd453885bda7dc69a82fef"
HEXES_LEVEL_8_ITEMID = "008865ed31514569a9fd11a6dc8a7ee0"

SERVICE_HEXES_6_ITEMID = "57a781feba684fe4a8410bf1e8e61d4f"
SERVICE_HEXES_7_ITEMID = "834f5cc5d22249f2a6dcf8636a9b8d1b"
SERVICE_HEXES_8_ITEMID = "25c23ead840e492a97dac6e117df72d8"
SERVICE_RECORDS_ITEMID = "a13eebdb3676417e94569b1483f0c6b5"
