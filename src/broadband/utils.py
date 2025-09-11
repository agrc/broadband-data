import geopandas as gpd
import h3.api.numpy_int as h3
import numpy as np
import pandas as pd
from palletjack import utils as pjutils


def create_service_polygons_at_hex_level(
    service_records: pd.DataFrame, hex_level: int, hex_polygons: pd.DataFrame
) -> gpd.GeoDataFrame:
    """Creates polygons representing service by provider, speeds, and technology at a given H3 hex level.

    Args:
        service_records (pd.DataFrame): All the service records to condense
        hex_level (int): Desired H3 hex level
        hex_polygons (pd.DataFrame): A spatially-enabled dataframe of H3 polygons at the desired level

    Returns:
        gpd.GeoDataFrame: Representation of what service is available where by provider, speeds, and technology
    """

    hex_id_field = f"h3_res{hex_level}_id"
    service_at_level = service_by_hex_level(service_records, hex_id_field, hex_polygons)
    service_gdf = pjutils.convert_to_gdf(service_at_level)
    service_dissolved = service_gdf.dissolve(
        by=[
            "technology_name",
            "common_tech",
            "brand_name",
            "max_advertised_download_speed",
            "max_advertised_upload_speed",
        ]
    )
    service_dissolved = categorize_service(service_dissolved.reset_index())

    return service_dissolved.drop(columns=["OBJECTID", "hex_id", hex_id_field])


def classify_common_tech(service_data_df: pd.DataFrame) -> pd.DataFrame:
    """Create a commonly-used technology name based on the FCC technology_name field

    Args:
        service_data_df (pd.DataFrame): Contains FCC service availability records (must have technology_name field)

    Returns:
        pd.DataFrame: Input dataframe with an added common_tech field
    """

    conditions = [
        service_data_df["technology_name"] == "Cable",
        service_data_df["technology_name"] == "Copper",
        service_data_df["technology_name"] == "Fiber to the Premises",
        service_data_df["technology_name"].isin(
            ["LBR Fixed Wireless", "Licensed Fixed Wireless", "Unlicensed Fixed Wireless"]
        ),
        service_data_df["technology_name"].isin(["GSO Satellite", "NGSO Satellite"]),
    ]
    tech_choices = [
        "Cable",
        "DSL",
        "Fiber",
        "Fixed Wireless",
        "Satellite",
    ]

    service_data_df["common_tech"] = np.select(conditions, tech_choices, "Other Tech")

    return service_data_df


def categorize_service(service_data_df: pd.DataFrame) -> pd.DataFrame | gpd.GeoDataFrame:
    """Categorize service records as either wired, wireless, or satellite based on common_tech field

    Args:
        service_data_df (pd.DataFrame): FCC service availability records with common_tech field added

    Returns:
        pd.DataFrame | gpd.GeoDataFrame: Input dataframe with an added category field
    """

    conditions = [
        service_data_df["common_tech"].isin(["Cable", "DSL", "Fiber"]),
        service_data_df["common_tech"] == "Fixed Wireless",
        service_data_df["common_tech"] == "Satellite",
    ]

    choices = ["wired", "wireless", "satellite"]

    service_data_df["category"] = np.select(conditions, choices, "Other Category")

    return service_data_df


def h3_to_parent(h3_str: str, parent_level: int) -> str:
    """Calculate the parent hex ID at a given level from a child hex ID

    Args:
        h3_str (str): Input H3 hex ID
        parent_level (int): Desired parent level

    Returns:
        str: Parent hex ID at the desired level
    """

    return h3.h3_to_string(h3.h3_to_parent(h3.string_to_h3(h3_str), parent_level))


def service_by_hex_level(all_records: pd.DataFrame, hex_id_field: str, hexes_df: pd.DataFrame) -> pd.DataFrame:
    """Groups residential service records by hex ID, technology, provider, and max up/down speeds

    Args:
        all_records (pd.DataFrame): All service records
        hex_id_field (str): Index field for hex ID
        hexes_df (pd.DataFrame): Spatially-enabled dataframe of the desired hex level to join the records to

    Returns:
        pd.DataFrame: Spatially-enabled dataframe of service records summarized by hex/tech/provider with max up/down speeds. Only hexes with service are included.
    """

    #: Calc max up/down speeds per hex/tech/provider
    residential_only = all_records[all_records["business_residential_code"].isin(["R", "X"])]
    individual_records_down = residential_only.groupby([hex_id_field, "technology_name", "brand_name", "common_tech"])[
        "max_advertised_download_speed"
    ].max()
    individual_records_up = residential_only.groupby([hex_id_field, "technology_name", "brand_name", "common_tech"])[
        "max_advertised_upload_speed"
    ].max()
    individual_records = pd.concat([individual_records_down, individual_records_up], axis=1).reset_index()

    #: Get the speeds as ints
    individual_records["max_advertised_download_speed"] = individual_records["max_advertised_download_speed"].astype(
        int
    )
    individual_records["max_advertised_upload_speed"] = individual_records["max_advertised_upload_speed"].astype(int)

    #: Merge with hexes, only keeping hexes that have service
    all_record_hexes_service = hexes_df.merge(individual_records, left_on="hex_id", right_on=hex_id_field, how="right")

    return all_record_hexes_service


def max_service_by_hex_all_providers(service_records: pd.DataFrame) -> pd.DataFrame:
    """Get a table of the max up/down speeds by hex/provider/tech for residential service records.

    This allows a relationship with the hex geometry layer so a user can click on a hex and see max advertised speeds by provider/tech.

    Args:
        service_records (pd.DataFrame): All service records

    Returns:
        pd.DataFrame: Service records aggregated by hex/provider/tech with max up/down speeds
    """

    res_only = service_records[service_records["business_residential_code"].isin(["R", "X"])]

    maxes = (
        res_only.groupby(["h3_res8_id", "brand_name", "common_tech", "category"])[
            ["max_advertised_download_speed", "max_advertised_upload_speed"]
        ]
        .agg("max")
        .reset_index()
    )

    #: Fix types for AGOL
    field_map = {
        "max_advertised_download_speed": "int16",
        "max_advertised_upload_speed": "int16",
    }
    for field, dtype in field_map.items():
        if field in maxes.columns:
            maxes[field] = maxes[field].astype(dtype)

    #: Clean up fields, provider names
    maxes.drop(
        columns=["frn", "provider_id", "location_id", "technology", "low_latency", "state_usps", "block_geoid"],
        inplace=True,
        errors="ignore",
    )
    maxes["brand_name"] = maxes["brand_name"].replace({"Utah Telecommunication Open Infrastructure Agency": "UTOPIA"})

    return maxes
