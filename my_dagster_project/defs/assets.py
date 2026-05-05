
from dagster import asset, AssetExecutionContext, Failure
import pandas as pd
from . import constants

import os
from pathlib import Path
from datetime import datetime, date
from pyinaturalist import get_observations


###### BirdLife Australia QAQC ########## 

filepath = "/home/eloisewm/Datasets/BirdLife Aus/bla_output_csiro_290525.csv"

@asset(
        group_name = "BirdLife_Australia", 
        metadata = {
           "source": "Birdlife Australia",
           "license": "CC-BY 4.0"}
)
def raw_obs_bla(context: AssetExecutionContext) -> pd.DataFrame:
    """Load raw BirdLife Australia observations from CSV."""

    # Load in data as a dataframe
    df = pd.read_csv(filepath)

    # Record size of the file and the date last modified
    path = Path(filepath)
    file_size_mb = os.path.getsize(path) / (1024 * 1024) # in bytes, convert to mb
    last_modified = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%d/%m/%Y")

    # Add metadata for this materialisation of the file
    context.add_output_metadata({
        "row_count": len(df),
        "column_count": len(df.columns),
        "filepath": filepath, 
        "file_size_mb": file_size_mb,
        "last_modified": last_modified
    })

    return df


@asset(group_name = "BirdLife_Australia")
def validated_structure_bla(context: AssetExecutionContext, raw_obs_bla: pd.DataFrame) -> pd.DataFrame:
    """
    Check that raw_obs has the expected column structure.
    Does not modify raw_obs - check only.
    Logs missing and additional columns for reference.
    """

    df = raw_obs_bla

    expected = set(constants.EXPECTED_BLA_COLUMNS)
    actual = set(df.columns)
    missing_cols = expected - actual
    additional_cols = actual - expected
 

    if missing_cols:
        raise Failure(description=f"MISSING COLUMNS ({len(missing_cols)}): {sorted(missing_cols)}")  
    else:
        context.log.info("No missing columns — all expected columns present.")

    if additional_cols:
        raise Failure(description=f"UNEXPECTED COLUMNS ({len(additional_cols)}): {sorted(additional_cols)}")

    else:
        context.log.info("No additional columns — file matches expected schema exactly.")

    return df


@asset(group_name = "BirdLife_Australia")
def deduplicated_bla(context: AssetExecutionContext, validated_structure_bla: pd.DataFrame) -> pd.DataFrame:
    """Removes duplicate rows from raw observations."""
    original_count = len(validated_structure_bla)
    df = validated_structure_bla.drop_duplicates()
    no_of_dupes = original_count - len(df)

    if no_of_dupes:
        context.log.info(f"{no_of_dupes} duplicate rows removed")
    else:
        context.log.info("No duplicate rows found")

    return df


@asset(group_name="BirdLife_Australia")
def flagged_data_bla(context: AssetExecutionContext, deduplicated_bla: pd.DataFrame) -> pd.DataFrame:
    """
    Flags all unusual entries for potential removal later on in the worklfow. 
    A new column for each flag type is created, and given a boolean True if that condition is met.
    """
    df = deduplicated_bla

    # SURVEY
    # Important for AA and PA datatypes
    df["FLAG_SURVEY_ID_BLANK"] = df["survey_id"].isna()
    df["FLAG_SURVEY_TYPE_BLANK"] = df["survey_type"].isna()
    df["FLAG_SURVEY_TYPE_UNEXPECTED"] = df["survey_type"].notna() & ~df["survey_type"].isin(constants.BLA_SURVEY_TYPES)


    # PROGRAM
    # Important for AA and PA datatypes
    df["FLAG_PROGRAM_NAME_BLANK"] = df["program_name"].isna()
    df["FLAG_PROGRAM_NAME_UNEXPECTED"] = df["program_name"].notna() & ~df["program_name"].isin(constants.BLA_PROGRAM_NAMES)

    # SPATIAL
    # Important for all datatypes 
    # Noting missing values only. I considered recording unexpected values, but there's a chance that BirdLife will at some point 
    # host non-australian obs, and in that case we'd want to keep everything 
    df["FLAG_LAT_BLANK"] = df["lat"].isna()
    df["FLAG_LON_BLANK"] = df["lon"].isna()

    # ACCURACY 
    # Important for all data types? Or not important at all? To discuss with Keith
    df["FLAG_ACCURACY_BLANK"] = df["accuracy_in_metres"].isna()
    df["FLAG_ACCURACY_ZERO"] = df["accuracy_in_metres"] == 0
    df["FLAG_ACCURACY_NEGATIVE"] = df["accuracy_in_metres"] < 0
    df["FLAG_ACCURACY_EXTREME"] = df["accuracy_in_metres"] > 10000  # TBD

    # TAXONOMY
    # Important for all datatypes
    # I will primarily refer to the scientific names during QAQC, but worth picking up issues with common names also
    df["FLAG_COMMON_NAME_BLANK"] = df["common_name"].isna()
    df["FLAG_COMMON_NAME_UNEXPECTED"] = df["common_name"].notna() & ~df["common_name"].isin(constants.BLA_EXPECTED_COMMON_NAMES)
    df["FLAG_SCIENTIFIC_NAME_BLANK"] = df["scientific_name"].isna()
    df["FLAG_SCIENTIFIC_NAME_UNEXPECTED"] = df["scientific_name"].notna() & ~df["scientific_name"].isin(constants.BLA_EXPECTED_SCIENTIFIC_NAMES)

    # COUNT 
    # Important for AA and PA data types (might also be used for PO? Keith doesn't value counts when PO...)
    df["FLAG_INDIVIDUAL_COUNT_BLANK"] = df["individual_count"].isna()
    df["FLAG_INDIVIDUAL_COUNT_ZERO"] = df["individual_count"] == 0
    df["FLAG_INDIVIDUAL_COUNT_NEGATIVE"] = df["individual_count"] < 0
    df["FLAG_INDIVIDUAL_COUNT_EXTREME"] = df["individual_count"] > 1000  # TBD

    # DATE 
    # Important for all data types
    df["FLAG_DATE_BLANK"] = df["start_date"].isna() 

    # Check that date follows the pattern dd/mm/yyyy or d/mm/yyyy
    date_pattern = r'^\d{1,2}/\d{2}/\d{4}$'
    df["FLAG_START_DATE_FORMAT_UNEXPECTED"] = (
        df["start_date"].notna() & 
        ~df["start_date"].str.match(date_pattern)
    )
    
    # Flag if date is in the future
    today = pd.Timestamp(date.today())
    df["FLAG_DATE_IN_FUTURE"] = (
        df["start_date"].notna() & 
        ~df["FLAG_START_DATE_FORMAT_UNEXPECTED"] &  
        (pd.to_datetime(df["start_date"], errors="coerce") > today)  # brackets needed
    )
    
    # TIME
    # Important for some survey_types for AA and PA
    df["FLAG_START_TIME_BLANK"] = df["start_time"].isna()

    # Check that time follows the format HH:MM:SS or H:MM:SS
    time_pattern = r"^\d{1,2}:\d{2}:\d{2}$"
    df["FLAG_START_TIME_FORMAT_UNEXPECTED"] = (
        df["start_time"].notna() & 
        ~df["start_time"].str.match(time_pattern)
    )

    # ALL SPECIES RECORDED
    # Important to flag for when we infer absences
    df["FLAG_ALL_SPECIES_RECORDED_BLANK"] = df["all_species_recorded"].isna()
    df["FLAG_ALL_SPECIES_RECORDED_UNEXPECTED"] = df["all_species_recorded"].notna() & ~df["all_species_recorded"].isin([True, False])

    # ATTRIBUTES SPECIFIC TO THE "SHOREBIRDS" PROGRAM
    # Some of the columns are only pertinent to obsevrations recorded under the Shorebirds program. 
    # The important one to track is all_shorebirds_visible_counted
    df["FLAG_ALL_SHOREBIRDS_VISIBLE_BLANK_IN_SHOREBIRDS"] = (
        (df["program_name"] == "Shorebirds") & 
        (df["all_shorebirds_visible_counted"].isna())
    )


    # Log a summary of all flags
    flag_cols = [x for x in df.columns if x.startswith("FLAG_")]
    for col in flag_cols:
        count = df[col].sum()
        if count:
            context.log.info(f"{col}: {count} records flagged")

    return df
  


# Currently I run out of memory before this asset can execute
# Will need to either:
#   1) Save each species to disk as I go through them in the loop
#   2) Write an asset for each individual species 
# Decision will impact how I write future assets - must decide before moving on
@asset(group_name="iNaturalist")
def raw_obs_inat(context: AssetExecutionContext) -> pd.DataFrame:

    all_observations = []

    for taxon_name in constants.ALL_TARGET_SPECIES_SCIENTIFIC:

        # Count the number of obs that we are about to request
        count_response = get_observations(
            taxon_name=taxon_name,
            quality_grade="research",
            count_only=True, 
            nelat=0,     
            swlat=-90,    
            nelng=180,    
            swlng=-180,
        )
        total = count_response["total_results"]
        context.log.info(f"[{taxon_name}] Total observations to download: {total}")

        page = 1        # Will iterate in the loop over all possible pages
        per_page = 200  # Max we can do

        while True:
            # API pull for all reasearch grade observations in the southern hemiphere
            response = get_observations(
                taxon_name=taxon_name,
                quality_grade="research",
                per_page=per_page,
                page=page,
                nelat=0,      
                swlat=-90,    
                nelng=180,   
                swlng=-180,
            )

            results = response["results"]
            if not results:
                break

            all_observations.extend(results)
            context.log.info(f"[{taxon_name}] Page {page} - {len(all_observations)} total records so far")
            page += 1

    df = pd.json_normalize(all_observations)

    context.add_output_metadata({
        "row_count": len(df),
        "column_count": len(df.columns),
        "species_count": len(constants.ALL_TARGET_SPECIES_SCIENTIFIC),
        "quality_grade": "research",
    })

    return df
    



        


