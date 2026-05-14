from dagster import asset, AssetExecutionContext, Failure
import pandas as pd
from tedisc_dagster.defs import constants

import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, date
from tedisc_dagster.defs.utils import create_absence_records, assign_data_type

# ───────────────────────────────────────────────────────────────────────────────────────────────────
# BirdLife Australia 
# ───────────────────────────────────────────────────────────────────────────────────────────────────

load_dotenv()
filepath = os.environ.get("BLA_FILEPATH")

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

    df = raw_obs_bla.copy()

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
    df = deduplicated_bla.copy()

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
    df["FLAG_INDIVIDUAL_COUNT_EXTREME"] = df["individual_count"] > 10000  # TBD. Shorebird counts can sometimes be in the thousands

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



@asset(group_name="BirdLife_Australia")
def typed_bla(context: AssetExecutionContext, flagged_data_bla: pd.DataFrame) -> pd.DataFrame:
    """
    Assigns data_type (AA, PA, PO) to each BirdLife Australia observation.

    Pre-processing steps before type assignment:
    - Removes Beach-washed Birds records entirely (dead birds; not for modelling)
    - Standardises all_species_recorded (NA → FALSE)
    - Standardises all_shorebirds_visible_counted (NA → FALSE within Shorebirds;
      NA outside Shorebirds)
    - Creates is_shorebird flag (used in absence padding)

    Data type assignment is performed by assign_data_type(). See that function
    in utils.py for full logic documentation.

    PO_FALLBACK records are retained in the output but logged as warnings.
    They should be investigated before use in modelling - they indicate a
    program/survey_type combination not covered by explicit rules.
    """

    df = flagged_data_bla.copy()

    # Remove Beach-washed Birds 
    n_beachwashed = (df["program_name"] == "Beach-washed Birds").sum()
    if n_beachwashed:
        context.log.info(
            f"Removing {n_beachwashed} Beach-washed Birds records "
            f"(dead bird records; not relevant to species distribution modelling)"
        )
        df = df[df["program_name"] != "Beach-washed Birds"].copy()

    # Standardise all_species_recorded 
    df["all_species_recorded"] = df["all_species_recorded"].fillna(False).astype(bool)

    # Standardise all_shorebirds_visible_counted 
    df.loc[df["program_name"] == "Shorebirds", "all_shorebirds_visible_counted"] = (
        df.loc[df["program_name"] == "Shorebirds", "all_shorebirds_visible_counted"]
        .fillna(False)
    )
    df.loc[df["program_name"] != "Shorebirds", "all_shorebirds_visible_counted"] = pd.NA

    # Create is_shorebird flag
    df["is_shorebird"] = df["common_name"].isin(constants.BLA_SHOREBIRD_TARGET_SPECIES)

    # Mark all original records as non-absences 
    # Absence records added in padded_bla will have is_absence=True.
    # Setting this here ensures the column exists and is typed correctly
    # before padding, and makes the distinction explicit throughout the pipeline.
    df["is_absence"] = False

    # Assign data types
    df["data_type"] = df.apply(assign_data_type, axis=1)

    # Log data type summary 
    for dtype, count in df["data_type"].value_counts().items():
        context.log.info(f"data_type={dtype}: {count} records")

    # Warn on fallbacks
    fallback_mask = df["data_type"] == "PO_FALLBACK"
    n_fallback = fallback_mask.sum()
    if n_fallback:
        affected = (
            df.loc[fallback_mask, ["program_name", "survey_type"]]
            .drop_duplicates()
            .to_dict("records")
        )
        context.log.warning(
            f"{n_fallback} records hit PO_FALLBACK (unrecognised survey type). "
            f"Affected program/survey_type combinations: {affected}"
        )
    else:
        context.log.info("No PO_FALLBACK records — all combinations handled by explicit rules.")

    return df


# TODO:
# Create asset that assigns the levels of severity to the flags (dependent on data type)
# Refer to BirdLife Aus summary csv for flag levels 
# @asset(group_name="BirdLife_Australia")
# def flag_leveled_bla(context: AssetExecutionContext, typed_bla: pd.DataFrame) -> pd.DataFrame:




@asset(group_name="BirdLife_Australia")
def padded_bla(context: AssetExecutionContext, typed_bla: pd.DataFrame) -> pd.DataFrame: # Will need to change input to flag_leveled_bla once written
    """
    Pads the dataset with inferred absence records for target species
    not observed during qualifying surveys.

    Two padding conditions:

    Condition 1 — All species padding:
        Surveys where:
        - all_species_recorded = TRUE
        - survey_type is in SYSTEMATIC_SURVEY_TYPES
        - program is not in PO_PROGRAMS
        Absence records created for ALL target species not observed.

    Condition 2 — Shorebirds only padding:
        Surveys where:
        - program = "Shorebirds"
        - all_shorebirds_visible_counted = TRUE
        - all_species_recorded = FALSE (prevents overlap with condition 1)
        Absence records created for SHOREBIRD target species only.

    PO_FALLBACK records are excluded from padding — unrecognised survey types
    cannot be assumed to represent complete checklists.

    Absence records are distinguished from observations via 'is_absence' (bool)
    rather than a sentinel value in individual_count. individual_count remains
    NaN for absence records, preserving numeric typing throughout.

    A 'padding_condition' column is added to all records for traceability:
    - None: original observation record
    - 'all_species': absence record added under condition 1
    - 'shorebirds_only': absence record added under condition 2
    """

    df = typed_bla.copy()
    df["padding_condition"] = None

    # Exclude PO_FALLBACK records from padding eligibility
    eligible = df[df["data_type"] != "PO_FALLBACK"]

    # Condition 1: All species surveys 
    all_species_survey_ids = eligible.loc[
        (eligible["all_species_recorded"] == True) &
        (eligible["survey_type"].isin(constants.BLA_SYSTEMATIC_SURVEY_TYPES)) &
        (~eligible["program_name"].isin(constants.BLA_PO_PROGRAMS)),
        "survey_id"
    ].unique()

    # Condition 2: Shorebirds only surveys
    shorebird_only_survey_ids = eligible.loc[
        (eligible["all_shorebirds_visible_counted"] == True) &
        (eligible["all_species_recorded"] == False) &
        (eligible["program_name"] == "Shorebirds"),
        "survey_id"
    ].unique()

    context.log.info(f"Surveys qualifying for all-species padding: {len(all_species_survey_ids)}")
    context.log.info(f"Surveys qualifying for shorebird-only padding: {len(shorebird_only_survey_ids)}")

    # Generate absence records 
    all_absence_records = []

    for survey_id_val in all_species_survey_ids:
        absence_df = create_absence_records(df, survey_id_val, constants.BLA_ALL_TARGET_SPECIES)
        if absence_df is not None:
            absence_df["padding_condition"] = "all_species"
            all_absence_records.append(absence_df)

    for survey_id_val in shorebird_only_survey_ids:
        absence_df = create_absence_records(df, survey_id_val, constants.BLA_SHOREBIRD_SPECIES_FOR_PADDING)
        if absence_df is not None:
            absence_df["padding_condition"] = "shorebirds_only"
            all_absence_records.append(absence_df)

    # Reconciliation check
    # For every all-species qualifying survey, verify all target species are
    # now accounted for (observed or as an absence record). Flags surveys where
    # species name mismatches in SPECIES_DICT may have caused silent failures.
    if all_absence_records:
        absence_combined = pd.concat(all_absence_records, ignore_index=True)
        absence_lookup = (
            absence_combined.groupby("survey_id")["common_name"]
            .apply(set)
            .to_dict()
        )
    else:
        absence_combined = pd.DataFrame()
        absence_lookup = {}

    observed_lookup = (
        df.groupby("survey_id")["common_name"]
        .apply(lambda x: set(x.dropna()))
        .to_dict()
    )

    for survey_id_val in all_species_survey_ids:
        observed = observed_lookup.get(survey_id_val, set())
        absent = absence_lookup.get(survey_id_val, set())
        covered = observed | absent

        expected = set(constants.BLA_ALL_TARGET_SPECIES)
        for group in constants.BLA_AMBIGUOUS_SPECIES_GROUPS:
            if observed & group:
                expected -= group

        missing = expected - covered
        if missing:
            context.log.warning(
                f"Survey {survey_id_val}: absence padding incomplete — "
                f"species not covered: {missing}. "
                f"Check for name mismatches in SPECIES_DICT."
            )

    # Combine and return
    if not absence_combined.empty:
        padded = pd.concat([df, absence_combined], ignore_index=True)
        padded = padded.sort_values(["survey_id", "common_name"]).reset_index(drop=True)
        context.log.info(f"Original records: {len(df)}")
        context.log.info(f"Absence records added: {len(absence_combined)}")
        context.log.info(f"Total records after padding: {len(padded)}")
    else:
        context.log.info("No absence records generated.")
        padded = df

    return padded


@asset(
    group_name="BirdLife_Australia",
    io_manager_key="bcp_io_manager", # envokes an io manager to handle whatever the output of the asset is
    metadata={
        "schema": "dbo",
        "table": "bla_processed_test",
        "asset_schema": constants.BLA_ASSET_SCHEMA,
    },
)
def exported_bla(context: AssetExecutionContext, padded_bla: pd.DataFrame) -> pd.DataFrame:
    """
    Bulk-loads the processed BirdLife Australia dataset into MSSQL
    using the BCP utility via dagster-mssql-bcp.
 
    The table will be created automatically if it does not exist.
    """
    return padded_bla