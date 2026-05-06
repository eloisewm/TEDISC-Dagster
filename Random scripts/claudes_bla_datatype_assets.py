from dagster import asset, AssetExecutionContext
import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Programs that are always PO regardless of survey type or other fields.
# Membership here is based on program DESIGN ambiguity - specifically that
# counts in these programs may not represent total birds present at a site,
# making counts uninterpretable as abundance indices.
#
# Beach-nesting Birds: surveys only record nesting birds, not all birds present.
#   A count of 3 Hooded Plovers means 3 nesting birds, not 3 total birds at
#   the site. This ambiguity is not recoverable from the data fields alone.
#
# Colonial Nesting Birds: surveys may only count nesting/colonial birds and
#   could miss flying or non-nesting individuals. Same count ambiguity as above.
PO_PROGRAMS = {
    "Beach-nesting Birds",
    "Colonial Nesting Birds",
}

# Survey types that represent genuine standardised effort.
# AA/PA assignment is permitted for records with these survey types
# (subject to program membership and other field conditions).
SYSTEMATIC_SURVEY_TYPES = {
    "2ha, 20 minute search",
    "2ha, non-20 minute search",
    "500m area search",
    "5km area search",
    "5 minute point search",
    "Shorebird count",
    "Fixed route search",
    "Breeding territory monitoring",
}

# Survey types that are always PO regardless of program or other fields.
ALWAYS_PO_SURVEY_TYPES = {
    "Incidental search",
    "Bird list",
}

# Programs where all_species_recorded is forced to FALSE regardless of observer input.
# These are single-target programs where general absence inference is not valid,
# regardless of what the observer recorded in the all_species_recorded field.
FORCE_ALL_SPECIES_FALSE = {
    "Swift Parrot Search",
    "WA Black Cockies",
}

# Shorebird target species - used to set is_shorebird flag and for shorebird-only padding.
# Includes the alternate spelling present in the source data.
SHOREBIRD_TARGET_SPECIES = {
    "Curlew Sandpiper",
    "Red Knot",
    "Far Eastern Curlew",
    "Lesser Sand Plover",
    "Lesser Sandplover",
}

# All target species for absence padding (all-species condition)
ALL_TARGET_SPECIES = [
    "Curlew Sandpiper",
    "Red Knot",
    "Far Eastern Curlew",
    "Lesser Sand Plover",
    "Grey-Headed Albatross",
    "Northern Royal Albatross",
    "Orange-bellied Parrot",
    "Shy Albatross",
    "Shy/White-capped Albatross spp",
    "Southern Giant-Petrel",
    "Swift Parrot",
    "White-capped Albatross",
]

# Shorebird target species for shorebird-only absence padding (condition 2)
SHOREBIRD_SPECIES_FOR_PADDING = [
    "Curlew Sandpiper",
    "Red Knot",
    "Far Eastern Curlew",
    "Lesser Sand Plover",
]

# Ambiguous species groups - if any member is observed in a survey, all members
# are excluded from absence inference to avoid spurious absences.
AMBIGUOUS_SPECIES_GROUPS = [
    {"Shy Albatross", "White-capped Albatross", "Shy/White-capped Albatross spp"},
]

# Species taxonomy dictionary for constructing absence records
SPECIES_DICT = {
    "Curlew Sandpiper":               {"scientific_name": "Calidris ferruginea",      "taxon_id": "u161"},
    "Red Knot":                       {"scientific_name": "Calidris canutus",          "taxon_id": "164"},
    "Far Eastern Curlew":             {"scientific_name": "Numenius madagascariensis", "taxon_id": "u149"},
    "Lesser Sand Plover":             {"scientific_name": "Charadrius mongolus",       "taxon_id": "u90"},
    "Grey-Headed Albatross":          {"scientific_name": "Thalassarche chrysostoma",  "taxon_id": "139"},
    "Northern Royal Albatross":       {"scientific_name": "Diomedea sanfordi",         "taxon_id": "u973"},
    "Orange-bellied Parrot":          {"scientific_name": "Neophema chrysogaster",     "taxon_id": "u305"},
    "Shy Albatross":                  {"scientific_name": "Thalassarche cauta",        "taxon_id": "u91"},
    "Shy/White-capped Albatross spp": {"scientific_name": "",                          "taxon_id": "5072"},
    "Southern Giant-Petrel":          {"scientific_name": "Macronectes giganteus",     "taxon_id": "u929"},
    "Swift Parrot":                   {"scientific_name": "Lathamus discolor",         "taxon_id": "u309"},
    "White-capped Albatross":         {"scientific_name": "Thalassarche steadi",       "taxon_id": "u861"},
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: assign data type for a single row
# ─────────────────────────────────────────────────────────────────────────────

def _assign_data_type(row: pd.Series) -> str:
    """
    Assigns a data_type of AA, PA, PO, or PO_FALLBACK to a single observation.

    PO_FALLBACK is returned for any combination not explicitly handled - these
    are logged as warnings in typed_bla and should be investigated before use
    in modelling.

    Evaluation order:
    1. PO_PROGRAMS - always PO (program design makes counts uninterpretable)
    2. ALWAYS_PO_SURVEY_TYPES - always PO (incidental search, bird list)
    3. Shorebirds program - shorebird-specific logic using all_shorebirds_visible_counted
    4. Swift Parrot Search - single-target; AA if count given, PO otherwise
    5. All remaining programs with systematic survey types - standard logic
    6. PO_FALLBACK - unrecognised survey type, flagged for review

    FORCE_ALL_SPECIES_FALSE: Swift Parrot Search and WA Black Cockies have
    all_species_recorded treated as FALSE regardless of observer input. WA Black
    Cockies reaches step 5 with all_species_recorded=FALSE, so it will only
    reach AA (count given) or PO (no count) - never PA.
    """

    program = row.get("program_name", "")
    survey_type = row.get("survey_type", "")
    count_made = not pd.isna(row.get("individual_count"))
    is_shorebird = row.get("is_shorebird", False)

    # Resolve all_species_recorded - forced FALSE for single-target programs
    if program in FORCE_ALL_SPECIES_FALSE:
        all_species = False
    else:
        all_species = bool(row.get("all_species_recorded", False))

    all_shorebirds = bool(row.get("all_shorebirds_visible_counted", False))

    # ── 1. PO programs ────────────────────────────────────────────────────────
    if program in PO_PROGRAMS:
        return "PO"

    # ── 2. Always-PO survey types ─────────────────────────────────────────────
    if survey_type in ALWAYS_PO_SURVEY_TYPES:
        return "PO"

    # ── 3. Shorebirds program ─────────────────────────────────────────────────
    # Uses all_shorebirds_visible_counted as an additional completeness flag.
    # Logic differs for shorebird vs non-shorebird species within the same survey.
    if program == "Shorebirds":

        if is_shorebird:
            if count_made:
                return "AA"
            if all_shorebirds or all_species:
                return "PA"
            return "PO"

        else:
            # Non-shorebird species recorded within a Shorebirds survey.
            # all_shorebirds_visible_counted does not imply completeness for
            # non-shorebird species, so only all_species_recorded gates PA here.
            if count_made:
                return "AA"
            if all_species:
                return "PA"
            return "PO"

    # ── 4. Swift Parrot Search ────────────────────────────────────────────────
    # Single-target program. all_species_recorded forced to FALSE (step above),
    # so PA is unreachable. AA if count given, PO otherwise.
    if program == "Swift Parrot Search":
        if count_made:
            return "AA"
        return "PO"

    # ── 5. Standard systematic survey logic ───────────────────────────────────
    # Applies to all remaining programs with a recognised systematic survey type.
    # Covers: Bittern, Birds in Backyards (incidental caught at step 2),
    # Birds on Farms, General BirdData, Powerful Owl, Sydney Olympic Park,
    # WA Black Cockies, Wetland Birds, and any future programs.
    if survey_type in SYSTEMATIC_SURVEY_TYPES:
        if count_made:
            return "AA"
        if all_species:
            return "PA"
        return "PO"

    # ── 6. Fallback ───────────────────────────────────────────────────────────
    # Reached when survey_type is not in SYSTEMATIC_SURVEY_TYPES or
    # ALWAYS_PO_SURVEY_TYPES and the program is not otherwise handled above.
    # Indicates an unrecognised survey type - logged as warning in typed_bla.
    return "PO_FALLBACK"


# ─────────────────────────────────────────────────────────────────────────────
# ASSET 1: typed_bla
# ─────────────────────────────────────────────────────────────────────────────

@asset(group_name="BirdLife_Australia")
def typed_bla(context: AssetExecutionContext, flagged_data_bla: pd.DataFrame) -> pd.DataFrame:
    """
    Assigns data_type (AA, PA, PO) to each BirdLife Australia observation.

    Pre-processing steps before type assignment:
    - Removes Beach-washed Birds records entirely (dead birds; not for modelling)
    - Removes the single Far Eastern Curlew record in Swift Parrot Search
      (data entry error confirmed in project documentation)
    - Standardises all_species_recorded (NA → FALSE)
    - Standardises all_shorebirds_visible_counted (NA → FALSE within Shorebirds;
      NA outside Shorebirds)
    - Creates is_shorebird flag

    Data type assignment is performed by _assign_data_type(). See that function
    for full logic documentation.

    PO_FALLBACK records are retained in the output but logged as warnings.
    They should be investigated before use in modelling - they indicate a
    program/survey_type combination not covered by explicit rules.
    """

    df = flagged_data_bla.copy()

    # ── Remove Beach-washed Birds ─────────────────────────────────────────────
    n_beachwashed = (df["program_name"] == "Beach-washed Birds").sum()
    if n_beachwashed:
        context.log.info(
            f"Removing {n_beachwashed} Beach-washed Birds records "
            f"(dead bird records; not relevant to species distribution modelling)"
        )
        df = df[df["program_name"] != "Beach-washed Birds"].copy()

    # ── Remove known data entry error ─────────────────────────────────────────
    error_mask = (
        (df["program_name"].str.strip() == "Swift Parrot Search") &
        (df["common_name"].str.strip() == "Far Eastern Curlew")
    )
    n_error = error_mask.sum()
    if n_error:
        context.log.warning(
            f"Removing {n_error} Far Eastern Curlew record(s) assigned to "
            f"Swift Parrot Search (data entry error - confirmed in project documentation)"
        )
        df = df[~error_mask].copy()

    # ── Standardise all_species_recorded ──────────────────────────────────────
    df["all_species_recorded"] = df["all_species_recorded"].fillna(False).astype(bool)

    # ── Standardise all_shorebirds_visible_counted ────────────────────────────
    df.loc[df["program_name"] == "Shorebirds", "all_shorebirds_visible_counted"] = (
        df.loc[df["program_name"] == "Shorebirds", "all_shorebirds_visible_counted"]
        .fillna(False)
    )
    df.loc[df["program_name"] != "Shorebirds", "all_shorebirds_visible_counted"] = pd.NA

    # ── Create is_shorebird flag ──────────────────────────────────────────────
    df["is_shorebird"] = df["common_name"].isin(SHOREBIRD_TARGET_SPECIES)

    # ── Mark all original records as non-absences ────────────────────────────
    # Absence records added in padded_bla will have is_absence=True.
    # Setting this here ensures the column exists and is typed correctly
    # before padding, and makes the distinction explicit throughout the pipeline.
    df["is_absence"] = False

    # ── Assign data types ─────────────────────────────────────────────────────
    df["data_type"] = df.apply(_assign_data_type, axis=1)

    # ── Log data type summary ─────────────────────────────────────────────────
    for dtype, count in df["data_type"].value_counts().items():
        context.log.info(f"data_type={dtype}: {count} records")

    # ── Warn on fallbacks ─────────────────────────────────────────────────────
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
        context.log.info("No PO_FALLBACK records - all combinations handled by explicit rules.")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: create absence records for a single survey
# ─────────────────────────────────────────────────────────────────────────────

def _create_absence_records(
    survey_data: pd.DataFrame,
    survey_id_val: str,
    target_species: list[str],
) -> pd.DataFrame | None:
    """
    Creates absence records for all target species not observed in a given survey.

    Returns a DataFrame of absence records, or None if all target species
    were already observed (or excluded via ambiguous group logic).

    Ambiguous species groups: if any member of a group is observed, all members
    of that group are excluded from absence inference. This prevents spurious
    absences when an observer recorded a group-level sighting (e.g.
    Shy/White-capped Albatross spp) that implicitly covers individual species.

    Absence data_type:
    - PA if any PA records exist in the survey
    - AA otherwise
    Note: this can produce asymmetric cases where a survey has AA presences
    and PA absences (one species counted, another only detected without count).
    Carried forward from the original R code - verify with project lead if
    this asymmetry is acceptable for downstream modelling.

    Absence records have:
    - individual_count = NaN (no count; bird was not observed)
    - is_absence = True
    Original observation records have is_absence = False (set in typed_bla).
    """

    survey_rows = survey_data[survey_data["survey_id"] == survey_id_val]
    if survey_rows.empty:
        return None

    survey_info = survey_rows.iloc[0].copy()
    observed_species = set(survey_rows["common_name"].dropna().tolist())

    # Exclude ambiguous groups where any member was observed
    species_to_exclude: set[str] = set()
    for group in AMBIGUOUS_SPECIES_GROUPS:
        if observed_species & group:
            species_to_exclude |= group

    missing_species = [
        sp for sp in target_species
        if sp not in observed_species and sp not in species_to_exclude
    ]

    if not missing_species:
        return None

    # Determine absence data_type
    absence_data_type = "PA" if "PA" in survey_rows["data_type"].values else "AA"

    # Build absence records
    records = []
    for sp in missing_species:
        taxa = SPECIES_DICT.get(sp, {})
        row = survey_info.to_dict()
        row["sighting_id"] = None
        row["taxon_id"] = taxa.get("taxon_id")
        row["common_name"] = sp
        row["scientific_name"] = taxa.get("scientific_name", "")
        row["individual_count"] = None  # NaN - bird not observed, no count possible
        row["is_absence"] = True
        row["data_type"] = absence_data_type
        row["vetting_id"] = None
        row["vetting_status"] = None
        row["is_shorebird"] = sp in SHOREBIRD_TARGET_SPECIES
        records.append(row)

    return pd.DataFrame(records)


# ─────────────────────────────────────────────────────────────────────────────
# ASSET 2: padded_bla
# ─────────────────────────────────────────────────────────────────────────────

@asset(group_name="BirdLife_Australia")
def padded_bla(context: AssetExecutionContext, typed_bla: pd.DataFrame) -> pd.DataFrame:
    """
    Pads the dataset with inferred absence records for target species
    not observed during qualifying surveys.

    Two padding conditions:

    Condition 1 - All species padding:
        Surveys where:
        - all_species_recorded = TRUE
        - survey_type is in SYSTEMATIC_SURVEY_TYPES
        - program is not in PO_PROGRAMS
        Absence records created for ALL target species not observed.

    Condition 2 - Shorebirds only padding:
        Surveys where:
        - program = "Shorebirds"
        - all_shorebirds_visible_counted = TRUE
        - all_species_recorded = FALSE (prevents overlap with condition 1)
        Absence records created for SHOREBIRD target species only.

    PO_FALLBACK records are excluded from padding - unrecognised survey types
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

    # ── Condition 1: All species surveys ──────────────────────────────────────
    all_species_survey_ids = eligible.loc[
        (eligible["all_species_recorded"] == True) &
        (eligible["survey_type"].isin(SYSTEMATIC_SURVEY_TYPES)) &
        (~eligible["program_name"].isin(PO_PROGRAMS)),
        "survey_id"
    ].unique()

    # ── Condition 2: Shorebirds only surveys ──────────────────────────────────
    shorebird_only_survey_ids = eligible.loc[
        (eligible["all_shorebirds_visible_counted"] == True) &
        (eligible["all_species_recorded"] == False) &
        (eligible["program_name"] == "Shorebirds"),
        "survey_id"
    ].unique()

    context.log.info(f"Surveys qualifying for all-species padding: {len(all_species_survey_ids)}")
    context.log.info(f"Surveys qualifying for shorebird-only padding: {len(shorebird_only_survey_ids)}")

    # ── Generate absence records ───────────────────────────────────────────────
    all_absence_records = []

    for survey_id_val in all_species_survey_ids:
        absence_df = _create_absence_records(df, survey_id_val, ALL_TARGET_SPECIES)
        if absence_df is not None:
            absence_df["padding_condition"] = "all_species"
            all_absence_records.append(absence_df)

    for survey_id_val in shorebird_only_survey_ids:
        absence_df = _create_absence_records(df, survey_id_val, SHOREBIRD_SPECIES_FOR_PADDING)
        if absence_df is not None:
            absence_df["padding_condition"] = "shorebirds_only"
            all_absence_records.append(absence_df)

    # ── Reconciliation check ──────────────────────────────────────────────────
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

        expected = set(ALL_TARGET_SPECIES)
        for group in AMBIGUOUS_SPECIES_GROUPS:
            if observed & group:
                expected -= group

        missing = expected - covered
        if missing:
            context.log.warning(
                f"Survey {survey_id_val}: absence padding incomplete - "
                f"species not covered: {missing}. "
                f"Check for name mismatches in SPECIES_DICT."
            )

    # ── Combine and return ─────────────────────────────────────────────────────
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