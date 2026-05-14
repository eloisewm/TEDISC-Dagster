import os
from dagster_mssql_bcp import PandasBCPIOManager, PandasBCPResource
 

# I/O manager that bulk-loads pandas DataFrames into MSSQL using Microsoft's bcp
# (Bulk Copy Program) utility.

# On materialisation, the I/O manager:
#   1. Creates the target table if it doesn't exist, using the asset's asset_schema
#   2. Creates a temporary staging table
#   3. Writes the DataFrame to a temp CSV and bulk-loads it into the staging table via bcp
#   4. Promotes the data from staging into the real table, then drops the staging table
#
# Connection details are read from environment variables at runtime.
# bcp must be installed at /opt/mssql-tools18/bin/bcp (WSL/Linux).
bcp_io_manager = PandasBCPIOManager(
    resource=PandasBCPResource(
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT", "1433"),
        database=os.environ.get("DB_NAME"),
        username=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        query_props={"TrustServerCertificate": "yes"},
        bcp_arguments={"-u": ""},
        bcp_path="/opt/mssql-tools18/bin/bcp",
    )
)