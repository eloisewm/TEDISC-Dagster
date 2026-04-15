from dagster import ConfigurableResource


class DatabaseResource(ConfigurableResource):
    """
    A resource representing a connection to the MSSQL database.
    Fill in your real connection string when you're ready to use it.

    Usage in an asset:
        @asset
        def my_asset(db: DatabaseResource):
            conn = db.get_connection()
            ...
    """

    connection_string: str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=imasoredev-db.its.utas.edu.au;"
        "Database=IMASORE;"
        "UID=username;"
        "PWD=password"
    )

    def get_connection(self):
        import pyodbc
        return pyodbc.connect(self.connection_string)