COLUMN_FILE = "./somalia/gsheets/columns.txt"
EXCLUDED_COLUMN_FILE = "./somalia/gsheets/excluded_columns.txt"


def get_column_mapping():
    """
    Reads the column names from columns.txt
    Returns a list of column names as well as a mapping of the position of these columns
    """
    with open(COLUMN_FILE, "r") as f:
        columns = f.read().rstrip().split("\n")
        # Mapping of column to column index
        column_map = {}

        # Populate mapping
        for i in range(len(columns)):
            column_map[columns[i]] = i

    return column_map


def get_excluded_column_mapping():
    """
    Returns a dictionary of excluded columns (mapping to weather this column was used) as well as a tab indicating excluded columns.
    """
    with open(EXCLUDED_COLUMN_FILE, "r") as f:
        exc_columns = f.read().rstrip().split("\n")

    excluded_columns_map = {}

    for col in exc_columns:
        excluded_columns_map[col] = False

    return excluded_columns_map
