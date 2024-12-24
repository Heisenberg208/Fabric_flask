import argparse
from pathlib import Path
from random import sample
import lancedb
import pandas as pd

from schema import Fabric


def create_combined_table(
    database: str,
    table_name: str,
    root_folder: str,
    schema: Fabric,
    mode: str = "overwrite",
    force: bool = False,
) -> None:
    """
    Create a single table in the specified vector database with data from all subfolders.

    Args:
        database (str): The name of the database to connect to.
        table_name (str): The name of the table to create.
        root_folder (str): The path to the root folder containing subfolders with images.
        schema (Schema, optional): The schema to use for the table. Defaults to Myntra.
        mode (str, optional): The mode for creating the table. Defaults to "overwrite".
        force (bool, optional): Force recreation of the table if it exists. Defaults to False.

    Returns:
        None
    """
    # Connect to the lancedb database
    db = lancedb.connect(database)

    # If force is True, drop the table if it exists
    if force and table_name in db:
        print(f"Force flag set. Dropping existing table {table_name}")
        db.drop_table(table_name)
        print(f"Table {table_name} dropped successfully")

    # Check if the table already exists in the database
    if table_name in db:
        print(f"Table {table_name} already exists in the database")
        return

    # Collect all image paths from subfolders
    root_path = Path(root_folder).expanduser()
    uris = [str(f) for f in root_path.rglob("*.jpg")]
    print(f"Found {len(uris)} images in all subfolders of {root_folder}")

    if not uris:
        print("No images found. Exiting.")
        return

    # Sample images (optional: you can shuffle or limit the size if needed)
    uris = sample(uris, len(uris))

    # Create the table
    print(f"Creating table {table_name} in the database")
    table = db.create_table(table_name, schema=schema, mode=mode)

    # Add the data to the table
    print(f"Adding {len(uris)} images to the table")
    table.add(pd.DataFrame({"image_uri": uris}))
    print(f"Added {len(uris)} images to the table")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a combined table in lancedb from all subfolders"
    )
    parser.add_argument(
        "--database", help="Path to the lancedb database", default="~/.lancedb"
    )
    parser.add_argument(
        "--table_name", help="Name of the table to be created", default="myntra_combined"
    )
    parser.add_argument(
        "--root_folder", help="Path to the root folder containing subfolders with images", required=True
    )
    parser.add_argument(
        "--schema",
        help="Schema to use for the table. Defaults to Myntra",
        default=Fabric,
    )
    parser.add_argument(
        "--mode",
        help="Mode for creating the table. Defaults to 'overwrite'",
        default="overwrite",
    )
    parser.add_argument(
        "--force",
        help="Force recreation of the table if it exists",
        action="store_true"
    )
    args = parser.parse_args()

    create_combined_table(
        args.database,
        args.table_name,
        args.root_folder,
        args.schema,
        args.mode,
        args.force,
    )
