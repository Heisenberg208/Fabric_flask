import argparse
from pathlib import Path
from random import sample
from typing import Any

import lancedb
import pandas as pd

from schema import Myntra


def create_table(
    database: str,
    table_name: str,
    data_path: str,
    schema: Any = Myntra,
    mode: str = "overwrite",
    num_samples: int = 1000,
    force: bool = False  # Add a new parameter for forced recreation
) -> None:
    """
    Create a table in the specified vector database and add data to it.

    Args:
        database (str): The name of the database to connect to.
        table_name (str): The name of the table to create.
        data_path (str): The path to the data directory.
        schema (Schema, optional): The schema to use for the table. Defaults to Myntra.
        mode (str, optional): The mode for creating the table. Defaults to "overwrite".
        num_samples(int, optional): The number of Image samples to be added to the database.
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

    # Create the table
    print(f"Creating table {table_name} in the database")

    # Create the table with the given schema
    table = db.create_table(table_name, schema=schema, mode=mode)

    # Define the Path of the images and obtain the Image uri
    p = Path(data_path).expanduser()
    uris = [str(f) for f in p.glob("*.jpg")]
    print(f"Found {len(uris)} images in {p}")

    # Sample images from the data
    uris = sample(uris, min(num_samples, len(uris)))

    # Add the data to the table
    print(f"Adding {len(uris)} images to the table")
    table.add(pd.DataFrame({"image_uri": uris}))
    print(f"Added {len(uris)} images to the table")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a table in lancedb with Myntra data"
    )
    parser.add_argument(
        "--database", help="Path to the lancedb database", default="~/.lancedb"
    )
    parser.add_argument(
        "--table_name", help="Name of the table to be created", default="myntra"
    )
    parser.add_argument(
        "--data_path", help="Path to the Myntra data images", required=True
    )
    parser.add_argument(
        "--schema",
        help="Schema to use for the table. Defaults to Myntra",
        default=Myntra,
    )
    parser.add_argument(
        "--mode",
        help="Mode for creating the table. Defaults to 'overwrite'",
        default="overwrite",
    )
    parser.add_argument(
        "--num_samples",
        help="Number of Image samples to be added to the table",
        type=int,
        default=1000,
    )
    parser.add_argument(
        "--force",
        help="Force recreation of the table if it exists",
        action="store_true"
    )
    args = parser.parse_args()

    create_table(
        args.database,
        args.table_name,
        args.data_path,
        args.schema,
        args.mode,
        args.num_samples,
        args.force,
    )