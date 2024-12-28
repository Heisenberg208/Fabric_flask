import argparse
import logging
from pathlib import Path
import lancedb
import pandas as pd
from schema import Fabric

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def update_table_with_new_images(
    database: str,
    table_name: str,
    root_folder: str,
    schema: Fabric,
) -> None:
    """
    Update an existing table with new images from the root folder if not already present.

    Args:
        database (str): The name of the database to connect to.
        table_name (str): The name of the table to update.
        root_folder (str): The path to the root folder containing subfolders with images.
        schema (Fabric): The schema to use for the table.

    Returns:
        None
    """
    # Connect to the lancedb database
    logger.info("Connecting to the database...")
    db = lancedb.connect(database)

    # Check if the table exists
    if table_name not in db:
        logger.info(f"Table '{table_name}' does not exist. Creating a new table...")
        create_combined_table(database, table_name, root_folder, schema, mode="overwrite", force=False)
        return

    # Table exists, retrieve existing image URIs
    table = db.open_table(table_name)
    existing_uris = set(table.to_pandas()["image_uri"])
    logger.info(f"Found {len(existing_uris)} existing image URIs in the table.")

    # Collect all image paths from the root folder
    root_path = Path(root_folder).expanduser()
    all_uris = {str(f) for f in root_path.rglob("*.jpg")}
    logger.info(f"Found {len(all_uris)} images in all subfolders of '{root_folder}'.")

    # Find new URIs not already in the table
    new_uris = all_uris - existing_uris
    logger.info(f"Found {len(new_uris)} new images to add to the table.")

    if new_uris:
        # Add new URIs to the table
        table.add(pd.DataFrame({"image_uri": list(new_uris)}))
        logger.info(f"Successfully added {len(new_uris)} new images to the table.")
    else:
        logger.info("No new images to add. The table is up-to-date.")
    logger.info("Previewing some rows from the updated table:")
    print(table.to_pandas().head())


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
        schema (Fabric): The schema to use for the table.
        mode (str, optional): The mode for creating the table. Defaults to "overwrite".
        force (bool, optional): Force recreation of the table if it exists. Defaults to False.

    Returns:
        None
    """
    # Connect to the lancedb database
    logger.info("Connecting to the database...")
    db = lancedb.connect(database)

    # If force is True, drop the table if it exists
    if force and table_name in db:
        logger.warning(f"Force flag set. Dropping existing table '{table_name}'.")
        db.drop_table(table_name)
        logger.info(f"Table '{table_name}' dropped successfully.")

    # Check if the table already exists
    if table_name in db:
        logger.info(f"Table '{table_name}' already exists. Updating with new images if needed.")
        update_table_with_new_images(database, table_name, root_folder, schema)
        return

    # Collect all image paths from subfolders
    root_path = Path(root_folder).expanduser()
    uris = [str(f) for f in root_path.rglob("*.jpg")]
    logger.info(f"Found {len(uris)} images in all subfolders of '{root_folder}'.")

    if not uris:
        logger.error("No images found. Exiting.")
        return

    # Create the table
    logger.info(f"Creating table '{table_name}' in the database.")
    table = db.create_table(table_name, schema=schema, mode=mode)

    # Add the data to the table
    logger.info(f"Adding {len(uris)} images to the table.")
    table.add(pd.DataFrame({"image_uri": uris}))
    logger.info(f"Successfully added {len(uris)} images to the table.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create or update a combined table in lancedb from all subfolders"
    )
    parser.add_argument(
        "--database", help="Path to the lancedb database", default="~/.lancedb"
    )
    parser.add_argument(
        "--table_name", help="Name of the table to be created or updated", default="myntra_combined"
    )
    parser.add_argument(
        "--root_folder", help="Path to the root folder containing subfolders with images", required=True
    )
    parser.add_argument(
        "--schema",
        help="Schema to use for the table. Defaults to Fabric",
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

    # Call the update function instead of create_combined_table
    update_table_with_new_images(
        args.database,
        args.table_name,
        args.root_folder,
        args.schema,
    )

    
