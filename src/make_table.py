import argparse
import logging
import hashlib
import os
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


def get_file_info(image_path: str) -> dict:
    """
    Compute both the SHA256 hash and get modification time of an image.
    Returns a dictionary with hash and mtime.
    """
    hasher = hashlib.sha256()
    with open(image_path, 'rb') as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    
    mtime = os.path.getmtime(image_path)
    return {
        'hash': hasher.hexdigest(),
        'mtime': mtime
    }


def collect_image_data(root_folder: str) -> list:
    """
    Collect information about all images in the root folder.
    """
    root_path = Path(root_folder).expanduser()
    image_data = []
    
    for img_path in root_path.rglob("*.jpg"):
        str_path = str(img_path)
        file_info = get_file_info(str_path)
        image_data.append({
            "image_uri": str_path,
            "hash": file_info['hash'],
            "mtime": file_info['mtime']
        })
    
    return image_data


def deduplicate_table_by_hash(table):
    """
    Remove duplicate entries in the table based on the 'hash' column.
    Keep the entry with the most recent modification time.
    """
    logger.info("Checking for duplicates in the table...")
    df = table.to_pandas()

    if 'hash' not in df.columns or 'mtime' not in df.columns:
        logger.error("Required columns not found. Skipping deduplication.")
        return

    # For each hash, keep only the row with the most recent mtime
    duplicates = df[df.duplicated(subset='hash', keep=False)]
    if duplicates.empty:
        logger.info("No duplicates found in the table.")
        return

    # Group by hash and find entries to remove
    for hash_value in duplicates['hash'].unique():
        hash_group = df[df['hash'] == hash_value].sort_values('mtime', ascending=False)
        if len(hash_group) > 1:
            # Keep the most recent entry, delete others
            entries_to_remove = hash_group.iloc[1:]
            for _, row in entries_to_remove.iterrows():
                table.delete(f"hash == '{row['hash']}' and image_uri == '{row['image_uri']}'")
                logger.info("removed ")
    logger.info("Successfully removed duplicate entries.")


def create_table(db, table_name: str, schema: Fabric, image_data: list):
    """
    Create a new table with the given image data.
    """
    logger.info(f"Creating new table '{table_name}'...")
    table = db.create_table(table_name, schema=schema, mode="overwrite")
    
    if image_data:
        logger.info(f"Adding {len(image_data)} images to the table.")
        table.add(pd.DataFrame(image_data))
        logger.info("Successfully added images to the table.")
        deduplicate_table_by_hash(table)
    else:
        logger.warning("No images found to add to the table.")
    
    return table


def update_existing_table(table, current_images: list):
    """
    Update existing table with new image data.
    """
    existing_data = table.to_pandas()
    
    # Convert current images to a DataFrame for easier comparison
    current_df = pd.DataFrame(current_images)
    
    # Find new or modified images
    new_or_modified = []
    for _, row in current_df.iterrows():
        existing_row = existing_data[existing_data['image_uri'] == row['image_uri']]
        
        if existing_row.empty or (not existing_row.empty and existing_row.iloc[0]['hash'] != row['hash']):
            new_or_modified.append(row.to_dict())
            if not existing_row.empty:
                # Delete the old entry if it exists
                table.delete(f"image_uri == '{row['image_uri']}'")
    
    # Add new or modified images
    if new_or_modified:
        table.add(pd.DataFrame(new_or_modified))
        logger.info(f"Added {len(new_or_modified)} new or modified images.")
    
    # Remove entries for missing images
    current_uris = set(current_df['image_uri'])
    existing_uris = set(existing_data['image_uri'])
    missing_uris = existing_uris - current_uris
    
    if missing_uris:
        for uri in missing_uris:
            table.delete(f"image_uri == '{uri}'")
        logger.info(f"Removed {len(missing_uris)} missing images.")
    
    deduplicate_table_by_hash(table)


def process_images(database: str, table_name: str, root_folder: str, schema: Fabric, force: bool = False):
    """
    Main function to process images and manage the database table.
    """
    # Connect to database
    logger.info("Connecting to the database...")
    db = lancedb.connect(database)
    
    # Collect current image data
    logger.info(f"Scanning directory: {root_folder}")
    current_images = collect_image_data(root_folder)
   
    
    if not current_images:
        logger.error("No images found in the specified directory.")
        return
    
    logger.info(f"Found {len(current_images)} images in the root folder.")
    
    # Check if table exists
    table_exists = table_name in db
    
    if force:
        # Force create new table
        if table_exists:
            logger.info(f"Force flag set. Dropping existing table '{table_name}'.")
            db.drop_table(table_name)
        table = create_table(db, table_name, schema, current_images)
    
    elif not table_exists:
        # Create new table if it doesn't exist
        logger.info(f"Table does'nt existss")
        table = create_table(db, table_name, schema, current_images)
    
    else:
        # Update existing table
        table = db.open_table(table_name)
        total_images=len(table)
        logger.info(f"Total {total_images} images in table")
        logger.info(f"Updating existing table '{table_name}'...")
        update_existing_table(table, current_images)
    
    # Show final table preview
    logger.info("Final table preview:")
    print(table.to_pandas())


def main():
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
        "--schema", help="Schema to use for the table", default=Fabric
    )
    parser.add_argument(
        "--force", help="Force recreation of the table if it exists", action="store_true"
    )
    
    args = parser.parse_args()
    
    process_images(
        database=args.database,
        table_name=args.table_name,
        root_folder=args.root_folder,
        schema=args.schema,
        force=args.force
    )


if __name__ == "__main__":
    main()