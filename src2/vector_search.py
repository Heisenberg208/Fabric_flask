import argparse
import os
from typing import Any
from PIL import Image

import lancedb

from schema import Fabric, get_schema_by_name


def run_vector_search(
    database: str,
    table_name: str,
    schema: Any,
    search_query: Any,
    limit: int = 6,
) -> list:
    """
    Performs a vector search and returns image URIs.
    """
    # Connect to the lancedb database
    db = lancedb.connect(database)
    table = db.open_table(table_name)

    # Handle text or image query
    try:
        if search_query.endswith(".jpg") or search_query.endswith(".png"):
            search_query = Image.open(search_query)
    except AttributeError:
        pass

    # Perform the search
    rs = table.search(search_query).limit(limit).to_pydantic(schema)

    # Extract image URIs
    image_uris = [result.image_uri for result in rs if hasattr(result, "image_uri")]
    return image_uris




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vector Search")
    parser.add_argument("--database", type=str, help="Path to the database")
    parser.add_argument("--table_name", type=str, help="Name of the table")
    parser.add_argument(
        "--schema", type=str, help="Schema of the table", default="Fabric"
    )
    parser.add_argument("--search_query", type=str, help="Search query")
    parser.add_argument(
        "--limit", type=int, default=6, help="Limit the number of results (default: 6)"
    )

    args = parser.parse_args()

    schema = get_schema_by_name(args.schema)
    if schema is None:
        raise ValueError(f"Unknown schema: {args.schema}")

    image_paths = run_vector_search(
        args.database,
        args.table_name,
        schema,
        args.search_query,
        args.limit,
    )

    
