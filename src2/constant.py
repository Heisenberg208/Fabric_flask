import os

# Paths and Directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Default Configuration
IMAGES_PER_BATCH = 8  # Number of images to load at once
DEFAULT_LIMIT = 24    # Initial number of results to fetch
IMAGES_PER_ROW = 4    # Number of images per row

# Allowed File Extensions
ALLOWED_EXTENSIONS = {"jpg", "jpeg", "png"}
