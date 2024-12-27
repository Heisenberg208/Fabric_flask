import os
from PIL import Image
import streamlit as st
from schema import Fabric
from vector_search import run_vector_search
from make_table import create_combined_table  # Import create_combined_table
import lancedb

# Streamlit Page Configuration
st.set_page_config(page_title="Vector Search App", layout="wide")

# Configurations
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "output")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

DATABASE_PATH = "~/.lancedb"  # Path to LanceDB
TABLE_NAME = "fabric2"  # Default table name
ROOT_FOLDER = r"C:\Users\Harish\Desktop\Fabric_flask\input"  # Root folder for images

# Utility function to check allowed file types
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in {"jpg", "jpeg", "png"}

# Ensure the table exists and display transparency
def ensure_table_exists():
    st.info("Checking the table status...")
    db = lancedb.connect(DATABASE_PATH)
    
    if TABLE_NAME in db:
        st.success(f"The table '{TABLE_NAME}' already exists in the database.")
    else:
        st.warning(f"The table '{TABLE_NAME}' does not exist. Creating the table now...")
        create_combined_table(
            database=DATABASE_PATH,
            table_name=TABLE_NAME,
            root_folder=ROOT_FOLDER,
            schema=Fabric,
            mode="overwrite",  # Change to "append" if you want to add new data
            force=False,  # Avoid forcing recreation
        )
        st.success(f"The table '{TABLE_NAME}' has been successfully created.")

def main():
    # Ensure the table is created or provide information if it exists
    ensure_table_exists()

    # Streamlit App
    st.title("Vector Search App")

    # Input fields
    st.subheader("Search Settings")
    table_name = st.text_input("Table Name", value=TABLE_NAME)
    search_query = st.text_input("Search Query", value="kurta")
    limit_input = st.number_input("Limit (0 for No Limit)", min_value=0, value=3, step=1)
    uploaded_image = st.file_uploader("Upload an Image", type=["jpg", "jpeg", "png"])

    # Button to trigger search
    if st.button("Run Search"):
        try:
            # Handle "No Limit" option
            limit = 50 if limit_input == 0 else int(limit_input)

            # Save the uploaded image if available
            image_path = None
            if uploaded_image and allowed_file(uploaded_image.name):
                image_path = os.path.join(UPLOAD_FOLDER, uploaded_image.name)
                with open(image_path, "wb") as f:
                    f.write(uploaded_image.getbuffer())
                search_query = image_path

            # Run vector search
            run_vector_search(DATABASE_PATH, table_name, Fabric, search_query, limit, OUTPUT_FOLDER)
            st.success("Search completed successfully. Results are displayed below.")
        except ValueError:
            st.error("Invalid input. Please check your entries.")
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

    # Display results
    st.subheader("Search Results")
    image_files = [
        f for f in os.listdir(OUTPUT_FOLDER) if f.endswith((".jpg", ".png"))
    ]
    image_files.sort()

    # Pagination settings
    images_per_page = 4
    total_images = len(image_files)
    total_pages = (total_images + images_per_page - 1) // images_per_page

    if total_images > 0:
        # Pagination control
        page = st.number_input(
            "Page Number", min_value=1, max_value=total_pages, value=1, step=1, key="pagination"
        )
        start_index = (page - 1) * images_per_page
        end_index = start_index + images_per_page

        # Display images horizontally with fixed dimensions
        current_images = image_files[start_index:end_index]
        if current_images:
            cols = st.columns(len(current_images))  # Create one column per image
            for col, image_file in zip(cols, current_images):
                image_path = os.path.join(OUTPUT_FOLDER, image_file)
                col.image(image_path, caption=image_file, width=200)  # Set a fixed width for consistency
    else:
        st.info("No images found in the output folder.")

# Allow script to be run standalone
if __name__ == "__main__":
    main()
