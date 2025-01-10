import os
import streamlit as st
from schema import Fabric
from vector_search import run_vector_search

# Streamlit Page Configuration
st.set_page_config(page_title="Vector Search App", layout="wide")

# Configurations
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Constants
IMAGES_PER_BATCH = 8  # Number of images to load at once
DEFAULT_LIMIT = 24    # Initial number of results to fetch
IMAGES_PER_ROW = 4    # Number of images per row

# Custom CSS for layout and spacing
st.markdown("""
    <style>
        .image-container {
            display: flex;
            flex-wrap: wrap;
            justify-content: center;
            gap: 15px;
        }
        .image-container img {
            max-width: 100%;
            height: auto;
            border-radius: 5px;
            box-shadow: 0px 2px 4px rgba(0, 0, 0, 0.1);
        }
    </style>
""", unsafe_allow_html=True)

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in {"jpg", "jpeg", "png"}

def display_images_grid(image_paths):
    """Display images in a grid with 4 images per row."""
    if image_paths:
        num_images = len(image_paths)
        rows = (num_images + IMAGES_PER_ROW - 1) // IMAGES_PER_ROW  # Calculate total rows
        
        for row in range(rows):
            start_idx = row * IMAGES_PER_ROW
            end_idx = min(start_idx + IMAGES_PER_ROW, num_images)
            cols = st.columns(IMAGES_PER_ROW)  # Create 4 columns per row
            
            for col, image_path in zip(cols, image_paths[start_idx:end_idx]):
                col.image(
                    image_path,
                    caption=os.path.basename(image_path),
                    use_container_width=True
                )
    else:
        st.info("No images found.")

def initialize_session_state():
    """Initialize or reset session state variables."""
    if 'displayed_count' not in st.session_state:
        st.session_state.displayed_count = IMAGES_PER_BATCH
    if 'search_results' not in st.session_state:
        st.session_state.search_results = []
    if 'total_available' not in st.session_state:
        st.session_state.total_available = 0

def main():
    initialize_session_state()
    st.title("Vector Search App")
    
    # Search settings in a form
    with st.form("search_settings"):
        st.subheader("Search Settings")
        col1, col2 = st.columns(2)
        
        with col1:
            table_name = st.text_input("Table Name", value="fabric2")
            search_query = st.text_input("Search Query", value="blue")
        
        with col2:
            limit_input = st.number_input(
                "Limit (0 for No Limit)",
                min_value=0,
                value=DEFAULT_LIMIT,
                step=1
            )
            uploaded_image = st.file_uploader(
                "Upload an Image",
                type=["jpg", "jpeg", "png"]
            )
        
        submit_button = st.form_submit_button("Run Search")

    if submit_button:
        # Reset display count when new search is performed
        st.session_state.displayed_count = IMAGES_PER_BATCH
        
        # Handle "No Limit" option
        limit = 50 if limit_input == 0 else int(limit_input)
        
        # Handle image upload
        image_path = None
        if uploaded_image and allowed_file(uploaded_image.name):
            image_path = os.path.join(UPLOAD_FOLDER, uploaded_image.name)
            with open(image_path, "wb") as f:
                f.write(uploaded_image.getbuffer())
            search_query = image_path
        
        # Run search and store results in session state
        try:
            st.session_state.search_results = run_vector_search(
                "~/.lancedb",
                table_name,
                Fabric,
                search_query,
                limit
            )
            st.session_state.total_available = len(st.session_state.search_results)
        except Exception as e:
            st.error(f"Error during search: {str(e)}")
            return

    # Display results if we have any
    if st.session_state.search_results:
        st.success("Search completed successfully!")
        
        # Display results section
        st.subheader("Search Results")
        total_images = st.session_state.total_available
        currently_showing = min(st.session_state.displayed_count, total_images)
        
        st.write(f"Showing {currently_showing} of {total_images} images")
        display_images_grid(st.session_state.search_results[:currently_showing])
        
        # Show "Load More" button if there are more images to display
        if currently_showing < total_images:
            if st.button("Load More Results"):
                st.session_state.displayed_count += IMAGES_PER_BATCH

    elif submit_button:
        st.info("No images found for the search query.")

if __name__ == "__main__":
    main()
