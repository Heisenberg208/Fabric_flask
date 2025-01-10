import os
import streamlit as st
import requests
from constant import IMAGES_PER_BATCH, DEFAULT_LIMIT, IMAGES_PER_ROW
from message import SUCCESSFUL_SEARCH, NO_RESULTS_FOUND, SEARCH_ERROR

# Streamlit Page Configuration
st.set_page_config(page_title="Vector Search App", layout="wide")

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
                col.image(image_path, caption=os.path.basename(image_path))
    else:
        st.info(NO_RESULTS_FOUND)

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

    with st.form("search_settings"):
        st.subheader("Search Settings")
        table_name = st.text_input("Table Name", value="fabric2")
        search_query = st.text_input("Search Query", value="blue")
        limit_input = st.number_input(
            "Limit (0 for No Limit)",
            min_value=0,
            value=DEFAULT_LIMIT,
            step=1
        )
        uploaded_image = st.file_uploader("Upload an Image", type=["jpg", "jpeg", "png"])
        submit_button = st.form_submit_button("Run Search")

    if submit_button:
        st.session_state.displayed_count = IMAGES_PER_BATCH
        limit = 50 if limit_input == 0 else int(limit_input)

        image_path = None
        if uploaded_image and allowed_file(uploaded_image.name):
            response = requests.post(
                "http://127.0.0.1:5000/upload",
                files={"file": uploaded_image}
            )
            if response.status_code == 200:
                image_path = response.json()["file_path"]
                search_query = image_path

        response = requests.post(
            "http://127.0.0.1:5000/search",
            data={"table_name": table_name, "search_query": search_query, "limit": limit}
        )

        if response.status_code == 200:
            st.session_state.search_results = response.json()["results"]
            st.session_state.total_available = len(st.session_state.search_results)
            st.success(SUCCESSFUL_SEARCH)
        else:
            st.error(SEARCH_ERROR)

    if st.session_state.search_results:
        total_images = st.session_state.total_available
        currently_showing = min(st.session_state.displayed_count, total_images)

        st.write(f"Showing {currently_showing} of {total_images} images")
        display_images_grid(st.session_state.search_results[:currently_showing])

        if currently_showing < total_images:
            if st.button("Load More Results"):
                st.session_state.displayed_count += IMAGES_PER_BATCH

if __name__ == "__main__":
    main()
