from flask import Flask, render_template, request, redirect, url_for, send_from_directory
from PIL import Image
import os
from schema import Fabric
from vector_search import run_vector_search

app = Flask(__name__)

# Configurations
UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = r"C:\Users\Harish\Desktop\Fabric_flask\output"
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["OUTPUT_FOLDER"] = OUTPUT_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        # Form data
        table_name = request.form.get("table_name", "myntra")
        search_query = request.form.get("search_query", "kurta")
        limit_input = request.form.get("limit", "3")  # Get limit as string

        # Handle No Limit option
        if limit_input == "0":  # Assuming "0" means No Limit
            limit = 50  # Set a high number for ANN/KNN queries
        else:
            limit = int(limit_input)  # Convert to integer

        # Handle image upload
        uploaded_image = request.files.get("uploaded_image")
        image_path = None
        if uploaded_image and allowed_file(uploaded_image.filename):
            image_path = os.path.join(app.config["UPLOAD_FOLDER"], uploaded_image.filename)
            uploaded_image.save(image_path)
            search_query = image_path

        # Run vector search
        run_vector_search("~/.lancedb", table_name, Fabric, search_query, limit, app.config["OUTPUT_FOLDER"])

        return redirect(url_for("results"))

    return render_template("index.html")

@app.route("/results", methods=["GET"])
def results():
    # Get output images
    image_files = [
        f for f in os.listdir(app.config["OUTPUT_FOLDER"]) if f.endswith((".jpg", ".png"))
    ]
    image_files.sort()

    if not image_files:
        return render_template("results.html", images=None, message="No images found.")

    current_index = int(request.args.get("index", 0))
    current_index %= len(image_files)

    current_image = image_files[current_index]
    prev_index = (current_index - 1) % len(image_files)
    next_index = (current_index + 1) % len(image_files)

    return render_template(
        "results.html",
        current_image=current_image,
        prev_index=prev_index,
        next_index=next_index,
    )

@app.route("/output/<filename>")
def output_file(filename):
    return send_from_directory(app.config["OUTPUT_FOLDER"], filename)

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in {"jpg", "jpeg", "png"}

if __name__ == "__main__":
    app.run(debug=True)
