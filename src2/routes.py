from flask import Flask, request, jsonify
import os
from constant import UPLOAD_FOLDER, ALLOWED_EXTENSIONS
from vector_search import run_vector_search
from schema import Fabric

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

def allowed_file(filename):
    """Check if the file extension is allowed."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/upload", methods=["POST"])
def upload_file():
    """Handle file uploads."""
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400
    if file and allowed_file(file.filename):
        file_path = os.path.join(app.config["UPLOAD_FOLDER"], file.filename)
        file.save(file_path)
        return jsonify({"message": "File uploaded successfully", "file_path": file_path}), 200
    return jsonify({"error": "Unsupported file type."}), 400

@app.route("/search", methods=["POST"])
def search():
    """Handle vector search requests."""
    data = request.form
    table_name = data.get("table_name")
    search_query = data.get("search_query")
    limit = int(data.get("limit", 0))
    image_path = data.get("image_path")

    if not table_name or not search_query:
        return jsonify({"error": "Missing required parameters"}), 400

    try:
        results = run_vector_search(
            "~/.lancedb",
            table_name,
            Fabric,
            search_query if not image_path else image_path,
            limit
        )
        return jsonify({"results": results}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
