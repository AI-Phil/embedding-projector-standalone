import os
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from astrapy import DataAPIClient
from astrapy.database import Database
from astrapy.data_types import DataAPIVector
import json
import logging
from fastapi import HTTPException
import numpy as np

# Configuration
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(ROOT_DIR, "static")       # Specific static dir
TEMPLATES_DIR = os.path.join(ROOT_DIR, "templates") # Directory for HTML templates
MAIN_SERVER_HOST = "0.0.0.0"                        # Listen on all interfaces
MAIN_SERVER_PORT = 8000                             # Port for users to access

app = FastAPI()

# --- Request Models (for validation) ---
class ConnectionInfo(BaseModel):
    endpoint_url: str
    token: str
    db_name: str
    keyspace: str | None = None

class SampleRequest(BaseModel):
    connection: ConnectionInfo
    collection_name: str

class SampleDataPayload(BaseModel):
    sample_data: list[dict]

class SaveConfigRequest(BaseModel):
    connection: ConnectionInfo
    tensor_name: str
    collection_name: str
    vector_dimension: int
    metadata_keys: list[str]
    sample_data: list[dict]

# --- Global DataAPIClient cache ---
astra_data_api_clients = {}

def get_data_api_client(info: ConnectionInfo) -> Database:
    """Gets or creates a Database instance via DataAPIClient."""
    key = info.endpoint_url + (info.keyspace or 'default_keyspace')
    if key not in astra_data_api_clients:
        print(f"Creating new DataAPIClient connection for {info.endpoint_url}, Keyspace: {info.keyspace or 'default_keyspace'}")
        try:
            client = DataAPIClient()
            db = client.get_database(
                info.endpoint_url, 
                token=info.token, 
                keyspace=info.keyspace or 'default_keyspace'
            )
            astra_data_api_clients[key] = db
            print(f"Connected to database via Data API: {info.endpoint_url}, Keyspace: {db.namespace}")
        except Exception as e:
            print(f"Failed to create DataAPIClient/Database: {e}")
            if "Unauthorized" in str(e) or "Forbidden" in str(e):
                 raise ValueError(f"Authentication failed. Check your token and Data API Endpoint URL. Error: {e}") from e
            else:
                 raise ValueError(f"Failed to connect using Data API: {e}") from e
    return astra_data_api_clients[key]

# --- Setup Templates ---
# Create templates directory if it doesn't exist
if not os.path.exists(TEMPLATES_DIR):
    os.makedirs(TEMPLATES_DIR)
templates = Jinja2Templates(directory=TEMPLATES_DIR)

@app.get("/astra", response_class=HTMLResponse, include_in_schema=False) # include_in_schema=False to avoid duplicate docs
@app.get("/astra/", response_class=HTMLResponse)                         # Add route for trailing slash
async def get_astra_page(request: Request):
    """Serves the main HTML page for the Astra helper app."""
    return templates.TemplateResponse("astra.html", {"request": request})

# --- API Routes (Defined Before Static Mounts) ---
@app.post("/api/astra/collections")
async def api_astra_get_collections(connection_info: ConnectionInfo):
    """API endpoint to list vector-enabled collections and their dimensions."""
    print(f"Received request for collections: Endpoint={connection_info.endpoint_url}, Keyspace: {connection_info.keyspace or 'default_keyspace'}")
    vector_collections_details = []
    try:
        db = get_data_api_client(connection_info)
        # Use list_collections() to get details
        collections_result = db.list_collections()
        
        if collections_result:
            print(f"Checking {len(collections_result)} collections for vector capability...")
            for col_desc in collections_result:
                try:
                    col_name = col_desc.name
                    definition = col_desc.definition
                    vector_options = definition.vector if definition else None
                    dimension = vector_options.dimension if vector_options else None
                    service_options = vector_options.service if vector_options else None

                    # Check if vector is enabled (has dimension OR service)
                    if vector_options and (dimension or service_options):
                        if dimension:
                            print(f" - Found vector collection: {col_name} (Dimension: {dimension})")
                            vector_collections_details.append({"name": col_name, "dimension": dimension})
                        else:
                            print(f" - Found vectorize collection (no dimension specified): {col_name}")
                            vector_collections_details.append({"name": col_name, "dimension": 0}) # Use 0 as placeholder
                    else:
                        print(f" - Skipping non-vector collection: {col_name}")
                except AttributeError as ae:
                    print(f"Warning: Could not process collection descriptor structure: {ae}. Descriptor: {col_desc}")
                except Exception as inner_e:
                     print(f"Warning: Error processing collection {getattr(col_desc, 'name', '?')}: {inner_e}")
        else:
             print("No collections found for this keyspace.")

        return {"collections": vector_collections_details} 
    except ValueError as e: 
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        print(f"Error listing collections via Data API: {e}") # Updated log
        return JSONResponse(status_code=500, content={"error": f"An unexpected error occurred: {e}"})

@app.post("/api/astra/metadata_keys")
async def api_astra_get_metadata_keys(payload: SampleDataPayload):
    """Analyzes sample data to suggest metadata keys."""
    if not payload.sample_data:
        return {"keys": []}

    keyset = set()
    ignore_keys = {"$vector"} 

    for doc in payload.sample_data:
        for key in doc.keys():
            if key not in ignore_keys:
                keyset.add(key)

    sorted_keys = sorted(list(keyset))
    print(f"Extracted potential metadata keys: {sorted_keys}")
    return {"keys": sorted_keys}

@app.post("/api/astra/sample")
async def api_astra_sample_data(sample_request: SampleRequest):
    """API endpoint to sample data from a collection via Data API."""
    print(f"Received request to sample collection: {sample_request.collection_name}")
    try:
        # Pass full connection info (including keyspace) to get the correct client
        db = get_data_api_client(sample_request.connection)
        collection = db.get_collection(sample_request.collection_name)
        # Explicitly project the $vector field, let other fields come by default
        cursor = collection.find(limit=10, projection={"$vector": True}) 
        sample_docs_raw = list(cursor)
        print(f"Sampled {len(sample_docs_raw)} documents via Data API.")

        # Convert DataAPIVector to list before sending to frontend
        sample_docs_processed = []
        for doc in sample_docs_raw:
            if "$vector" in doc and isinstance(doc["$vector"], DataAPIVector):
                 # Convert DataAPIVector to a standard list
                 # Usually, just casting to list() works for vector types
                 doc["$vector"] = list(doc["$vector"]) 
            sample_docs_processed.append(doc)

        return {"sample_data": sample_docs_processed}
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        print(f"Error sampling data via Data API: {e}")
        return JSONResponse(status_code=500, content={"error": f"An unexpected error occurred while sampling: {e}"})

# --- Placeholder for TSV Generation ---
@app.post("/api/astra/generate_tsv")
async def api_astra_generate_tsv(request: Request):
     # TODO: Implement TSV generation
     # Needs payload: { sample_data: list[dict], selected_keys: list[str] }
     # data = await request.json()
     return JSONResponse(status_code=501, content={"error": "TSV generation not implemented yet"})

@app.post("/api/astra/save_data")
async def save_astra_data(request: SaveConfigRequest):
    """Fetches data, saves vectors (.bytes) and metadata (.tsv), updates config."""
    logging.info(f"Received request to save data for tensor: {request.tensor_name}")
    
    # Define the data directory path locally using the global ROOT_DIR
    # This avoids the NameError related to accessing the global ASTRA_DATA_DIR directly
    local_astra_data_dir = os.path.join(ROOT_DIR, "astra_data")
    
    try:
        # Use the connection info from the request body
        db = get_data_api_client(request.connection) # Use the correct function and pass the connection object
        collection = db.get_collection(request.collection_name)

        # Fetch all documents (consider pagination for very large collections later)
        # Project only necessary fields: _id, $vector, and selected metadata keys
        projection = {"$vector": True}
        # Ensure _id is implicitly included or add explicitly if needed by find()
        # Add other selected keys to projection
        for key in request.metadata_keys: # Use metadata_keys
             projection[key] = True 
        
        logging.info(f"Fetching documents from {request.collection_name} with projection: {projection}")
        documents = list(collection.find(projection=projection)) # Apply projection

        if not documents:
            logging.warning(f"No documents found in collection '{request.collection_name}' with projection.")
            raise HTTPException(status_code=404, detail=f"No documents found in the collection '{request.collection_name}'. Check if the collection is empty or the projection filters out all docs.")

        # Prepare file paths
        # Ensure the output directory exists right before we need it
        try:
            os.makedirs(local_astra_data_dir, exist_ok=True) # Use local variable
            logging.info(f"Ensured output directory exists: {local_astra_data_dir}") # Use local variable
        except OSError as e:
            logging.error(f"Could not create output directory {local_astra_data_dir}: {e}") # Use local variable
            raise HTTPException(status_code=500, detail=f"Server configuration error: Could not create data directory.")

        # Sanitize the tensor name received from the request (replace spaces with underscores)
        sanitized_tensor_name = request.tensor_name.replace(" ", "_")
        logging.info(f"Sanitized tensor name: '{request.tensor_name}' -> '{sanitized_tensor_name}'")

        # Use tensor_name for filenames, ensuring it's filesystem-safe
        # Base the safe name on the *sanitized* tensor name
        safe_tensor_name = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in sanitized_tensor_name)
        if not safe_tensor_name:
            safe_tensor_name = "default_tensor"
            logging.warning(f"Sanitized tensor name '{sanitized_tensor_name}' resulted in empty safe name. Using '{safe_tensor_name}'.")

        vector_file_path = os.path.join(local_astra_data_dir, f"{safe_tensor_name}.bytes") # Use safe name for path
        metadata_file_path = os.path.join(local_astra_data_dir, f"{safe_tensor_name}_metadata.tsv") # Use safe name for path

        # Extract vectors and metadata
        vectors = []
        metadata_rows = [] # Changed name for clarity
        # Ensure _id is first, then other selected keys
        metadata_header = ["_id"] + [key for key in request.metadata_keys if key != "_id"] # Use metadata_keys
        logging.info(f"Processing {len(documents)} documents. Metadata header: {metadata_header}")

        processed_doc_count = 0
        skipped_vector_count = 0
        skipped_dimension_count = 0

        for doc in documents:
            doc_id = doc.get('_id', 'UNKNOWN_ID') # Get ID for logging
            
            if "$vector" not in doc or not isinstance(doc["$vector"], (list, DataAPIVector)):
                 logging.warning(f"Document {doc_id} missing or has invalid $vector type ({type(doc.get('$vector'))}). Skipping.")
                 skipped_vector_count += 1
                 continue

            # Handle DataAPIVector explicitly
            if isinstance(doc["$vector"], DataAPIVector):
                 doc["$vector"] = list(doc["$vector"])
            
            # Ensure vector has the expected dimension
            if len(doc["$vector"]) != request.vector_dimension: # Use correct field name
                 logging.warning(f"Document {doc_id} vector dimension ({len(doc['$vector'])}) mismatch. Expected {request.vector_dimension}. Skipping.")
                 skipped_dimension_count += 1
                 continue
                 
            # Append vector (convert to float32)
            try:
                 vectors.append(np.array(doc["$vector"], dtype=np.float32))
            except ValueError as ve:
                 logging.warning(f"Document {doc_id} vector could not be converted to float32 array: {ve}. Skipping.")
                 skipped_vector_count += 1 # Count as a vector issue
                 continue

            # Construct metadata row based on header order
            row_data = []
            for key in metadata_header:
                 # Handle potential missing keys and escape TSV special chars
                 value = doc.get(key, '')
                 value_str = str(value).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                 row_data.append(value_str)
            metadata_rows.append("	".join(row_data))
            processed_doc_count += 1

        logging.info(f"Processed {processed_doc_count} documents. Skipped {skipped_vector_count} (vector issue), {skipped_dimension_count} (dimension issue).")

        if not vectors:
            error_detail = "No valid vector data found after processing." 
            if skipped_vector_count > 0:
                 error_detail += f" {skipped_vector_count} documents skipped due to missing/invalid vectors."
            if skipped_dimension_count > 0:
                 error_detail += f" {skipped_dimension_count} documents skipped due to dimension mismatch (expected {request.vector_dimension})."
            logging.error(error_detail)
            raise HTTPException(status_code=400, detail=error_detail)

        # Save vectors as .bytes (binary float32)
        # Use with statement for automatic file closing
        vector_data = np.array(vectors) # This creates a potentially large intermediate array
        logging.info(f"Attempting to save {len(vectors)} vectors ({vector_data.nbytes} bytes) to {vector_file_path}")
        try:
            with open(vector_file_path, 'wb') as vf:
                 vf.write(vector_data.tobytes()) # Write directly
            logging.info(f"Successfully saved vectors to {vector_file_path}")
        except IOError as e:
             logging.error(f"IOError saving vector file {vector_file_path}: {e}")
             raise HTTPException(status_code=500, detail=f"Could not write vector file: {e}")
        except Exception as e:
            logging.exception(f"Unexpected error saving vector file {vector_file_path}")
            raise HTTPException(status_code=500, detail=f"Unexpected error writing vector file: {str(e)}")

        # Save metadata as .tsv
        logging.info(f"Attempting to save metadata for {len(metadata_rows)} documents to {metadata_file_path}")
        try:
            with open(metadata_file_path, 'w', encoding='utf-8') as mf:
                mf.write("\t".join(metadata_header) + "\n")
                mf.write("\n".join(metadata_rows)) # Corrected: newline outside quotes
            logging.info(f"Successfully saved metadata to {metadata_file_path}")
        except IOError as e:
             logging.error(f"IOError saving metadata file {metadata_file_path}: {e}")
             raise HTTPException(status_code=500, detail=f"Could not write metadata file: {e}")
        except Exception as e:
            logging.exception(f"Unexpected error saving metadata file {metadata_file_path}")
            raise HTTPException(status_code=500, detail=f"Unexpected error writing metadata file: {str(e)}")

        # Update projector config JSON
        config = {}
        config_file_path = os.path.join(local_astra_data_dir, "astra_projector_config.json") # Use local variable for config path
        logging.info(f"Attempting to read and update config file: {config_file_path}")
        if os.path.exists(config_file_path):
            try:
                with open(config_file_path, 'r', encoding='utf-8') as f: # Specify encoding
                    config = json.load(f)
                    if not isinstance(config, dict): # Basic validation
                        logging.warning(f"Config file {config_file_path} does not contain a JSON object. Resetting.")
                        config = {}
            except json.JSONDecodeError as e:
                logging.warning(f"Could not decode existing config file {config_file_path}: {e}. Starting fresh.")
                config = {}
            except Exception as e: # Catch other potential file errors
                 logging.error(f"Error reading config file {config_file_path}: {e}. Starting fresh.")
                 config = {} # Ensure config is an empty dict
        else:
             logging.info(f"Config file {config_file_path} not found. Creating new one.")
        
        if "embeddings" not in config or not isinstance(config.get("embeddings"), list):
             logging.warning("Config 'embeddings' key missing or not a list. Initializing.")
             config["embeddings"] = []

        # Check if tensor already exists and update, otherwise add
        # Use the *sanitized* tensor name for the config entry
        tensor_entry = {
            "tensorName": sanitized_tensor_name, 
            "tensorShape": [len(vectors), request.vector_dimension],
            "tensorPath": os.path.relpath(vector_file_path, local_astra_data_dir), 
            "metadataPath": os.path.relpath(metadata_file_path, local_astra_data_dir)
        }

        found_index = -1
        for i, entry in enumerate(config["embeddings"]):
            # Ensure entry is a dict and has tensorName before comparing
            # Compare against the *sanitized* tensor name
            if isinstance(entry, dict) and entry.get("tensorName") == sanitized_tensor_name:
                found_index = i
                break
        
        if found_index != -1:
             logging.info(f"Updating existing entry for tensor '{sanitized_tensor_name}' in config at index {found_index}.")
             config["embeddings"][found_index] = tensor_entry
        else:
             # Corrected f-string quote
             logging.info(f"Adding new entry for tensor '{sanitized_tensor_name}' to config.")
             config["embeddings"].append(tensor_entry)

        try:
            with open(config_file_path, 'w', encoding='utf-8') as f: # Use local variable for config path
                json.dump(config, f, indent=2)
            logging.info(f"Successfully updated config file {config_file_path}")
        except IOError as e:
             logging.error(f"IOError writing updated config file {config_file_path}: {e}")
             raise HTTPException(status_code=500, detail=f"Failed to write config file: {e}")
        except Exception as e:
             logging.exception(f"Unexpected error writing updated config file {config_file_path}")
             raise HTTPException(status_code=500, detail=f"Unexpected error writing config file: {str(e)}")


        # Corrected f-string quote
        return {"message": f"Successfully saved data for tensor '{sanitized_tensor_name}'", 
                "vector_file": os.path.basename(vector_file_path),
                "metadata_file": os.path.basename(metadata_file_path),
                "config_file": os.path.basename(config_file_path), # Use local variable for config path
                "vectors_saved": len(vectors)} # Changed key name to match JS

    except HTTPException as e:
        # Log the error detail that will be sent to the client
        logging.error(f"HTTP Exception during save_astra_data: Status={e.status_code}, Detail='{e.detail}'")
        raise e # Re-raise the HTTPException to be handled by FastAPI
    except Exception as e:
        logging.exception("Unexpected error during save_astra_data process") # Log full traceback
        # Return a generic 500 error to the client
        raise HTTPException(status_code=500, detail=f"An internal server error occurred. Check server logs for details.")

# --- Setup Static Files (CSS, JS, etc. - Mount Specific Paths First) ---
# Create static directory if it doesn't exist
if not os.path.exists(STATIC_DIR):
    os.makedirs(STATIC_DIR)
# Mount static files at /static path
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static_assets")

# --- Serve Static Files (Embedding Projector Root - Mount Last) ---
# Serve index.html and other root files from the main project directory
app.mount("/", StaticFiles(directory=ROOT_DIR, html=True), name="root_static")


# --- Startup/Shutdown Events (Removed Streamlit logic) ---
@app.on_event("startup")
async def startup_event():
    print("Server starting up...")
    # Any other startup logic can go here

@app.on_event("shutdown")
def shutdown_event():
    print("Server shutting down...")
    # Any cleanup logic can go here


if __name__ == "__main__":
    print(f"Starting server on http://{MAIN_SERVER_HOST}:{MAIN_SERVER_PORT}")
    print(f"Serving root static files from: {ROOT_DIR}")
    print(f"Serving asset static files from: {STATIC_DIR}")
    print(f"Astra helper available at: /astra")
    uvicorn.run(app, host=MAIN_SERVER_HOST, port=MAIN_SERVER_PORT) 