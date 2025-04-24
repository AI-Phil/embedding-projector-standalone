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
STATIC_DIR = os.path.join(ROOT_DIR, "static")
TEMPLATES_DIR = os.path.join(ROOT_DIR, "templates")
MAIN_SERVER_HOST = "0.0.0.0"
MAIN_SERVER_PORT = 8000

app = FastAPI()

# Request Models
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
    document_limit: int | None = None

# Global DataAPIClient cache
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

# Setup Templates
if not os.path.exists(TEMPLATES_DIR):
    os.makedirs(TEMPLATES_DIR)
templates = Jinja2Templates(directory=TEMPLATES_DIR)

@app.get("/astra", response_class=HTMLResponse, include_in_schema=False)
@app.get("/astra/", response_class=HTMLResponse)
async def get_astra_page(request: Request):
    """Serves the main HTML page for the Astra helper app."""
    return templates.TemplateResponse("astra.html", {"request": request})

# API Routes
@app.post("/api/astra/collections")
async def api_astra_get_collections(connection_info: ConnectionInfo):
    """API endpoint to list vector-enabled collections and their dimensions."""
    print(f"Received request for collections: Endpoint={connection_info.endpoint_url}, Keyspace: {connection_info.keyspace or 'default_keyspace'}")
    vector_collections_details = []
    try:
        db = get_data_api_client(connection_info)
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

                    if vector_options and (dimension or service_options):
                        est_count = "N/A"
                        try:
                            collection_obj = db.get_collection(col_name)
                            count_result = collection_obj.estimated_document_count()
                            if isinstance(count_result, int):
                                est_count = count_result
                                print(f" - Collection '{col_name}': Estimated count = {est_count}")
                            else:
                                print(f" - Collection '{col_name}': Could not parse count from result: {count_result}")
                                est_count = "Unknown"
                        except Exception as count_e:
                            print(f" - Warning: Could not get estimated count for '{col_name}': {count_e}")
                            est_count = "Error"
                            
                        collection_detail = {"name": col_name, "dimension": dimension or 0, "count": est_count}
                        
                        if dimension:
                            print(f" - Found vector collection: {col_name} (Dimension: {dimension}, Count: {est_count})")
                            vector_collections_details.append(collection_detail)
                        else:
                            print(f" - Found vectorize collection (no dimension specified): {col_name} (Count: {est_count})")
                            vector_collections_details.append(collection_detail)
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
        print(f"Error listing collections via Data API: {e}")
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
        db = get_data_api_client(sample_request.connection)
        collection = db.get_collection(sample_request.collection_name)
        cursor = collection.find(limit=10, projection={"$vector": True}) 
        sample_docs_raw = list(cursor)
        print(f"Sampled {len(sample_docs_raw)} documents via Data API.")

        sample_docs_processed = []
        for doc in sample_docs_raw:
            if "$vector" in doc and isinstance(doc["$vector"], DataAPIVector):
                doc["$vector"] = list(doc["$vector"]) 
            sample_docs_processed.append(doc)

        return {"sample_data": sample_docs_processed}
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        print(f"Error sampling data via Data API: {e}")
        return JSONResponse(status_code=500, content={"error": f"An unexpected error occurred while sampling: {e}"})

@app.post("/api/astra/save_data")
async def save_astra_data(request: SaveConfigRequest):
    """Fetches data, saves vectors (.bytes) and metadata (.tsv), updates config."""
    logging.info(f"Received request to save data for tensor: {request.tensor_name}, Limit: {request.document_limit}")
    
    local_astra_data_dir = os.path.join(ROOT_DIR, "astra_data")
    
    try:
        db = get_data_api_client(request.connection)
        collection = db.get_collection(request.collection_name)

        projection = {"$vector": True}
        for key in request.metadata_keys:
             projection[key] = True 
        
        find_options = {"projection": projection}
        if request.document_limit and request.document_limit > 0:
            find_options["limit"] = request.document_limit
            logging.info(f"Fetching documents from {request.collection_name} with projection and limit: {find_options}")
        else:
            logging.info(f"Fetching documents from {request.collection_name} with projection (no limit): {projection}")

        documents = list(collection.find(**find_options))

        if not documents:
            logging.warning(f"No documents found in collection '{request.collection_name}' with projection.")
            raise HTTPException(status_code=404, detail=f"No documents found in the collection '{request.collection_name}'. Check if the collection is empty or the projection filters out all docs.")

        try:
            os.makedirs(local_astra_data_dir, exist_ok=True)
            logging.info(f"Ensured output directory exists: {local_astra_data_dir}")
        except OSError as e:
            logging.error(f"Could not create output directory {local_astra_data_dir}: {e}")
            raise HTTPException(status_code=500, detail=f"Server configuration error: Could not create data directory.")

        sanitized_tensor_name = request.tensor_name.replace(" ", "_")
        logging.info(f"Sanitized tensor name: '{request.tensor_name}' -> '{sanitized_tensor_name}'")

        safe_tensor_name = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in sanitized_tensor_name)
        if not safe_tensor_name:
            safe_tensor_name = "default_tensor"
            logging.warning(f"Sanitized tensor name '{sanitized_tensor_name}' resulted in empty safe name. Using '{safe_tensor_name}'.")

        vector_file_path = os.path.join(local_astra_data_dir, f"{safe_tensor_name}.bytes")
        metadata_file_path = os.path.join(local_astra_data_dir, f"{safe_tensor_name}_metadata.tsv")

        vectors = []
        metadata_rows = []
        metadata_header = ["_id"] + [key for key in request.metadata_keys if key != "_id"]
        logging.info(f"Processing {len(documents)} documents. Metadata header: {metadata_header}")

        processed_doc_count = 0
        skipped_vector_count = 0
        skipped_dimension_count = 0

        for doc in documents:
            doc_id = doc.get('_id', 'UNKNOWN_ID')
            
            if "$vector" not in doc or not isinstance(doc["$vector"], (list, DataAPIVector)):
                 logging.warning(f"Document {doc_id} missing or has invalid $vector type ({type(doc.get('$vector'))}). Skipping.")
                 skipped_vector_count += 1
                 continue

            if isinstance(doc["$vector"], DataAPIVector):
                 doc["$vector"] = list(doc["$vector"])
            
            if len(doc["$vector"]) != request.vector_dimension:
                 logging.warning(f"Document {doc_id} vector dimension ({len(doc['$vector'])}) mismatch. Expected {request.vector_dimension}. Skipping.")
                 skipped_dimension_count += 1
                 continue
                 
            try:
                 vectors.append(np.array(doc["$vector"], dtype=np.float32))
            except ValueError as ve:
                 logging.warning(f"Document {doc_id} vector could not be converted to float32 array: {ve}. Skipping.")
                 skipped_vector_count += 1
                 continue

            row_data = []
            for key in metadata_header:
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

        vector_data = np.array(vectors)
        logging.info(f"Attempting to save {len(vectors)} vectors ({vector_data.nbytes} bytes) to {vector_file_path}")
        try:
            with open(vector_file_path, 'wb') as vf:
                 vf.write(vector_data.tobytes())
            logging.info(f"Successfully saved vectors to {vector_file_path}")
        except IOError as e:
             logging.error(f"IOError saving vector file {vector_file_path}: {e}")
             raise HTTPException(status_code=500, detail=f"Could not write vector file: {e}")
        except Exception as e:
            logging.exception(f"Unexpected error saving vector file {vector_file_path}")
            raise HTTPException(status_code=500, detail=f"Unexpected error writing vector file: {str(e)}")

        logging.info(f"Attempting to save metadata for {len(metadata_rows)} documents to {metadata_file_path}")
        try:
            with open(metadata_file_path, 'w', encoding='utf-8') as mf:
                mf.write("\t".join(metadata_header) + "\n")
                mf.write("\n".join(metadata_rows))
            logging.info(f"Successfully saved metadata to {metadata_file_path}")
        except IOError as e:
             logging.error(f"IOError saving metadata file {metadata_file_path}: {e}")
             raise HTTPException(status_code=500, detail=f"Could not write metadata file: {e}")
        except Exception as e:
            logging.exception(f"Unexpected error saving metadata file {metadata_file_path}")
            raise HTTPException(status_code=500, detail=f"Unexpected error writing metadata file: {str(e)}")

        config = {}
        config_file_path = os.path.join(local_astra_data_dir, "astra_projector_config.json")
        logging.info(f"Attempting to read and update config file: {config_file_path}")
        if os.path.exists(config_file_path):
            try:
                with open(config_file_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    if not isinstance(config, dict):
                        logging.warning(f"Config file {config_file_path} does not contain a JSON object. Resetting.")
                        config = {}
            except json.JSONDecodeError as e:
                logging.warning(f"Could not decode existing config file {config_file_path}: {e}. Starting fresh.")
                config = {}
            except Exception as e:
                 logging.error(f"Error reading config file {config_file_path}: {e}. Starting fresh.")
                 config = {}
        else:
             logging.info(f"Config file {config_file_path} not found. Creating new one.")
        
        if "embeddings" not in config or not isinstance(config.get("embeddings"), list):
             logging.warning("Config 'embeddings' key missing or not a list. Initializing.")
             config["embeddings"] = []

        tensor_entry = {
            "tensorName": sanitized_tensor_name, 
            "tensorShape": [len(vectors), request.vector_dimension],
            "tensorPath": os.path.relpath(vector_file_path, ROOT_DIR), 
            "metadataPath": os.path.relpath(metadata_file_path, ROOT_DIR)
        }

        found_index = -1
        for i, entry in enumerate(config["embeddings"]):
            if isinstance(entry, dict) and entry.get("tensorName") == sanitized_tensor_name:
                found_index = i
                break
        
        if found_index != -1:
             logging.info(f"Removing existing entry for tensor '{sanitized_tensor_name}' from index {found_index}.")
             del config["embeddings"][found_index]
        
        logging.info(f"Inserting entry for tensor '{sanitized_tensor_name}' at the beginning of the config list.")
        config["embeddings"].insert(0, tensor_entry)

        try:
            with open(config_file_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2)
            logging.info(f"Successfully updated config file {config_file_path}")
        except IOError as e:
             logging.error(f"IOError writing updated config file {config_file_path}: {e}")
             raise HTTPException(status_code=500, detail=f"Failed to write config file: {e}")
        except Exception as e:
             logging.exception(f"Unexpected error writing updated config file {config_file_path}")
             raise HTTPException(status_code=500, detail=f"Unexpected error writing config file: {str(e)}")

        config_relative_path = os.path.relpath(config_file_path, ROOT_DIR)
        logging.info(f"Returning config path relative to root: {config_relative_path}")
        return {"message": f"Successfully saved data for tensor '{sanitized_tensor_name}'", 
                "vector_file": os.path.basename(vector_file_path),
                "metadata_file": os.path.basename(metadata_file_path),
                "config_file": config_relative_path, 
                "vectors_saved": len(vectors),
                "limit_applied": request.document_limit
                }

    except HTTPException as e:
        logging.error(f"HTTP Exception during save_astra_data: Status={e.status_code}, Detail='{e.detail}'")
        raise e
    except Exception as e:
        logging.exception("Unexpected error during save_astra_data process")
        raise HTTPException(status_code=500, detail=f"An internal server error occurred. Check server logs for details.")

# Setup Static Files (CSS, JS, etc. - Mount Specific Paths First)
if not os.path.exists(STATIC_DIR):
    os.makedirs(STATIC_DIR)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static_assets")

# Serve Static Files (Embedding Projector Root - Mount Last)
app.mount("/", StaticFiles(directory=ROOT_DIR, html=True), name="root_static")

@app.on_event("startup")
async def startup_event():
    print("Server starting up...")

@app.on_event("shutdown")
def shutdown_event():
    print("Server shutting down...")

if __name__ == "__main__":
    print(f"Starting server on http://{MAIN_SERVER_HOST}:{MAIN_SERVER_PORT}")
    print(f"Serving root static files from: {ROOT_DIR}")
    print(f"Serving asset static files from: {STATIC_DIR}")
    print(f"Astra helper available at: /astra")
    uvicorn.run(app, host=MAIN_SERVER_HOST, port=MAIN_SERVER_PORT) 