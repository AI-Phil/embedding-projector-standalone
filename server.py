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
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse
import numpy as np
from astrapy.table import Table
from astrapy.info import ColumnType, TableVectorColumnTypeDescriptor
from typing import Literal
import data_fetcher # Import the new module

# Configuration
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(ROOT_DIR, "static")
TEMPLATES_DIR = os.path.join(ROOT_DIR, "templates")
MAIN_SERVER_HOST = "0.0.0.0"
MAIN_SERVER_PORT = 8000

app = FastAPI()

# --- Added Exception Handler for Validation Errors --- 
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    # Log the details of the validation error
    logging.error(f"Validation error for request: {request.url}")
    logging.error(f"Error details: {exc.errors()}")
    # Return a standard validation error response
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors()},
    )
# --- End Added Exception Handler ---

# Request Models
class ConnectionInfo(BaseModel):
    endpoint_url: str
    token: str
    db_name: str
    keyspace: str | None = None

class SampleRequest(BaseModel):
    connection: ConnectionInfo
    collection_name: str

# Added Model for Table Sampling
class TableSampleRequest(BaseModel):
    connection: ConnectionInfo
    table_name: str
    vector_column: str # Name of the column containing the vector
# End Added Model

class SampleDataPayload(BaseModel):
    sample_data: list[dict]

class SaveConfigRequest(BaseModel):
    connection: ConnectionInfo
    tensor_name: str
    vector_dimension: int
    metadata_keys: list[str]
    document_limit: int | None = None
    collection_name: str | None = None
    table_name: str | None = None
    vector_column: str | None = None
    primary_key_columns: list[str] | None = None
    partition_key_columns: list[str] | None = None
    sampling_strategy: Literal["first_rows", "token_range"] = "first_rows"

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

@app.post("/api/astra/tables")
async def api_astra_get_tables(connection_info: ConnectionInfo):
    """API endpoint to list CQL tables with vector columns."""
    print(f"Received request for tables: Endpoint={connection_info.endpoint_url}, Keyspace: {connection_info.keyspace or 'default_keyspace'}")
    vector_tables_details = []
    try:
        db = get_data_api_client(connection_info)
        # Get table descriptors using list_tables()
        tables_result = db.list_tables()
        #print(f"Found {len(tables_result)} tables. Fetching full definition for each to check vector columns...")

        for table_desc in tables_result:
            table_name = table_desc.name
            #print(f"\nProcessing table: '{table_name}'")
            #print(f"  - Descriptor: {table_desc}")
            try:
                # Get the full Table object
                table = db.get_table(table_name)
                # Fetch the detailed definition (this should have column types)
                try:
                     full_definition = table.definition()
                     #print(f"  - Fetched Full Definition: {full_definition}")
                except AttributeError:
                     print(f"   - Skipping table '{table_name}': Cannot retrieve full definition (method missing).")
                     continue

                if not full_definition or not full_definition.columns or not full_definition.primary_key:
                    print(f" - Skipping table '{table_name}': Missing or incomplete full definition.")
                    continue

                # Now inspect the columns from the full_definition (expected to be a dict)
                vector_columns = []
                #print(f"  - Checking columns type: {type(full_definition.columns)}")
                if isinstance(full_definition.columns, dict):
                    #print(f"  - Iterating through {len(full_definition.columns)} columns in definition:")
                    for col_name, col_def in full_definition.columns.items():
                        #print(f"    - Checking column: '{col_name}', Definition: {col_def}")
                        # Check type attribute (should exist on full definition descriptors)
                        col_type_attr = getattr(col_def, 'type', None)
                        # Also check column_type for robustness (e.g., TableVectorColumnTypeDescriptor)
                        col_type_val = getattr(col_def, 'column_type', None) 
                        #print(f"      - col_type_attr: {col_type_attr} (Type: {type(col_type_attr)}) | col_type_val: {col_type_val}")

                        is_vector = False
                        # Check if the col_def object is an instance of the Vector Descriptor
                        if isinstance(col_def, TableVectorColumnTypeDescriptor):
                            #print(f"      - Matched isinstance(col_def, TableVectorColumnTypeDescriptor)")
                            is_vector = True
                        
                        #print(f"      - Is Vector: {is_vector}")
                        if is_vector:
                            dimension = getattr(col_def, 'dimension', None)
                            #print(f"      - Dimension: {dimension}")
                            if dimension:
                                vector_columns.append({"name": col_name, "dimension": dimension})
                            else:
                                print(f" - Warning: Vector column '{col_name}' in table '{table_name}' has no dimension specified.")
                else:
                     print(f" - Skipping table '{table_name}': Full definition columns attribute is not a dictionary ({type(full_definition.columns)}).")
                     continue # Skip if columns structure is unexpected

                if not vector_columns:
                    print(f" - Skipping table '{table_name}': No vector columns found in full definition.")
                    # print(f" - Full Definition (for debug): {full_definition}") # Optional debug print
                    continue

                # Extract primary key info from full definition
                # Ensure primary_key attribute exists and has expected sub-attributes
                pk_columns = []
                #print(f"  - Extracting primary keys from full definition: {getattr(full_definition, 'primary_key', 'N/A')}")
                if hasattr(full_definition.primary_key, 'partition_by'):
                     pk_columns.extend(full_definition.primary_key.partition_by or [])
                     #print(f"    - Added from partition_by: {full_definition.primary_key.partition_by}")
                if hasattr(full_definition.primary_key, 'partition_sort') and full_definition.primary_key.partition_sort:
                     pk_columns.extend(full_definition.primary_key.partition_sort.keys())
                     #print(f"    - Added from partition_sort: {list(full_definition.primary_key.partition_sort.keys())}")
                
                #print(f"  - Initial PK columns from full def: {pk_columns}")
                if not pk_columns:
                     # Fallback to PK info from the descriptor if needed, though full_definition should be primary
                     pk_desc = getattr(table_desc.definition, 'primary_key', None)
                     if pk_desc and hasattr(pk_desc, 'partition_by'): # Simplified check
                          pk_columns = pk_desc.partition_by
                          #print(f"    - Info: Used primary key from ListTableDescriptor for '{table_name}'. PK: {pk_columns}")
                     else: 
                          print(f"    - Warning: Could not determine primary key columns for table '{table_name}' from full definition or descriptor.")

                #print(f"  - Final PK columns for table '{table_name}': {pk_columns}")

                # Estimate count using the table object - REMOVED due to issues with estimated_document_count
                est_count = "N/A" 

                table_detail = {
                    "name": table_name,
                    "vector_columns": vector_columns,
                    "primary_key_columns": pk_columns,
                    "count": est_count
                }
                vector_tables_details.append(table_detail)
                print(f" - Found vector table: {table_name} (Vector Cols: {len(vector_columns)}, PK Cols: {len(pk_columns)}, Count: {est_count})")

            except AttributeError as ae:
                 print(f"Warning: Attribute error processing table '{table_name}': {ae}.")
            except Exception as inner_e:
                 print(f"Warning: Error processing table '{table_name}': {inner_e}")

        return {"tables": vector_tables_details}
    # Keep outer error handling the same
    except AttributeError as ae:
        # Handle case where list_tables might not exist
        if "'Database' object has no attribute 'list_tables'" in str(ae):
             print("Error: The `list_tables` method is not available on the Database object.")
             return JSONResponse(status_code=501, content={"error": "Listing tables is not supported by this version or setup of astrapy."})
        print(f"AttributeError encountered: {ae}")
        return JSONResponse(status_code=500, content={"error": f"An attribute error occurred: {ae}"})
    except ValueError as e:
        # Handle authentication errors etc.
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        print(f"Error listing tables via Data API: {e}")
        # Check for specific API command errors if possible
        if "Unknown command: listTables" in str(e):
             print("Error: Server does not support listTables command.")
             return JSONResponse(status_code=501, content={"error": "Listing tables command not supported by the Data API endpoint."})
        return JSONResponse(status_code=500, content={"error": f"An unexpected error occurred while listing tables: {e}"})

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

# Added Endpoint for Table Sampling
@app.post("/api/astra/sample_table")
async def api_astra_sample_table_data(sample_request: TableSampleRequest):
    """API endpoint to sample data from a CQL table via Data API."""
    print(f"Received request to sample table: {sample_request.table_name}, vector column: {sample_request.vector_column}")
    try:
        db = get_data_api_client(sample_request.connection)
        table = db.get_table(sample_request.table_name)

        # Fetch a small number of rows. Assume find() returns all columns by default.
        find_options = {"limit": 10}
        
        # --- Apply Filter --- 
        api_commander_logger = logging.getLogger('astrapy.utils.api_commander')
        zero_filter_suppressor = SuppressZeroFilterWarning()
        api_commander_logger.addFilter(zero_filter_suppressor)
        
        sample_docs_raw = []
        try:
            cursor = table.find(**find_options)
            sample_docs_raw = list(cursor)
        finally:
            # --- Remove Filter --- 
            api_commander_logger.removeFilter(zero_filter_suppressor)

        print(f"Sampled {len(sample_docs_raw)} rows via Data API from table '{sample_request.table_name}'.")

        sample_docs_processed = []
        for doc in sample_docs_raw:
            # Convert vector column if it's returned as DataAPIVector
            vector_col_name = sample_request.vector_column
            if vector_col_name in doc:
                if isinstance(doc[vector_col_name], DataAPIVector):
                    doc[vector_col_name] = list(doc[vector_col_name])
                # Add checks here if other vector types are possible (e.g., bytes)
            else:
                # Attempt to infer primary key for logging
                pk_val_str = "(PK not found)"
                # This logic assumes definition was fetched earlier or PK is simple _id
                # For robust PK logging, we might need definition here.
                # Trying a simple guess for now.
                if '_id' in doc:
                     pk_val_str = str(doc['_id'])
                elif doc:
                     pk_val_str = str(list(doc.values())[0]) # Fallback to first value

                logging.warning(f"Vector column '{vector_col_name}' not found in sampled row with PK/ID '{pk_val_str}'. Row keys: {list(doc.keys())}")

            sample_docs_processed.append(doc)

        return {"sample_data": sample_docs_processed}
    except ValueError as e:
        # Specific handling for authentication errors from get_data_api_client
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        # Catch potential errors from astrapy (e.g., table not found)
        # Check if the exception message indicates the table doesn't exist
        err_str = str(e).lower()
        if "table" in err_str and f"'{sample_request.table_name}'" in err_str and ("not found" in err_str or "does not exist" in err_str):
             logging.error(f"Table '{sample_request.table_name}' not found: {e}")
             return JSONResponse(status_code=404, content={"error": f"Table '{sample_request.table_name}' not found in keyspace."}) # Changed to 404

        logging.exception(f"Error sampling data from table '{sample_request.table_name}' via Data API")
        return JSONResponse(status_code=500, content={"error": f"An unexpected error occurred while sampling table: {e}"})
# End Added Endpoint

@app.post("/api/astra/save_data")
async def save_astra_data(request: SaveConfigRequest):
    """Fetches data (from collection OR table), saves vectors (.bytes) and metadata (.tsv), updates config."""
    logging.info(f"Received request to save data for tensor: {request.tensor_name}, Limit: {request.document_limit}, Strategy: {request.sampling_strategy}")

    local_astra_data_dir = os.path.join(ROOT_DIR, "astra_data")
    is_table_mode = bool(request.table_name and request.vector_column) # Simplified check for table mode

    # Validate inputs based on mode and strategy
    if is_table_mode:
        if not request.primary_key_columns:
             raise HTTPException(status_code=422, detail="primary_key_columns are required for table mode.")
        if request.sampling_strategy == "token_range":
            if not request.partition_key_columns:
                raise HTTPException(status_code=422, detail="partition_key_columns are required for token_range sampling strategy.")
            if not request.document_limit or request.document_limit <= 0:
                 raise HTTPException(status_code=422, detail="A positive document_limit is required for token_range sampling strategy.")
    elif request.collection_name is None:
         raise HTTPException(status_code=422, detail="collection_name is required when not in table mode.")
    
    # Ensure vector dimension is positive
    if request.vector_dimension <= 0:
         raise HTTPException(status_code=422, detail="vector_dimension must be a positive integer.")

    logging.info(f"Save mode: {'Table' if is_table_mode else 'Collection'}")

    try:
        db = get_data_api_client(request.connection)
        documents = []
        target_name = "Unknown"
        vector_key_name = ""

        # --- Determine Projection ---
        projection = {}
        if is_table_mode:
            target_name = request.table_name
            vector_key_name = request.vector_column
            projection[vector_key_name] = True
            # Ensure PK columns are included for processing later
            all_needed_keys = set(request.metadata_keys) | set(request.primary_key_columns)
            for key in all_needed_keys:
                projection[key] = True
            # Ensure partition keys are included if token range sampling (already covered by PK check above)
            if request.sampling_strategy == "token_range":
                 for key in request.partition_key_columns:
                      projection[key] = True
        else: # Collection mode
            target_name = request.collection_name
            vector_key_name = "$vector"
            projection[vector_key_name] = True
            for key in request.metadata_keys:
                projection[key] = True
            # Ensure _id is projected (collections implicitly have it, but explicit is safer)
            if '_id' not in projection:
                projection['_id'] = True

        logging.debug(f"Final projection: {projection}")

        # --- Fetch Data using Data Fetcher ---
        if is_table_mode and request.sampling_strategy == "token_range":
            logging.info(f"Using token_range strategy for table '{target_name}'")
            documents = await data_fetcher.fetch_data_token_range(
                db=db,
                table_name=target_name,
                partition_key_columns=request.partition_key_columns,
                projection=projection,
                total_limit=request.document_limit, # Already validated > 0
                vector_column=vector_key_name
            )
        else:
            # Default to "first_rows" strategy for collections or if specified for tables
            logging.info(f"Using first_rows strategy for {'table' if is_table_mode else 'collection'} '{target_name}'")
            find_options = {"projection": projection}
            # Apply limit only if using "first_rows" strategy and limit is provided
            if request.document_limit and request.document_limit > 0:
                 find_options["limit"] = request.document_limit
                 logging.info(f"Applying limit: {request.document_limit}")
            else:
                 logging.info("No limit applied for fetch_data_first_rows.")
            
            documents = await data_fetcher.fetch_data_first_rows(
                db=db,
                target_name=target_name,
                find_options=find_options,
                is_table_mode=is_table_mode,
                vector_key_name=vector_key_name
            )

        # --- Check if documents were found AFTER fetching ---
        if not documents:
            logging.warning(f"No documents found in '{target_name}' using the '{request.sampling_strategy}' strategy.")
            # Consider if this should be a 404 or just proceed and save empty files
            # Let's raise 404 for clarity, as processing will fail anyway
            raise HTTPException(status_code=404, detail=f"No documents found in '{target_name}'. Check source, filters, or sampling strategy.")

        # --- Prepare Output Directory ---
        try:
            os.makedirs(local_astra_data_dir, exist_ok=True)
            logging.info(f"Ensured output directory exists: {local_astra_data_dir}")
        except OSError as e:
            logging.error(f"Could not create output directory {local_astra_data_dir}: {e}")
            raise HTTPException(status_code=500, detail=f"Server configuration error: Could not create data directory.")

        # --- Prepare Filenames ---
        sanitized_tensor_name = request.tensor_name.replace(" ", "_")
        logging.info(f"Sanitized tensor name: '{request.tensor_name}' -> '{sanitized_tensor_name}'")
        safe_tensor_name = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in sanitized_tensor_name)
        if not safe_tensor_name:
            safe_tensor_name = "default_tensor"
            logging.warning(f"Sanitized tensor name '{sanitized_tensor_name}' resulted in empty safe name. Using '{safe_tensor_name}'.")
        vector_file_path = os.path.join(local_astra_data_dir, f"{safe_tensor_name}.bytes")
        metadata_file_path = os.path.join(local_astra_data_dir, f"{safe_tensor_name}_metadata.tsv")

        # --- Process Data ---
        vectors = []
        metadata_rows = []
        metadata_header = []
        
        # Determine metadata header based on mode and PK
        if is_table_mode:
             # Ensure primary_key_columns is not None (validated earlier)
             pk_cols = request.primary_key_columns
             if len(pk_cols) == 1:
                  pk_header = pk_cols[0]
                  metadata_header = [pk_header] + [k for k in request.metadata_keys if k != pk_header]
             else: # Multiple PK columns
                  pk_header = "PRIMARY_KEY" # Composite key identifier
                  metadata_header = [pk_header] + [k for k in request.metadata_keys if k not in pk_cols]
        else: # Collection mode
             pk_header = "_id"
             metadata_header = [pk_header] + [k for k in request.metadata_keys if k != pk_header]

        logging.info(f"Processing {len(documents)} documents. Vector key: '{vector_key_name}'. Metadata header: {metadata_header}")

        processed_doc_count = 0
        skipped_vector_count = 0
        skipped_dimension_count = 0
        skipped_pk_count = 0

        for doc in documents:
            # Get vector
            doc_vector = doc.get(vector_key_name)
            if doc_vector is None or not isinstance(doc_vector, (list, DataAPIVector)):
                 # Attempt to get PK for logging context
                 pk_for_log = "(PK lookup failed)"
                 try:
                      if is_table_mode and request.primary_key_columns:
                           if len(request.primary_key_columns) == 1:
                                pk_for_log = str(doc.get(request.primary_key_columns[0], "(missing)"))
                           else:
                                pk_parts = [str(doc.get(k, "(missing)")) for k in request.primary_key_columns]
                                pk_for_log = "_".join(pk_parts)
                      elif not is_table_mode:
                           pk_for_log = str(doc.get("_id", "(missing)"))
                 except Exception: pass # Ignore errors during logging lookup

                 logging.warning(f"Doc PK='{pk_for_log}': Missing or invalid vector type ({type(doc_vector)}). Vector key: '{vector_key_name}'. Skipping. Doc keys: {list(doc.keys())}")
                 skipped_vector_count += 1
                 continue

            # Ensure vector is a list
            if isinstance(doc_vector, DataAPIVector):
                 doc_vector = list(doc_vector)
            
            # Check dimension
            if len(doc_vector) != request.vector_dimension:
                 logging.warning(f"Document vector dimension ({len(doc_vector)}) mismatch. Expected {request.vector_dimension}. Skipping.")
                 skipped_dimension_count += 1
                 continue
                 
            # Try converting to float32 numpy array
            try:
                 np_vector = np.array(doc_vector, dtype=np.float32)
            except ValueError as ve:
                 logging.warning(f"Document vector could not be converted to float32 array: {ve}. Skipping.")
                 skipped_vector_count += 1 # Count as vector issue
                 continue

            # Generate primary key string for metadata
            pk_value_str = ""
            missing_pk = False
            if is_table_mode:
                 pk_cols = request.primary_key_columns # Already checked not None
                 if len(pk_cols) == 1:
                      pk_val = doc.get(pk_cols[0])
                      if pk_val is None:
                           logging.warning(f"Document missing primary key value for '{pk_cols[0]}'. Skipping.")
                           missing_pk = True
                      else:
                           pk_value_str = str(pk_val)
                 else: # Multiple PKs
                      pk_parts = []
                      missing_part = False
                      for pk_col_name in pk_cols:
                           part_val = doc.get(pk_col_name)
                           if part_val is None:
                                logging.warning(f"Document missing composite primary key part '{pk_col_name}'. Skipping.")
                                missing_part = True
                                break
                           # Sanitize parts slightly before joining
                           pk_parts.append(str(part_val).replace('_', '-').replace('\\t', ' ').replace('\\n', ' ').replace('\\r', ' '))
                      if missing_part:
                           missing_pk = True
                      else:
                           pk_value_str = "_".join(pk_parts) # This is the value for the composite PRIMARY_KEY column
            else: # Collection mode
                 _id_val = doc.get("_id")
                 if _id_val is None:
                     logging.warning(f"Document missing '_id'. Skipping.")
                     missing_pk = True
                 else:
                     pk_value_str = str(_id_val)

            if missing_pk:
                skipped_pk_count += 1
                continue # Skip doc if PK is missing

            # Add vector to list
            vectors.append(np_vector)

            # Build metadata row
            row_data = []
            for key in metadata_header:
                 if key == pk_header: # Handle the explicitly generated PK string (single or composite)
                     value_str = pk_value_str.replace('\\t', ' ').replace('\\n', ' ').replace('\\r', ' ')
                 else:
                     # For other keys, get value directly from doc
                     value = doc.get(key, '') # Default to empty string if metadata key is missing
                     value_str = str(value).replace('\\t', ' ').replace('\\n', ' ').replace('\\r', ' ')
                 row_data.append(value_str)
            metadata_rows.append("\t".join(row_data))
            processed_doc_count += 1

        # --- Log processing summary ---
        logging.info(f"Processed {processed_doc_count} documents. Skipped: {skipped_vector_count} (vector issue), {skipped_dimension_count} (dimension issue), {skipped_pk_count} (PK issue).")

        if not vectors:
            error_detail = "No valid vector data found after processing."
            if skipped_vector_count > 0 or skipped_dimension_count > 0 or skipped_pk_count > 0:
                 error_detail += f" Skipped docs breakdown: VectorIssue={skipped_vector_count}, DimensionIssue={skipped_dimension_count}, PKIssue={skipped_pk_count}."
            logging.error(error_detail)
            raise HTTPException(status_code=400, detail=error_detail)

        # --- Save Files ---
        vector_data = np.array(vectors) # Re-create numpy array from list of arrays
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

        # --- Update Config File ---
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
            "tensorPath": os.path.relpath(vector_file_path, ROOT_DIR).replace('\\', '/'), # Ensure forward slashes
            "metadataPath": os.path.relpath(metadata_file_path, ROOT_DIR).replace('\\', '/') # Ensure forward slashes
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

        # --- Return Success Response ---
        config_relative_path = os.path.relpath(config_file_path, ROOT_DIR).replace('\\\\', '/') # Ensure forward slashes
        logging.info(f"Returning config path relative to root: {config_relative_path}")
        return {"message": f"Successfully saved data for tensor '{sanitized_tensor_name}' using '{request.sampling_strategy}' strategy",
                "vector_file": os.path.basename(vector_file_path),
                "metadata_file": os.path.basename(metadata_file_path),
                "config_file": config_relative_path,
                "vectors_saved": len(vectors),
                "limit_applied": request.document_limit if request.sampling_strategy == 'token_range' or (request.document_limit and request.document_limit > 0) else None,
                "tensor_name": sanitized_tensor_name,
                "tensor_shape": [len(vectors), request.vector_dimension],
                "tensor_path_rel": os.path.relpath(vector_file_path, ROOT_DIR).replace('\\\\', '/'),
                "metadata_path_rel": os.path.relpath(metadata_file_path, ROOT_DIR).replace('\\\\', '/')
                }

    except HTTPException as e:
        # Log and re-raise HTTPExceptions (like 404, 422, etc.)
        logging.error(f"HTTP Exception during save_astra_data: Status={e.status_code}, Detail='{e.detail}'")
        raise e
    except ValueError as e:
         # Catch ValueErrors from get_data_api_client or data_fetcher validation
         logging.error(f"ValueError during save_astra_data: {e}")
         # Return as 400 Bad Request or 422 Unprocessable Entity
         status_code = 400
         if "Partition key columns must be provided" in str(e) or "Total limit must be positive" in str(e):
              status_code = 422 
         elif "Authentication failed" in str(e) or "Failed to connect" in str(e):
              status_code = 400 # Keep as bad request for connection issues
         raise HTTPException(status_code=status_code, detail=str(e)) from e
    except Exception as e:
        # Catch any other unexpected errors
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

# --- Custom Logging Filter --- 
class SuppressZeroFilterWarning(logging.Filter):
    def filter(self, record):
        # Check if the specific warning code is in the message
        # Adapt this check if the exact formatting changes in astrapy versions
        return 'ZERO_FILTER_OPERATIONS' not in record.getMessage()
# --- End Filter Definition ---

if __name__ == "__main__":
    print(f"Starting server on http://{MAIN_SERVER_HOST}:{MAIN_SERVER_PORT}")
    print(f"Serving root static files from: {ROOT_DIR}")
    print(f"Serving asset static files from: {STATIC_DIR}")
    print(f"Astra helper available at: /astra")
    uvicorn.run(app, host=MAIN_SERVER_HOST, port=MAIN_SERVER_PORT) 