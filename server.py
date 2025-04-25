import os
import uvicorn
from fastapi import FastAPI, Request, File, UploadFile, Form
from fastapi.responses import HTMLResponse, JSONResponse, Response
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
from typing import Literal, List, Optional
import data_fetcher # Import the new module
import pandas as pd
import io
import ast # For literal_eval
import random # For sampling
import re # For splitting non-bracketed strings
import csv # Need for quoting constants
# import csv # Remove Sniffer import

# Configure logging to show INFO messages
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

# Configure logging to suppress python-multipart debug messages
logging.getLogger('python-multipart').setLevel(logging.WARNING)

# Server configuration settings
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(ROOT_DIR, "static")
TEMPLATES_DIR = os.path.join(ROOT_DIR, "templates")
MAIN_SERVER_HOST = "0.0.0.0"
MAIN_SERVER_PORT = 8000

app = FastAPI()

# Custom exception handler for validation errors
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors from Pydantic models.
    
    Logs the validation error details and returns a standardized JSON response.
    """
    logging.error(f"Validation error for request: {request.url}")
    logging.error(f"Error details: {exc.errors()}")
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors()},
    )

# Request Models
class ConnectionInfo(BaseModel):
    """Connection details for Astra DB."""
    endpoint_url: str
    token: str
    db_name: str
    keyspace: str | None = None

class SampleRequest(BaseModel):
    """Request model for sampling collection data."""
    connection: ConnectionInfo
    collection_name: str

class TableSampleRequest(BaseModel):
    """Request model for sampling table data."""
    connection: ConnectionInfo
    table_name: str
    vector_column: str # Name of the column containing the vector

class SampleDataPayload(BaseModel):
    """Payload containing sample data for metadata key analysis."""
    sample_data: list[dict]

class SaveConfigRequest(BaseModel):
    """Request model for saving tensor configuration and data."""
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
    sampling_strategy: Literal["first_rows", "token_range", "distributed"] = "first_rows"

class FileProcessRequest(BaseModel):
    filename: str 
    tensorName: str
    vectorColumnName: str 
    selectedMetadataColumns: List[str] 
    samplingStrategy: Literal["all", "first_n", "random_n"] = "all"
    limit: Optional[int] = None

# Global DataAPIClient cache
astra_data_api_clients = {}

# Pydantic models for file processing configuration
def get_data_api_client(info: ConnectionInfo) -> Database:
    """Get or create a Database instance via DataAPIClient.
    
    Maintains a cache of database connections to avoid creating new ones
    for the same endpoint and keyspace.
    
    Args:
        info: Connection details including endpoint URL, token, and keyspace
        
    Returns:
        Database instance for the specified connection
        
    Raises:
        ValueError: If authentication fails or connection cannot be established
    """
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
    """Serve the main HTML page for the Astra helper app."""
    return templates.TemplateResponse("astra.html", {"request": request})

@app.get("/file", response_class=HTMLResponse, include_in_schema=False)
@app.get("/file/", response_class=HTMLResponse)
async def get_file_page(request: Request):
    """Serve the main HTML page for file upload and processing."""
    return templates.TemplateResponse("file.html", {"request": request})

# API Routes
@app.post("/api/astra/collections")
async def api_astra_get_collections(connection_info: ConnectionInfo):
    """List vector-enabled collections and their dimensions.
    
    Args:
        connection_info: Database connection details
        
    Returns:
        JSON response containing list of vector collections with their dimensions
        and estimated document counts
    """
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
    """List CQL tables with vector columns.
    
    Args:
        connection_info: Database connection details
        
    Returns:
        JSON response containing list of tables with vector columns, their dimensions,
        and primary key information
    """
    print(f"Received request for tables: Endpoint={connection_info.endpoint_url}, Keyspace: {connection_info.keyspace or 'default_keyspace'}")
    vector_tables_details = []
    try:
        db = get_data_api_client(connection_info)
        tables_result = db.list_tables()

        for table_desc in tables_result:
            table_name = table_desc.name
            try:
                table = db.get_table(table_name)
                try:
                     full_definition = table.definition()
                except AttributeError:
                     print(f"   - Skipping table '{table_name}': Cannot retrieve full definition (method missing).")
                     continue

                if not full_definition or not full_definition.columns or not full_definition.primary_key:
                    print(f" - Skipping table '{table_name}': Missing or incomplete full definition.")
                    continue

                vector_columns = []
                if isinstance(full_definition.columns, dict):
                    for col_name, col_def in full_definition.columns.items():
                        col_type_attr = getattr(col_def, 'type', None)
                        col_type_val = getattr(col_def, 'column_type', None) 

                        is_vector = False
                        if isinstance(col_def, TableVectorColumnTypeDescriptor):
                            is_vector = True
                        
                        if is_vector:
                            dimension = getattr(col_def, 'dimension', None)
                            if dimension:
                                vector_columns.append({"name": col_name, "dimension": dimension})
                            else:
                                print(f" - Warning: Vector column '{col_name}' in table '{table_name}' has no dimension specified.")
                else:
                     print(f" - Skipping table '{table_name}': Full definition columns attribute is not a dictionary ({type(full_definition.columns)}).")
                     continue

                if not vector_columns:
                    print(f" - Skipping table '{table_name}': No vector columns found in full definition.")
                    continue

                pk_columns = []
                if hasattr(full_definition.primary_key, 'partition_by'):
                     pk_columns.extend(full_definition.primary_key.partition_by or [])
                if hasattr(full_definition.primary_key, 'partition_sort') and full_definition.primary_key.partition_sort:
                     pk_columns.extend(full_definition.primary_key.partition_sort.keys())
                
                if not pk_columns:
                     pk_desc = getattr(table_desc.definition, 'primary_key', None)
                     if pk_desc and hasattr(pk_desc, 'partition_by'):
                          pk_columns = pk_desc.partition_by
                     else: 
                          print(f"    - Warning: Could not determine primary key columns for table '{table_name}' from full definition or descriptor.")

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
    except AttributeError as ae:
        if "'Database' object has no attribute 'list_tables'" in str(ae):
             print("Error: The `list_tables` method is not available on the Database object.")
             return JSONResponse(status_code=501, content={"error": "Listing tables is not supported by this version or setup of astrapy."})
        print(f"AttributeError encountered: {ae}")
        return JSONResponse(status_code=500, content={"error": f"An attribute error occurred: {ae}"})
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        print(f"Error listing tables via Data API: {e}")
        if "Unknown command: listTables" in str(e):
             print("Error: Server does not support listTables command.")
             return JSONResponse(status_code=501, content={"error": "Listing tables command not supported by the Data API endpoint."})
        return JSONResponse(status_code=500, content={"error": f"An unexpected error occurred while listing tables: {e}"})

@app.post("/api/astra/metadata_keys")
async def api_astra_get_metadata_keys(payload: SampleDataPayload):
    """Analyze sample data to suggest metadata keys.
    
    Args:
        payload: Contains sample documents to analyze
        
    Returns:
        JSON response with list of potential metadata keys found in the sample data
    """
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
    """Sample data from a collection via Data API.
    
    Args:
        sample_request: Contains connection info and collection name
        
    Returns:
        JSON response with sample documents from the collection
    """
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

@app.post("/api/astra/sample_table")
async def api_astra_sample_table_data(sample_request: TableSampleRequest):
    """Sample data from a CQL table via Data API.
    
    Args:
        sample_request: Contains connection info, table name, and vector column name
        
    Returns:
        JSON response with sample rows from the table
    """
    print(f"Received request to sample table: {sample_request.table_name}, vector column: {sample_request.vector_column}")
    try:
        db = get_data_api_client(sample_request.connection)
        table = db.get_table(sample_request.table_name)

        find_options = {"limit": 10}
        
        api_commander_logger = logging.getLogger('astrapy.utils.api_commander')
        zero_filter_suppressor = SuppressZeroFilterWarning()
        api_commander_logger.addFilter(zero_filter_suppressor)
        
        sample_docs_raw = []
        try:
            cursor = table.find(**find_options)
            sample_docs_raw = list(cursor)
        finally:
            api_commander_logger.removeFilter(zero_filter_suppressor)

        print(f"Sampled {len(sample_docs_raw)} rows via Data API from table '{sample_request.table_name}'.")

        sample_docs_processed = []
        for doc in sample_docs_raw:
            vector_col_name = sample_request.vector_column
            if vector_col_name in doc:
                if isinstance(doc[vector_col_name], DataAPIVector):
                    doc[vector_col_name] = list(doc[vector_col_name])
            else:
                pk_val_str = "(PK not found)"
                if '_id' in doc:
                     pk_val_str = str(doc['_id'])
                elif doc:
                     pk_val_str = str(list(doc.values())[0])

                logging.warning(f"Vector column '{vector_col_name}' not found in sampled row with PK/ID '{pk_val_str}'. Row keys: {list(doc.keys())}")

            sample_docs_processed.append(doc)

        return {"sample_data": sample_docs_processed}
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        err_str = str(e).lower()
        if "table" in err_str and f"'{sample_request.table_name}'" in err_str and ("not found" in err_str or "does not exist" in err_str):
             logging.error(f"Table '{sample_request.table_name}' not found: {e}")
             return JSONResponse(status_code=404, content={"error": f"Table '{sample_request.table_name}' not found in keyspace."})

        logging.exception(f"Error sampling data from table '{sample_request.table_name}' via Data API")
        return JSONResponse(status_code=500, content={"error": f"An unexpected error occurred while sampling table: {e}"})

@app.post("/api/astra/save_data")
async def save_astra_data(request: SaveConfigRequest):
    """Save tensor data and configuration from Astra DB.
    
    This endpoint handles saving vector data and metadata from either collections
    or tables, generating the necessary files for the embedding projector.
    
    Args:
        request: Configuration details including connection info, tensor name,
                vector dimension, metadata keys, and sampling strategy
        
    Returns:
        JSON response with details about the saved data and generated configuration
    """
    logging.info(f"Received request to save data for tensor: {request.tensor_name}, Limit: {request.document_limit}, Strategy: {request.sampling_strategy}")

    local_astra_data_dir = os.path.join(ROOT_DIR, "astra_data")
    is_table_mode = bool(request.table_name and request.vector_column)

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
    elif request.sampling_strategy == "distributed":
        if not request.document_limit or request.document_limit <= 0:
            raise HTTPException(status_code=422, detail="A positive document_limit is required for distributed sampling strategy.")
    
    if request.vector_dimension <= 0:
         raise HTTPException(status_code=422, detail="vector_dimension must be a positive integer.")

    logging.info(f"Save mode: {'Table' if is_table_mode else 'Collection'}")

    try:
        db = get_data_api_client(request.connection)
        documents = []
        target_name = "Unknown"
        vector_key_name = ""

        # Determine projection based on mode
        projection = {}
        if is_table_mode:
            target_name = request.table_name
            vector_key_name = request.vector_column
            projection[vector_key_name] = True
            all_needed_keys = set(request.metadata_keys) | set(request.primary_key_columns)
            for key in all_needed_keys:
                projection[key] = True
            if request.sampling_strategy == "token_range":
                 for key in request.partition_key_columns:
                      projection[key] = True
        else:
            target_name = request.collection_name
            vector_key_name = "$vector"
            projection[vector_key_name] = True
            for key in request.metadata_keys:
                projection[key] = True
            if '_id' not in projection:
                projection['_id'] = True

        logging.debug(f"Final projection: {projection}")

        # Fetch data using appropriate strategy
        if is_table_mode and request.sampling_strategy == "token_range":
            logging.info(f"Using token_range strategy for table '{target_name}'")
            documents = await data_fetcher.fetch_data_token_range(
                db=db,
                table_name=target_name,
                partition_key_columns=request.partition_key_columns,
                projection=projection,
                total_limit=request.document_limit,
                vector_column=vector_key_name
            )
        elif not is_table_mode and request.sampling_strategy == "distributed":
            logging.info(f"Using distributed strategy for collection '{target_name}'")
            documents = await data_fetcher.fetch_data_distributed(
                db=db,
                collection_name=target_name,
                projection=projection,
                total_limit=request.document_limit,
                vector_key_name=vector_key_name
            )
        else:
            logging.info(f"Using first_rows strategy for {'table' if is_table_mode else 'collection'} '{target_name}'")
            find_options = {"projection": projection}
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

        if not documents:
            logging.warning(f"No documents found in '{target_name}' using the '{request.sampling_strategy}' strategy.")
            raise HTTPException(status_code=404, detail=f"No documents found in '{target_name}'. Check source, filters, or sampling strategy.")

        # Prepare output directory
        try:
            os.makedirs(local_astra_data_dir, exist_ok=True)
            logging.info(f"Ensured output directory exists: {local_astra_data_dir}")
        except OSError as e:
            logging.error(f"Could not create output directory {local_astra_data_dir}: {e}")
            raise HTTPException(status_code=500, detail=f"Server configuration error: Could not create data directory.")

        # Prepare filenames
        sanitized_tensor_name = request.tensor_name.replace(" ", "_")
        logging.info(f"Sanitized tensor name: '{request.tensor_name}' -> '{sanitized_tensor_name}'")
        safe_tensor_name = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in sanitized_tensor_name)
        if not safe_tensor_name:
            safe_tensor_name = "default_tensor"
            logging.warning(f"Sanitized tensor name '{sanitized_tensor_name}' resulted in empty safe name. Using '{safe_tensor_name}'.")
        vector_file_path = os.path.join(local_astra_data_dir, f"{safe_tensor_name}.bytes")
        metadata_file_path = os.path.join(local_astra_data_dir, f"{safe_tensor_name}_metadata.tsv")

        # Process data
        vectors = []
        metadata_rows = []
        metadata_header = []
        
        # Determine metadata header based on mode and PK
        if is_table_mode:
             pk_cols = request.primary_key_columns
             if len(pk_cols) == 1:
                  pk_header = pk_cols[0]
                  metadata_header = [pk_header] + [k for k in request.metadata_keys if k != pk_header]
             else:
                  pk_header = "PRIMARY_KEY"
                  metadata_header = [pk_header] + [k for k in request.metadata_keys if k not in pk_cols]
        else:
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
                 except Exception: pass

                 logging.warning(f"Doc PK='{pk_for_log}': Missing or invalid vector type ({type(doc_vector)}). Vector key: '{vector_key_name}'. Skipping. Doc keys: {list(doc.keys())}")
                 skipped_vector_count += 1
                 continue

            if isinstance(doc_vector, DataAPIVector):
                 doc_vector = list(doc_vector)
            
            if len(doc_vector) != request.vector_dimension:
                 logging.warning(f"Document vector dimension ({len(doc_vector)}) mismatch. Expected {request.vector_dimension}. Skipping.")
                 skipped_dimension_count += 1
                 continue
                 
            try:
                 np_vector = np.array(doc_vector, dtype=np.float32)
            except ValueError as ve:
                 logging.warning(f"Document vector could not be converted to float32 array: {ve}. Skipping.")
                 skipped_vector_count += 1
                 continue

            # Generate primary key string for metadata
            pk_value_str = ""
            missing_pk = False
            if is_table_mode:
                 pk_cols = request.primary_key_columns
                 if len(pk_cols) == 1:
                      pk_val = doc.get(pk_cols[0])
                      if pk_val is None:
                           logging.warning(f"Document missing primary key value for '{pk_cols[0]}'. Skipping.")
                           missing_pk = True
                      else:
                           pk_value_str = str(pk_val)
                 else:
                      pk_parts = []
                      missing_part = False
                      for pk_col_name in pk_cols:
                           part_val = doc.get(pk_col_name)
                           if part_val is None:
                                logging.warning(f"Document missing composite primary key part '{pk_col_name}'. Skipping.")
                                missing_part = True
                                break
                           pk_parts.append(str(part_val).replace('_', '-').replace('\\t', ' ').replace('\\n', ' ').replace('\\r', ' '))
                      if missing_part:
                           missing_pk = True
                      else:
                           pk_value_str = "_".join(pk_parts)
            else:
                 _id_val = doc.get("_id")
                 if _id_val is None:
                     logging.warning(f"Document missing '_id'. Skipping.")
                     missing_pk = True
                 else:
                     pk_value_str = str(_id_val)

            if missing_pk:
                skipped_pk_count += 1
                continue

            vectors.append(np_vector)

            # Build metadata row
            row_data = []
            for key in metadata_header:
                 if key == pk_header:
                     value_str = pk_value_str.replace('\\t', ' ').replace('\\n', ' ').replace('\\r', ' ')
                 else:
                     value = doc.get(key, '')
                     value_str = str(value).replace('\\t', ' ').replace('\\n', ' ').replace('\\r', ' ')
                 row_data.append(value_str)
            metadata_rows.append("\t".join(row_data))
            processed_doc_count += 1

        logging.info(f"Processed {processed_doc_count} documents. Skipped: {skipped_vector_count} (vector issue), {skipped_dimension_count} (dimension issue), {skipped_pk_count} (PK issue).")

        if not vectors:
            error_detail = "No valid vector data found after processing."
            if skipped_vector_count > 0 or skipped_dimension_count > 0 or skipped_pk_count > 0:
                 error_detail += f" Skipped docs breakdown: VectorIssue={skipped_vector_count}, DimensionIssue={skipped_dimension_count}, PKIssue={skipped_pk_count}."
            logging.error(error_detail)
            raise HTTPException(status_code=400, detail=error_detail)

        # Save vector data
        vector_data = np.array(vectors)
        logging.info(f"Attempting to save {len(vectors)} vectors ({vector_data.nbytes} bytes) to {vector_file_path}")
        try:
            with open(vector_file_path, 'wb') as vf:
                 vf.write(vector_data.tobytes())
            logging.info(f"Successfully saved vectors to {vector_file_path}")
        except IOError as e:
             logging.error(f"IOError saving vector file {vector_file_path}: {e}")
             raise HTTPException(status_code=500, detail=f"Could not write vector file: {e}")

        # Save metadata
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

        # Update config file
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
            "tensorPath": os.path.relpath(vector_file_path, ROOT_DIR).replace('\\', '/'),
            "metadataPath": os.path.relpath(metadata_file_path, ROOT_DIR).replace('\\', '/')
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

        # Return success response
        config_relative_url = os.path.relpath(config_file_path, ROOT_DIR).replace('\\', '/')
        # Calculate relative paths for the response using the actual file paths
        vector_response_path = os.path.relpath(vector_file_path, ROOT_DIR).replace('\\', '/')
        metadata_response_path = os.path.relpath(metadata_file_path, ROOT_DIR).replace('\\', '/')
        
        logging.info(f"Processing successful. Config URL: {config_relative_url}")
        return {
            "message": f"Successfully saved data for tensor '{sanitized_tensor_name}' using '{request.sampling_strategy}' strategy",
            "vector_file": os.path.basename(vector_file_path),
            "metadata_file": os.path.basename(metadata_file_path),
            "config_file": config_relative_url,
            "vectors_saved": len(vectors),
            "limit_applied": request.document_limit if request.sampling_strategy == 'token_range' or (request.document_limit and request.document_limit > 0) else None,
            "tensor_name": sanitized_tensor_name,
            "tensor_shape": [len(vectors), request.vector_dimension],
            "tensor_path_rel": vector_response_path, # Use correct relative path
            "metadata_path_rel": metadata_response_path, # Use correct relative path
            "output_dir": os.path.relpath(local_astra_data_dir, ROOT_DIR).replace('\\', '/')
        }

    except HTTPException as e:
        logging.error(f"HTTP Exception during save_astra_data: Status={e.status_code}, Detail='{e.detail}'")
        raise e
    except ValueError as e:
         logging.error(f"ValueError during save_astra_data: {e}")
         status_code = 400
         if "Partition key columns must be provided" in str(e) or "Total limit must be positive" in str(e):
              status_code = 422 
         elif "Authentication failed" in str(e) or "Failed to connect" in str(e):
              status_code = 400
         raise HTTPException(status_code=status_code, detail=str(e)) from e
    except Exception as e:
        logging.exception("Unexpected error during save_astra_data process")
        raise HTTPException(status_code=500, detail=f"An internal server error occurred. Check server logs for details.")

# Helper function (Python) to parse potential vector - MORE CONSERVATIVE
def python_parse_potential_vector(value):
    if not isinstance(value, str): value = str(value) # Ensure string
    value = value.strip()
    if not value: return None

    arr = None
    is_bracketed = (value.startswith('[') and value.endswith(']')) or \
                   (value.startswith('(') and value.endsWith(')'))

    try:
        if is_bracketed:
            # Use literal_eval for bracketed strings
            # Ensure content inside brackets is valid list content
            content_to_eval = value[1:-1]
            # Check if content looks reasonable before eval (e.g., only numbers, commas, spaces, dots, e, E, -, +)
            if not re.fullmatch(r'[\d\s,\.\-eE\+]*', content_to_eval):
                 return None # Contains disallowed characters
            # Wrap in [] for literal_eval to ensure list context
            evaluated = ast.literal_eval(f"[{content_to_eval}]") 
            if isinstance(evaluated, list):
                 arr = evaluated
            else: return None # Must evaluate to a list
        else:
            # Non-bracketed: Must contain comma or space AND split into > 1 part
            delimiter_pattern = None
            if ',' in value:
                delimiter_pattern = r'\s*,\s*' # Handle spaces around comma
            elif ' ' in value:
                 # Use regex for one or more spaces as delimiter
                 # Check if it *only* contains numbers/delimiters first
                 if not re.fullmatch(r'[\d\s\.\-eE\+]+(?<!\s)', value):
                      return None # Contains non-numeric/non-space characters
                 delimiter_pattern = r'\s+' 
            
            if delimiter_pattern is None: return None # Must have a delimiter

            parts = [s.strip() for s in re.split(delimiter_pattern, value) if s.strip()]
            
            if len(parts) <= 1: return None # Must have multiple parts if not bracketed

            # Try converting all parts to float - this must succeed for all
            arr = [float(p) for p in parts]

    except (ValueError, SyntaxError, TypeError):
        # Handles float conversion errors, literal_eval errors, bad regex splits
        return None 

    # Final check: Ensure non-empty list of numbers
    if isinstance(arr, list) and len(arr) > 0 and all(isinstance(x, (int, float)) for x in arr):
         # Check length > 1 required if NOT originally bracketed
         if len(arr) > 1 or is_bracketed:
             return arr # Return the list of numbers
             
    return None

@app.post("/api/file/upload")
async def api_file_upload(file: UploadFile = File(...)):
    """Handle file upload and detect header presence based on vector data in first row.
    
    The header detection logic works as follows:
    1. Default assumption is no header (has_header = False)
    2. Peek at first row to check for vector data
    3. If vector data found in first row -> No header (data starts at row 1)
    4. If no vector data in first row -> Header present (data starts at row 2)
    5. If file empty or peek fails -> Default to no header
    """
    filename = file.filename
    logging.info(f"Received file upload request: {filename}")
    if not filename: raise HTTPException(status_code=400, detail="No file selected.")
    _, extension = os.path.splitext(filename)
    extension = extension.lower()

    try:
        contents = await file.read()
        contents_stream = io.BytesIO(contents)
        
        has_header = False # Default to False unless proven otherwise
        df_peek = None
        # Define standard read options for CSV/TSV peek - read only the first row
        csv_tsv_peek_opts = {
            'nrows': 1, # Read only 1 row 
            'header': None, # Don't assume header yet
            'quotechar': '"',
            'quoting': csv.QUOTE_MINIMAL 
        }
        
        # --- Peek at first row --- 
        try:
            peek_stream = io.BytesIO(contents)
            if extension == '.csv':
                 df_peek = pd.read_csv(peek_stream, **csv_tsv_peek_opts)
            elif extension == '.tsv':
                 csv_tsv_peek_opts['sep'] = '\t'
                 df_peek = pd.read_csv(peek_stream, **csv_tsv_peek_opts)
            elif extension in ['.xls', '.xlsx']:
                 engine = 'openpyxl' if extension == '.xlsx' else 'xlrd'
                 df_peek = pd.read_excel(peek_stream, engine=engine, header=None, nrows=1) 
            else:
                 raise HTTPException(status_code=400, detail=f"Unsupported file type: {extension}")

            # --- Apply Header Detection Heuristic --- 
            if df_peek is not None and not df_peek.empty:
                logging.debug("Header Detection - First Row Analysis:")
                logging.debug(f"First row data: {df_peek.to_string()}")
                
                first_row = df_peek.iloc[0]
                logging.debug("Checking first row for vector data:")
                found_vector_in_first_row = False
                for i, item in enumerate(first_row):
                    item_str = str(item)
                    parsed_vector = python_parse_potential_vector(item_str)
                    vector_found_here = parsed_vector is not None
                    logging.debug(f"  Column {i}: Value='{item_str[:100]}...', Is Vector: {vector_found_here}")
                    if vector_found_here:
                        found_vector_in_first_row = True
                        break
                
                if found_vector_in_first_row:
                    logging.info("Header Detection Result: Vector found in first row -> No header (data starts at row 1)")
                    has_header = False # Vector in row 0 means it's data
                else:
                    logging.info("Header Detection Result: No vector in first row -> Header present (data starts at row 2)")
                    has_header = True # No vector in row 0 means it's likely a header
            else:
                 logging.info("Header Detection Result: File empty or peek failed -> Defaulting to no header")
                 has_header = False # Default to no header for empty/error 
        
        except Exception as peek_error:
            logging.warning(f"Header Detection Error: {peek_error}. Defaulting to no header.")
            has_header = False

        # --- Re-read the full file with determined header setting --- 
        logging.info(f"Reading full file with header setting: has_header = {has_header}")
        contents_stream.seek(0) # Reset original stream
        df = None
        read_opts_full = {}
        read_opts_full['header'] = 0 if has_header else None
        # Apply quoting for final read of CSV/TSV as well
        if extension in ['.csv', '.tsv']:
             read_opts_full['quotechar'] = '"'
             read_opts_full['quoting'] = csv.QUOTE_MINIMAL
             if extension == '.tsv': read_opts_full['sep'] = '\t'
        
        try:
            if extension == '.csv' or extension == '.tsv':
                df = pd.read_csv(contents_stream, **read_opts_full)
            elif extension in ['.xls', '.xlsx']:
                # Excel doesn't have standard quoting like CSV, read normally
                engine = 'openpyxl' if extension == '.xlsx' else 'xlrd'
                df = pd.read_excel(contents_stream, engine=engine, header=read_opts_full['header'])
        except Exception as read_error:
             header_msg = "with assumed header" if has_header else "assuming no header"
             logging.error(f"Error reading full file {header_msg}: {read_error}")
             raise HTTPException(status_code=400, detail=f"Could not read file content {header_msg}. Error: {read_error}")

        if df is None or df.empty:
            raise HTTPException(status_code=400, detail="File is empty or could not be read correctly.")

        if not has_header:
            columns = [f"Column_{i+1}" for i in range(len(df.columns))]
            df.columns = columns
            logging.info(f"Applied generated default columns: {columns}")
        else:
            columns = df.columns.astype(str).tolist() 
            logging.info(f"Using detected header columns: {columns}")

        sample_data = df.head(10).to_dict(orient='records') 

        logging.info(f"File upload analysis complete. Detected Header: {has_header}. Final Columns: {columns}")
        return {
            "filename": filename,
            "columns": columns,
            "has_header": has_header, 
            "sample_data": sample_data
        }

    except Exception as e:
        logging.exception(f"Error processing uploaded file '{filename}'")
        raise HTTPException(status_code=500, detail=f"An error occurred processing the file: {str(e)}")

@app.post("/api/file/process")
async def api_file_process(config_json: str = Form(...), file: UploadFile = File(...)):
    """Process uploaded file. Header detected via vector heuristic. Sampling applied."""
    try:
        config_dict = json.loads(config_json)
        # Rename the validated request model to avoid shadowing
        request_config = FileProcessRequest(**config_dict)
        logging.info(f"Received file processing request for: {request_config.filename} with tensor name '{request_config.tensorName}'")
        logging.info(f"Sampling Strategy: {request_config.samplingStrategy}, Limit: {request_config.limit}")
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid configuration format.")
    except Exception as e: # Catch validation errors from Pydantic
         logging.error(f"Configuration validation error: {e}")
         # Improve Pydantic error reporting if possible
         error_details = str(e)
         try:
             # Attempt to extract more specific Pydantic errors
             if hasattr(e, 'errors') and callable(e.errors):
                 error_details = ", ".join([f"'{err['loc'][0]}': {err['msg']}" for err in e.errors()])
         except Exception: pass
         raise HTTPException(status_code=422, detail=f"Invalid configuration data: {error_details}")

    # Use request_config throughout for request details
    if request_config.samplingStrategy in ["first_n", "random_n"] and (request_config.limit is None or request_config.limit <= 0):
        raise HTTPException(status_code=422, detail=f"A positive limit is required for sampling strategy '{request_config.samplingStrategy}'.")
    if request_config.samplingStrategy == "all" and request_config.limit is not None:
        logging.warning("Limit provided but sampling strategy is 'all'. Limit will be ignored.")
        request_config.limit = None 

    filename = file.filename 
    if not filename:
         raise HTTPException(status_code=400, detail="File is missing in the process request.")

    _, extension = os.path.splitext(filename)
    extension = extension.lower()

    try:
        contents = await file.read()
        
        # --- Header Detection (Repeat logic from /upload) --- 
        has_header = False 
        try:
            # Define peek options HERE
            csv_tsv_peek_opts = {
                'nrows': 1, 
                'header': None, 
                'quotechar': '"',
                'quoting': csv.QUOTE_MINIMAL 
            }
            peek_stream = io.BytesIO(contents)
            df_peek = None
            if extension == '.csv':
                 df_peek = pd.read_csv(peek_stream, **csv_tsv_peek_opts)
            elif extension == '.tsv':
                 csv_tsv_peek_opts['sep'] = '\t'
                 df_peek = pd.read_csv(peek_stream, **csv_tsv_peek_opts)
            elif extension in ['.xls', '.xlsx']:
                 engine = 'openpyxl' if extension == '.xlsx' else 'xlrd'
                 df_peek = pd.read_excel(peek_stream, engine=engine, header=None, nrows=1) 
            else:
                 raise HTTPException(status_code=400, detail=f"Unsupported file type: {extension}")

            if df_peek is not None and not df_peek.empty:
                first_row = df_peek.iloc[0]
                found_vector_in_first_row = False
                for i, item in enumerate(first_row):
                    item_str = str(item)
                    parsed_vector = python_parse_potential_vector(item_str)
                    vector_found_here = parsed_vector is not None
                    if vector_found_here:
                        found_vector_in_first_row = True
                        break
                
                if found_vector_in_first_row:
                    logging.info("Heuristic Result: Found vector in first row -> No header (data starts at row 1)")
                    has_header = False
                else:
                    logging.info("Heuristic Result: No vector in first row -> Header present (data starts at row 2)")
                    has_header = True
            else:
                 logging.info("Heuristic Result: File empty or peek failed -> Defaulting to no header")
                 has_header = False
        except Exception as peek_error:
            logging.warning(f"Error during header detection peek: {peek_error}. Defaulting to assuming NO header.")
            has_header = False
        logging.info(f"Processing Step Header Detection Result: has_header = {has_header}")
        # --- End Header Detection --- 

        # --- Read Full File --- 
        file_stream = io.BytesIO(contents)
        df = None
        # Determine read options based on locally detected has_header
        read_opts = {'header': 0 if has_header else None}
        # Add quoting options for CSV/TSV
        if extension in ['.csv', '.tsv']:
             read_opts['quotechar'] = '"'
             read_opts['quoting'] = csv.QUOTE_MINIMAL
             if extension == '.tsv': read_opts['sep'] = '\t'
             
        logging.info(f"Reading full file with pandas using determined options: {read_opts}")
        try:
            if extension == '.csv' or extension == '.tsv':
                df = pd.read_csv(file_stream, **read_opts)
            elif extension in ['.xls', '.xlsx']:
                # header option is part of read_opts
                engine = 'openpyxl' if extension == '.xlsx' else 'xlrd'
                df = pd.read_excel(file_stream, engine=engine, **read_opts)
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported file type: {extension}")
                
            if df is None or df.empty:
                 raise ValueError("File read resulted in an empty DataFrame.")
                 
            original_pandas_columns = df.columns.astype(str).tolist()
            logging.info(f"Read {len(df)} rows {('with header' if has_header else 'without header')}. Columns: {original_pandas_columns}")

        except Exception as read_error:
             header_msg = "with assumed header" if has_header else "assuming no header"
             logging.error(f"Error reading full file {header_msg}: {read_error}")
             raise HTTPException(status_code=400, detail=f"Could not read file content {header_msg}. Error: {read_error}")

        # --- Apply Sampling --- 
        sampled_df = None # Initialize
        if request_config.samplingStrategy == "first_n":
            limit = min(request_config.limit, len(df))
            sampled_slice = df.head(limit) 
            sampled_df = sampled_slice.copy() # Explicitly create a copy
            logging.info(f"Applied sampling: First {len(sampled_df)} rows (copied).")
        elif request_config.samplingStrategy == "random_n":
            limit = min(request_config.limit, len(df))
            sampled_sample = df.sample(n=limit, random_state=42) 
            sampled_df = sampled_sample.copy() # Explicitly create a copy
            logging.info(f"Applied sampling: Random {len(sampled_df)} rows (copied).")
        else: # "all"
             sampled_df = df # No sampling, just use the original DataFrame
             logging.info("No sampling applied (strategy: all).")

        if sampled_df is None or sampled_df.empty: # Added check for None just in case
            raise HTTPException(status_code=400, detail="No data remaining after sampling.")
        
        # --- Rename Columns AFTER Sampling (if no header detected) ---
        final_user_columns = [request_config.vectorColumnName] + request_config.selectedMetadataColumns
        
        # New code to auto-detect which column contains vector data
        if not has_header:
            current_sampled_columns = sampled_df.columns.astype(str).tolist()
            if len(final_user_columns) != len(current_sampled_columns):
                logging.error(f"Column count mismatch after sampling. UI Config: {final_user_columns}. Sampled data cols: {current_sampled_columns}")
                raise HTTPException(status_code=400, detail="Config Error: Column count mismatch.")
            
            # Auto-detect which column contains vector data
            vector_col_index = -1
            for idx, col in enumerate(current_sampled_columns):
                # Check first row for vector-like data
                sample_val = str(sampled_df.iloc[0, idx])
                is_vector_like = sample_val.startswith('[') and sample_val.endswith(']') and ',' in sample_val
                if is_vector_like:
                    vector_col_index = idx
                    logging.info(f"Auto-detected vector data in column index {vector_col_index} (value: {sample_val[:30]}...)")
                    break
            
            if vector_col_index == -1:
                logging.warning("Could not auto-detect vector column. Using first column as default.")
                vector_col_index = 0
            
            # Create a list of column indices in the order we want them
            column_order = [vector_col_index]  # Start with the vector column
            for i in range(len(current_sampled_columns)):
                if i != vector_col_index:
                    column_order.append(i)
            
            logging.info(f"Reordering columns using index order: {column_order}")
            
            # Create new DataFrame with reordered columns
            try:
                # First reorder the columns by index
                reordered_df = sampled_df.iloc[:, column_order]
                
                # Then rename the columns to the desired names
                reordered_df.columns = final_user_columns
                
                # Replace the original DataFrame
                sampled_df = reordered_df
                
                logging.info(f"Columns after reordering and renaming: {sampled_df.columns.tolist()}")
            except Exception as rename_err:
                logging.error(f"Error reordering and renaming columns: {rename_err}")
                raise HTTPException(status_code=500, detail=f"Internal error during column reordering: {rename_err}")
        
        # --- Select and Validate Columns --- 
        vector_col = request_config.vectorColumnName
        metadata_cols = request_config.selectedMetadataColumns
        available_cols_final = sampled_df.columns.astype(str).tolist()

        missing_cols = [col for col in final_user_columns if col not in available_cols_final]
        if missing_cols:
             logging.error(f"Internal Error: Columns {missing_cols} not found after sampling and rename. Available: {available_cols_final}. Expected: {final_user_columns}")
             raise HTTPException(status_code=500, detail=f"Internal processing error: Column mismatch after configuration.")

        final_df = sampled_df[final_user_columns] 
        logging.info(f"Processing with Vector Column: '{vector_col}', Metadata Columns: {metadata_cols}")

        # --- Vector and Metadata Processing (using final_df) --- 
        vectors = []
        metadata_rows = []
        metadata_header = metadata_cols 

        logging.info(f"Processing {len(final_df)} documents. Vector key: '{vector_col}'. Metadata header: {metadata_header}")

        processed_doc_count = 0
        skipped_rows_parsing = 0 # Changed from skipped_vector_count for clarity
        skipped_dimension_count = 0
        expected_dimension = None # Moved initialization here
        # skipped_pk_count = 0 # Removed, not relevant here

        for index, row in final_df.iterrows():
            vector_data = row[vector_col]
            parsed_vector = None

            # --- Robust Vector Parsing --- 
            if isinstance(vector_data, str):
                try:
                    parsed_vector = ast.literal_eval(vector_data)
                    if not isinstance(parsed_vector, list):
                         parsed_vector = None 
                except (ValueError, SyntaxError):
                     try:
                          vector_data_cleaned = vector_data.strip("[]() ")
                          delimiter = ',' if ',' in vector_data_cleaned else ' '
                          parts = [v.strip() for v in vector_data_cleaned.split(delimiter) if v.strip()] 
                          if parts:
                               parsed_vector = [float(p) for p in parts] 
                          else:
                               parsed_vector = None
                     except ValueError:
                          parsed_vector = None
            elif isinstance(vector_data, (list, tuple)):
                 try:
                      parsed_vector = [float(item) for item in vector_data]
                 except (ValueError, TypeError):
                      parsed_vector = None 
            elif isinstance(vector_data, np.ndarray):
                parsed_vector = vector_data.tolist()
            
            if parsed_vector is None or not isinstance(parsed_vector, list):
                logging.warning(f"Row index {index}: Could not parse vector data ('{vector_data}', type: {type(vector_data)}). Skipping.")
                skipped_rows_parsing += 1
                continue

            # --- Convert to numpy and Check Dimension --- 
            try:
                 numeric_vector = [float(item) for item in parsed_vector] 
                 np_vector = np.array(numeric_vector, dtype=np.float32)
            except (ValueError, TypeError) as e:
                 logging.warning(f"Row index {index}: Vector conversion failed ({e}). Vector: {parsed_vector}. Skipping.")
                 skipped_rows_parsing += 1
                 continue

            current_dimension = len(np_vector)
            if expected_dimension is None:
                expected_dimension = current_dimension
                if expected_dimension <= 0:
                     logging.error(f"Row index {index}: Invalid vector dimension detected ({expected_dimension}). Skipping.")
                     skipped_rows_parsing += 1 
                     expected_dimension = None 
                     continue
                logging.info(f"Detected vector dimension: {expected_dimension}")
            elif current_dimension != expected_dimension:
                logging.warning(f"Row index {index}: Vector dimension mismatch ({current_dimension} vs expected {expected_dimension}). Skipping.")
                skipped_dimension_count += 1
                continue

            vectors.append(np_vector)

            # --- Build Metadata Row --- (Using metadata_header directly)
            meta_row_data = []
            for meta_col_name in metadata_header: # Use the simplified header
                 value = row.get(meta_col_name, '')
                 value_str = str(value).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                 meta_row_data.append(value_str)
            metadata_rows.append("\t".join(meta_row_data))
            processed_doc_count += 1

        logging.info(f"Processed {processed_doc_count} documents. Skipped: {skipped_rows_parsing} (parsing), {skipped_dimension_count} (dimension).") # Removed PK skip count

        if not vectors or expected_dimension is None:
            error_detail = "No valid vector data found after processing and validation."
            if skipped_rows_parsing > 0 or skipped_dimension_count > 0:
                 error_detail += f" Skipped rows breakdown: ParsingIssue={skipped_rows_parsing}, DimensionIssue={skipped_dimension_count}. Check vector format and column selection."
            logging.error(error_detail)
            raise HTTPException(status_code=400, detail=error_detail)

        # --- Define Output Paths --- 
        local_data_dir = os.path.join(ROOT_DIR, "file_data")
        os.makedirs(local_data_dir, exist_ok=True)
        logging.info(f"Using output directory: {local_data_dir}")

        # Use tensor name from request, sanitize it
        raw_tensor_name = request_config.tensorName
        sanitized_tensor_name = raw_tensor_name.replace(" ", "_")
        safe_tensor_name = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in sanitized_tensor_name)
        if not safe_tensor_name:
            safe_tensor_name = os.path.splitext(request_config.filename)[0].replace(" ", "_")
            safe_tensor_name = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in safe_tensor_name)
            if not safe_tensor_name: 
                 safe_tensor_name = "uploaded_tensor"
            logging.warning(f"Provided tensor name '{raw_tensor_name}' sanitized to empty or invalid. Using fallback: '{safe_tensor_name}'")
        
        logging.info(f"Using final safe tensor name: '{safe_tensor_name}'")
        vector_file_path = os.path.join(local_data_dir, f"{safe_tensor_name}.bytes")
        metadata_file_path = os.path.join(local_data_dir, f"{safe_tensor_name}_metadata.tsv")
        config_file_path = os.path.join(local_data_dir, "file_projector_config.json") # Define config path here too

        # --- Save Vector Data --- 
        vector_data = np.array(vectors)
        logging.info(f"Attempting to save {len(vectors)} vectors ({vector_data.nbytes} bytes) to {vector_file_path}")
        try:
            with open(vector_file_path, 'wb') as vf:
                 vf.write(vector_data.tobytes())
            logging.info(f"Successfully saved vectors to {vector_file_path}")
        except IOError as e:
             logging.error(f"IOError saving vector file {vector_file_path}: {e}")
             raise HTTPException(status_code=500, detail=f"Could not write vector file: {e}")

        # --- Save Metadata --- 
        logging.info(f"Attempting to save metadata for {len(metadata_rows)} rows to {metadata_file_path}")
        try:
            with open(metadata_file_path, 'w', encoding='utf-8') as mf:
                mf.write("\t".join(metadata_header) + "\n") # Use the simplified header
                mf.write("\n".join(metadata_rows))
            logging.info(f"Successfully saved metadata to {metadata_file_path}")
        except IOError as e:
             logging.error(f"IOError saving metadata file {metadata_file_path}: {e}")
             raise HTTPException(status_code=500, detail=f"Could not write metadata file: {e}")

        # --- Update Config File --- 
        logging.info(f"Attempting to read and update config file: {config_file_path}")
        # Use a different name for the loaded config dictionary
        proj_config = {} 
        if os.path.exists(config_file_path):
            try:
                with open(config_file_path, 'r', encoding='utf-8') as f:
                    proj_config = json.load(f) # Load into proj_config
                    if not isinstance(proj_config, dict):
                         proj_config = {}
            except Exception as e:
                 logging.warning(f"Error reading config file {config_file_path}: {e}. Starting fresh.")
                 proj_config = {}

        if "embeddings" not in proj_config or not isinstance(proj_config.get("embeddings"), list):
             proj_config["embeddings"] = []

        # Create tensor entry using expected_dimension and safe_tensor_name
        tensor_entry = {
            "tensorName": safe_tensor_name, 
            "tensorShape": [len(vectors), expected_dimension], 
            "tensorPath": os.path.relpath(vector_file_path, ROOT_DIR).replace('\\', '/'),
            "metadataPath": os.path.relpath(metadata_file_path, ROOT_DIR).replace('\\', '/')
        }

        # Update proj_config dictionary
        found_index = -1
        for i, entry in enumerate(proj_config["embeddings"]):
            if isinstance(entry, dict) and entry.get("tensorName") == safe_tensor_name:
                found_index = i
                break
        
        if found_index != -1:
             logging.info(f"Removing existing entry for tensor '{safe_tensor_name}' from index {found_index}.")
             del proj_config["embeddings"][found_index]
        
        logging.info(f"Inserting entry for tensor '{safe_tensor_name}' at the beginning of the config list.")
        proj_config["embeddings"].insert(0, tensor_entry)

        # Save the updated proj_config dictionary
        try:
            with open(config_file_path, 'w', encoding='utf-8') as f:
                json.dump(proj_config, f, indent=2)
            logging.info(f"Successfully updated config file {config_file_path}")
        except IOError as e:
             logging.error(f"IOError writing updated config file {config_file_path}: {e}")
             raise HTTPException(status_code=500, detail=f"Failed to write config file: {e}")
        except Exception as e:
             logging.exception(f"Unexpected error writing updated config file {config_file_path}")
             raise HTTPException(status_code=500, detail=f"Unexpected error writing config file: {str(e)}")

        # Return success response - use request_config for original filename
        config_relative_url = os.path.relpath(config_file_path, ROOT_DIR).replace('\\', '/')
        vector_response_path = os.path.relpath(vector_file_path, ROOT_DIR).replace('\\', '/')
        metadata_response_path = os.path.relpath(metadata_file_path, ROOT_DIR).replace('\\', '/')
        
        logging.info(f"Processing successful. Config URL: {config_relative_url}")
        return {
            "message": f"Successfully processed '{request_config.filename}' ({processed_doc_count} rows saved).",
            "projector_config_url": config_relative_url,
            "tensor_name": safe_tensor_name,
            "tensor_shape": [len(vectors), expected_dimension],
            "tensor_path_rel": vector_response_path, 
            "metadata_path_rel": metadata_response_path, 
            "output_dir": os.path.relpath(local_data_dir, ROOT_DIR).replace('\\', '/')
        }

    except pd.errors.EmptyDataError:
        logging.error(f"File '{filename}' is empty or became empty after read.")
        raise HTTPException(status_code=400, detail="The uploaded file is empty.")
    except HTTPException as e:
        raise e # Reraise HTTP exceptions directly
    except Exception as e:
        logging.exception(f"Error processing file '{filename}' with configuration")
        raise HTTPException(status_code=500, detail=f"An error occurred during file processing: {str(e)}")

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

# Setup Static Files
if not os.path.exists(STATIC_DIR):
    os.makedirs(STATIC_DIR)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static_assets")

# Serve Static Files
app.mount("/", StaticFiles(directory=ROOT_DIR, html=True), name="root_static")

@app.on_event("startup")
async def startup_event():
    """Handle server startup."""
    print("Server starting up...")

@app.on_event("shutdown")
def shutdown_event():
    """Handle server shutdown."""
    print("Server shutting down...")

# Custom Logging Filter
class SuppressZeroFilterWarning(logging.Filter):
    """Filter to suppress zero filter operation warnings from astrapy."""
    def filter(self, record):
        return 'ZERO_FILTER_OPERATIONS' not in record.getMessage()

if __name__ == "__main__":
    print(f"Starting server on http://{MAIN_SERVER_HOST}:{MAIN_SERVER_PORT}")
    print(f"Serving root static files from: {ROOT_DIR}")
    print(f"Serving asset static files from: {STATIC_DIR}")
    print(f"Astra helper available at: /astra")
    uvicorn.run(app, host=MAIN_SERVER_HOST, port=MAIN_SERVER_PORT) 