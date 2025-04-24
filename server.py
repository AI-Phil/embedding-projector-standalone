import os
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field # For request body validation
from astrapy import DataAPIClient
from astrapy.database import Database # Import Database for type hint

# Configuration
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(ROOT_DIR, "static") # Specific static dir
TEMPLATES_DIR = os.path.join(ROOT_DIR, "templates") # Directory for HTML templates
MAIN_SERVER_HOST = "0.0.0.0" # Listen on all interfaces
MAIN_SERVER_PORT = 8000 # Port for users to access

app = FastAPI()

# --- Request Models (for validation) ---
class ConnectionInfo(BaseModel):
    endpoint_url: str # Should be the *Data API* endpoint
    token: str
    db_name: str # Nickname, might be used as namespace/keyspace if applicable

class SampleRequest(BaseModel):
    connection: ConnectionInfo
    collection_name: str

# --- Global DataAPIClient cache ---
astra_data_api_clients = {}

def get_data_api_client(info: ConnectionInfo) -> Database:
    """Gets or creates a Database instance via DataAPIClient."""
    # Cache key based on endpoint. Assumes token doesn't change for the same endpoint during server run.
    key = info.endpoint_url
    if key not in astra_data_api_clients:
        print(f"Creating new DataAPIClient connection for {info.endpoint_url}")
        try:
            # Initialize the client first (no args needed here for basic init)
            client = DataAPIClient()
            # Get the Database object using the endpoint and token
            db = client.get_database(
                info.endpoint_url, 
                token=info.token, 
            )
            astra_data_api_clients[key] = db # Store the Database object
            print(f"Connected to database via Data API: {info.endpoint_url} (Using default namespace)")
        except Exception as e:
            print(f"Failed to create DataAPIClient/Database: {e}")
            # Add more specific error handling if needed
            if "Unauthorized" in str(e) or "Forbidden" in str(e):
                 raise ValueError(f"Authentication failed. Check your token and Data API Endpoint URL. Error: {e}") from e
            else:
                 raise ValueError(f"Failed to connect using Data API: {e}") from e
    # Return the Database object
    return astra_data_api_clients[key]

# --- Setup Templates ---
# Create templates directory if it doesn't exist
if not os.path.exists(TEMPLATES_DIR):
    os.makedirs(TEMPLATES_DIR)
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# --- Astra Helper App Routes (Defined Before Static Mounts) ---

@app.get("/astra", response_class=HTMLResponse, include_in_schema=False) # include_in_schema=False to avoid duplicate docs
@app.get("/astra/", response_class=HTMLResponse) # Add route for trailing slash
async def get_astra_page(request: Request):
    """Serves the main HTML page for the Astra helper app."""
    # We will create templates/astra.html later
    return templates.TemplateResponse("astra.html", {"request": request})

# --- API Routes (Defined Before Static Mounts) ---

@app.post("/api/astra/collections")
async def api_astra_get_collections(connection_info: ConnectionInfo):
    """API endpoint to list collections using provided credentials via Data API."""
    print(f"Received request for collections: Endpoint={connection_info.endpoint_url}")
    try:
        db = get_data_api_client(connection_info) # Get Database object
        # Data API uses list_collection_names() on the Database object
        # result = db.find_collections() # Incorrect method
        collection_names = db.list_collection_names()
        print(f"Found collections via Data API: {collection_names}")
        return {"collections": collection_names}
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        print(f"Error getting collections via Data API: {e}")
        return JSONResponse(status_code=500, content={"error": f"An unexpected error occurred: {e}"})

@app.post("/api/astra/sample")
async def api_astra_sample_data(sample_request: SampleRequest):
    """API endpoint to sample data from a collection via Data API."""
    print(f"Received request to sample collection: {sample_request.collection_name}")
    try:
        db = get_data_api_client(sample_request.connection)
        # Get collection object from the Database object
        collection = db.get_collection(sample_request.collection_name)
        
        # Fetch a small number of documents (e.g., 10) using find
        # find() returns an iterable cursor
        cursor = collection.find(limit=10)
        # sample_docs = list(results.get("data", {}).get("documents", [])) # Incorrect: tried to .get() on cursor
        sample_docs = list(cursor) # Correct: iterate the cursor into a list
        
        print(f"Sampled {len(sample_docs)} documents via Data API.")
        return {"sample_data": sample_docs}
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        print(f"Error sampling data via Data API: {e}")
        return JSONResponse(status_code=500, content={"error": f"An unexpected error occurred while sampling: {e}"})

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