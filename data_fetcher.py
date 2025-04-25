import logging
import math
from typing import List, Dict, Any, Literal

from astrapy.database import Database
from astrapy.table import Table # Added for type hinting
from astrapy.collection import Collection # Added for type hinting
from fastapi import HTTPException

# Custom filter class definition (copied from server.py)
class SuppressZeroFilterWarning(logging.Filter):
    """Filter to suppress zero filter operation warnings from astrapy."""
    def filter(self, record):
        return 'ZERO_FILTER_OPERATIONS' not in record.getMessage()

async def fetch_data_first_rows(
    db: Database,
    target_name: str,
    find_options: dict,
    is_table_mode: bool,
    vector_key_name: str # Added for logging consistency if needed later
) -> List[Dict[str, Any]]:
    """Fetch the first N documents/rows based on find_options.
    
    Args:
        db: Database instance to query
        target_name: Name of the collection or table
        find_options: Options for the find operation (limit, projection, etc.)
        is_table_mode: Whether to query a table (True) or collection (False)
        vector_key_name: Name of the vector field for logging purposes
        
    Returns:
        List of documents/rows from the query
        
    Raises:
        HTTPException: If the target doesn't exist or other errors occur
    """
    logging.info(f"Fetching data using 'first_rows' strategy from {'table' if is_table_mode else 'collection'} '{target_name}' with options: {find_options}")
    
    documents = []
    # --- Temporarily Add Filter to Suppress Specific Warning --- 
    api_commander_logger = logging.getLogger('astrapy.utils.api_commander')
    zero_filter_suppressor = SuppressZeroFilterWarning()
    api_commander_logger.addFilter(zero_filter_suppressor)
    # --- End Add Filter --- 

    try:
        if is_table_mode:
             table = db.get_table(target_name)
             cursor = table.find(**find_options) # Use find_options directly
             documents = list(cursor)
             logging.info(f"Fetched {len(documents)} rows from table '{target_name}'.")
        else:
             collection = db.get_collection(target_name)
             cursor = collection.find(**find_options) # Use find_options directly
             documents = list(cursor)
             logging.info(f"Fetched {len(documents)} documents from collection '{target_name}'.")

    except Exception as e:
        # Catch potential errors from astrapy (e.g., table/collection not found)
        logging.exception(f"Error fetching data using 'first_rows' strategy from '{target_name}'")
        # Check for specific API command errors if possible
        err_str = str(e).lower()
        not_found_keywords = ["not found", "does not exist", "doesn't exist"]
        is_not_found_error = any(keyword in err_str for keyword in not_found_keywords) and f"'{target_name}'" in err_str

        if is_not_found_error:
            target_type = "Table" if is_table_mode else "Collection"
            logging.error(f"{target_type} '{target_name}' not found: {e}")
            raise HTTPException(status_code=404, detail=f"{target_type} '{target_name}' not found.")
        else:
            # Re-raise other exceptions to be handled by the main endpoint
            raise HTTPException(status_code=500, detail=f"An unexpected error occurred while fetching data: {e}") from e
    finally:
        # --- Remove the specific filter --- 
        api_commander_logger.removeFilter(zero_filter_suppressor)
        # --- End Remove Filter --- 

    if not documents:
        logging.warning(f"No documents found in '{target_name}' with the specified find options.")
        # Let the calling function handle the "no documents" case, maybe it's not an error depending on context
        # Raise HTTPException(status_code=404, detail=f"No documents found in '{target_name}'. Check if it's empty or the filters exclude all docs.")

    return documents

async def fetch_data_token_range(
    db: Database,
    table_name: str,
    partition_key_columns: List[str],
    projection: Dict[str, Any],
    total_limit: int,
    vector_column: str # Pass vector column name for consistency checks later if needed
) -> List[Dict[str, Any]]:
    """Fetch data by sampling across 10 token range parts.
    
    This function samples data by dividing the token range into 10 parts and fetching
    documents from each part. This helps get a more distributed sample of the data.
    
    Note: This function assumes the underlying Data API and astrapy's find method
    support filtering directly on 'token(...)'. If this fails, a different approach
    (e.g., using paging or accepting limitations) may be required.
    
    Args:
        db: Database instance to query
        table_name: Name of the table to query
        partition_key_columns: List of partition key column names
        projection: Fields to include in the result
        total_limit: Maximum number of documents to return
        vector_column: Name of the vector column for logging purposes
        
    Returns:
        List of sampled documents from across the token ranges
        
    Raises:
        ValueError: If partition key columns are missing or limit is invalid
        HTTPException: If the table doesn't exist or other errors occur
    """
    if not partition_key_columns:
        raise ValueError("Partition key columns must be provided for token range sampling.")
    if total_limit <= 0:
        raise ValueError("Total limit must be positive for token range sampling.")

    logging.info(f"Fetching data using 'token_range' strategy from table '{table_name}'. Total limit: {total_limit}")

    table = db.get_table(table_name)
    all_sampled_docs = []
    num_ranges = 10
    # Calculate limit per range, ensuring it's at least 1, fetch 5x needed for sub-sampling
    limit_per_range = max(1, math.ceil(total_limit / num_ranges)) * 5 
    
    # Cassandra token range
    min_token = -9223372036854775808  # -2**63
    max_token = 9223372036854775807  # 2**63 - 1
    token_range_size = max_token - min_token
    range_step = token_range_size // num_ranges

    # Construct the partition key string for the token function
    pk_string = ", ".join(partition_key_columns)
    token_filter_key = f"token({pk_string})"

    # --- Setup Filter Suppressor --- 
    api_commander_logger = logging.getLogger('astrapy.utils.api_commander')
    zero_filter_suppressor = SuppressZeroFilterWarning()
    api_commander_logger.addFilter(zero_filter_suppressor)
    # --- End Setup Filter --- 

    try:
        for i in range(num_ranges):
            range_start = min_token + i * range_step
            # Ensure the last range goes up to max_token
            range_end = min_token + (i + 1) * range_step if i < num_ranges - 1 else max_token + 1 # Use +1 because $lt is exclusive

            logging.debug(f"Querying token range {i+1}/{num_ranges}: {range_start} to {range_end} with limit {limit_per_range}")

            # *** WARNING: This filter format relies on Data API supporting token() directly ***
            find_options = {
                "filter": {token_filter_key: {"$gte": range_start, "$lt": range_end}},
                "limit": limit_per_range,
                "projection": projection
            }

            try:
                cursor = table.find(**find_options)
                range_docs = list(cursor)
                logging.debug(f"Range {i+1}: Fetched {len(range_docs)} documents.")

                # Sub-sample every 5th document
                sampled_range_docs = range_docs[::5]
                logging.debug(f"Range {i+1}: Sampled {len(sampled_range_docs)} documents.")
                all_sampled_docs.extend(sampled_range_docs)

            except Exception as e:
                # Log error for specific range but continue to try other ranges
                logging.error(f"Error fetching or processing token range {i+1} ({range_start} to {range_end}): {e}")
                # Check if the error suggests token() is unsupported
                if "token function is not supported" in str(e).lower() or "unable to make query" in str(e).lower() or "invalid filter" in str(e).lower():
                    logging.error(f"Failed query might indicate token() filtering is not supported by the API/astrapy. Query options: {find_options}")
                    # Depending on requirements, we might want to raise an exception here or just log and return potentially incomplete results
                    # For now, just log and continue

        logging.info(f"Finished token range queries. Total sampled documents: {len(all_sampled_docs)}")

    except Exception as e:
        # Catch broader errors (e.g., table not found during get_table)
        logging.exception(f"Error during 'token_range' fetch from table '{table_name}'")
        # Check for table not found specifically
        err_str = str(e).lower()
        not_found_keywords = ["not found", "does not exist", "doesn't exist"]
        is_not_found_error = any(keyword in err_str for keyword in not_found_keywords) and f"'{table_name}'" in err_str
        if is_not_found_error:
            logging.error(f"Table '{table_name}' not found: {e}")
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")
        else:
            raise HTTPException(status_code=500, detail=f"An unexpected error occurred during token range fetch: {e}") from e
    finally:
        # --- Remove the specific filter --- 
        api_commander_logger.removeFilter(zero_filter_suppressor)
        # --- End Remove Filter --- 

    # Trim results if we collected more than total_limit due to sampling
    if len(all_sampled_docs) > total_limit:
        logging.info(f"Trimming token range results from {len(all_sampled_docs)} to {total_limit}")
        all_sampled_docs = all_sampled_docs[:total_limit]

    return all_sampled_docs 

async def fetch_data_distributed(
    db: Database,
    collection_name: str,
    projection: Dict[str, Any],
    total_limit: int,
    vector_key_name: str
) -> List[Dict[str, Any]]:
    """Fetch data by sampling across different segments of the collection.
    
    This function samples data by:
    1. Getting the total document count
    2. Dividing the collection into segments
    3. Fetching documents from each segment using pagination
    
    Args:
        db: Database instance to query
        collection_name: Name of the collection to query
        projection: Fields to include in the result
        total_limit: Maximum number of documents to return
        vector_key_name: Name of the vector field
        
    Returns:
        List of sampled documents from across the collection
        
    Raises:
        ValueError: If limit is invalid
        HTTPException: If the collection doesn't exist or other errors occur
    """
    if total_limit <= 0:
        raise ValueError("Total limit must be positive for distributed sampling.")

    logging.info(f"Fetching data using 'distributed' strategy from collection '{collection_name}'. Total limit: {total_limit}")

    collection = db.get_collection(collection_name)
    all_sampled_docs = []
    num_segments = 10  # Number of segments to sample from
    
    try:
        # Get total document count
        total_count = collection.estimated_document_count()
        if total_count == 0:
            logging.warning(f"Collection '{collection_name}' appears to be empty.")
            return []
            
        # Calculate segment size and documents per segment
        segment_size = total_count // num_segments
        docs_per_segment = max(1, math.ceil(total_limit / num_segments))
        
        logging.info(f"Collection size: {total_count}, Segment size: {segment_size}, Docs per segment: {docs_per_segment}")
        
        for i in range(num_segments):
            # Calculate skip value for this segment
            skip = i * segment_size
            
            # Fetch documents from this segment
            find_options = {
                "projection": projection,
                "limit": docs_per_segment,
                "skip": skip
            }
            
            try:
                cursor = collection.find(**find_options)
                segment_docs = list(cursor)
                logging.debug(f"Segment {i+1}: Fetched {len(segment_docs)} documents.")
                all_sampled_docs.extend(segment_docs)
                
            except Exception as e:
                logging.error(f"Error fetching segment {i+1} (skip={skip}): {e}")
                # Continue with other segments even if one fails
                
        logging.info(f"Finished distributed sampling. Total sampled documents: {len(all_sampled_docs)}")
        
    except Exception as e:
        logging.exception(f"Error during 'distributed' fetch from collection '{collection_name}'")
        err_str = str(e).lower()
        not_found_keywords = ["not found", "does not exist", "doesn't exist"]
        is_not_found_error = any(keyword in err_str for keyword in not_found_keywords) and f"'{collection_name}'" in err_str
        if is_not_found_error:
            raise HTTPException(status_code=404, detail=f"Collection '{collection_name}' not found.")
        else:
            raise HTTPException(status_code=500, detail=f"An unexpected error occurred during distributed fetch: {e}") from e

    # Trim results if we collected more than total_limit
    if len(all_sampled_docs) > total_limit:
        logging.info(f"Trimming distributed results from {len(all_sampled_docs)} to {total_limit}")
        all_sampled_docs = all_sampled_docs[:total_limit]

    return all_sampled_docs 