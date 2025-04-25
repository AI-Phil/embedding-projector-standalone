# Embedding Projector Standalone

A standalone version of the TensorFlow Embedding Projector.

## Installation

1.  **Install uv:** Follow the instructions at [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv) to install `uv`.
2.  **Sync dependencies:** Navigate to the project directory and run:
    ```bash
    uv sync
    ```
3.  **Run the server:**
    ```bash
    uv run python server.py
    ```

## Usage

Once the server is running, open your web browser and navigate to `http://localhost:8000` (or the address provided by the server).

You can then load your embedding data and metadata files to visualize them in the projector.

## Data Preparation Helpers

This project includes helper pages to generate the necessary tensor (`_data.tsv`) and metadata (`_metadata.tsv`) files required by the Embedding Projector.

*   **Astra DB Helper (`/astra`):** Navigate to `http://localhost:8000/astra` to connect to your Astra DB instance. This page allows you to:
    *   List vector-enabled collections or tables.
    *   Sample data from a selected collection/table.
    *   Select metadata fields.
    *   Generate the `_data.bytes` and `_metadata.tsv` files on the server.
    *   Provides a link to the Embedding Projector configured to load the generated data.

*   **File Upload Helper (`/file`):** Navigate to `http://localhost:8000/file` to upload a data file (e.g., CSV). This page allows you to:
    *   Specify the column containing the vector embeddings.
    *   Select columns to be used as metadata.
    *   Choose a sampling strategy (all, first N, random N).
    *   Generate the `_data.bytes` and `_metadata.tsv` files on the server.
    *   Provides a link to the Embedding Projector configured to load the generated data.

Once you have generated these files via one of the helpers, click the provided link to visualize them in the main Embedding Projector interface.
