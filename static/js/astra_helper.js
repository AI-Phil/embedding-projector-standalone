const connectionForm = document.getElementById('connection-form');
const collectionsSection = document.getElementById('collections-section');
const collectionsListDiv = document.getElementById('collections-list');
const sampleButton = document.getElementById('sample-button');
const metadataSelectionSection = document.getElementById('metadata-selection-section');
const sampleDataContainer = document.getElementById('sample-data');
const samplingPreviewSection = document.getElementById('sampling-preview-section');
const metadataKeysListDiv = document.getElementById('metadata-keys-list');
const previewDocsButton = document.getElementById('preview-docs-button');
const generateConfigButton = document.getElementById('generate-config-button');
const configSection = document.getElementById('config-section');
const configJsonTextarea = document.getElementById('config-json');
const dataJsonTextarea = document.getElementById('data-json');
const metadataTsvTextarea = document.getElementById('metadata-tsv');
const resultsDiv = document.getElementById('results');
const errorDiv = document.getElementById('error');
const tensorNameOverrideInput = document.getElementById('tensor_name_override');

// Added elements for Tables
const tablesSection = document.getElementById('tables-section');
const tablesListDiv = document.getElementById('tables-list');
const sampleTableButton = document.getElementById('sample-table-button');
const connectTablesButton = document.getElementById('connect-tables-button'); // Added table button reference

// Added button reference for Collections
const connectCollectionsButton = document.getElementById('connect-collections-button');

let currentConnection = null;
let selectedCollection = null;
let selectedCollectionDimension = null;
let selectedCollectionCount = null;
let currentSampleData = null;
let availableMetadataKeys = [];

// Added state for Tables
let selectedTable = null;
let selectedVectorColumn = null;
let selectedTableDimension = null;
let selectedTablePrimaryKeyCols = [];
let selectedTableCount = null;
let isTableMode = false; // Flag to track if we are in table or collection mode

// Initialize the page by checking for stored connection details
// This allows users to refresh without re-entering credentials
window.addEventListener('DOMContentLoaded', () => {
    console.log("Page loaded. Checking session storage for connection details.");
    const storedEndpoint = sessionStorage.getItem('astraEndpointUrl');
    const storedToken = sessionStorage.getItem('astraToken');
    const storedDbName = sessionStorage.getItem('astraDbName');
    const storedKeyspace = sessionStorage.getItem('astraKeyspace');

    if (storedEndpoint && storedToken) {
        console.log("Found stored details, populating form.");
        connectionForm.elements['endpoint_url'].value = storedEndpoint;
        connectionForm.elements['token'].value = storedToken;
        connectionForm.elements['db_name'].value = storedDbName || '';
        connectionForm.elements['keyspace'].value = storedKeyspace || '';
        showStatus("Loaded connection details from session storage. Click 'Connect & List Collections' to proceed.");
    } else {
         console.log("No connection details found in session storage.");
    }
});

function showError(message) {
    errorDiv.textContent = message;
    errorDiv.classList.remove('hidden');
    resultsDiv.textContent = 'Error occurred.';
}

function showStatus(message) {
    resultsDiv.textContent = message;
    errorDiv.classList.add('hidden');
}

// Handle the connection form submission (FOR COLLECTIONS)
// This manages the connection to Astra DB and fetches available collections
// connectionForm.addEventListener('submit', async (event) => { // <<<< REMOVED THIS BLOCK
//     event.preventDefault();
//     isTableMode = false; // Set mode to collection
//     showStatus('Connecting and fetching collections...');
// ... [rest of the old submit listener code] ...
// });

// --- Added Click Listener for Collections Button --- 
connectCollectionsButton.addEventListener('click', async () => {
    // Note: event.preventDefault() is NOT needed for type="button"
    isTableMode = false; // Set mode to collection
    showStatus('Connecting and fetching collections...');
    collectionsSection.classList.add('hidden');
    tablesSection.classList.add('hidden'); // Hide tables section
    metadataSelectionSection.classList.add('hidden');
    samplingPreviewSection.classList.add('hidden');
    configSection.classList.add('hidden');
    sampleButton.disabled = true;
    generateConfigButton.disabled = true;
    collectionsListDiv.innerHTML = '';
    metadataKeysListDiv.innerHTML = '';

    const formData = new FormData(connectionForm);
    const data = Object.fromEntries(formData.entries());

    // Use 'default_keyspace' if the user leaves the keyspace field blank
    if (!data.keyspace) {
        data.keyspace = 'default_keyspace';
    }

    try {
        // Fetch collections from the backend API
        const response = await fetch('/api/astra/collections', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });

        if (!response.ok) {
            const errorData = await response.text();
            throw new Error(`HTTP error ${response.status}: ${errorData}`);
        }
        const result = await response.json();

        if (result.collections && result.collections.length > 0) {
            // On successful connection, store details for potential reuse
            currentConnection = data;
            sessionStorage.setItem('astraEndpointUrl', data.endpoint_url);
            sessionStorage.setItem('astraToken', data.token);
            sessionStorage.setItem('astraDbName', data.db_name);
            sessionStorage.setItem('astraKeyspace', data.keyspace);
            console.log("Connection successful, saved details to session storage.");

            populateCollections(result.collections);
            showStatus('Connected. Select a vector-enabled collection.');
            collectionsSection.classList.remove('hidden');
        } else if (result.collections) {
            // If connection is successful but no vector collections found, clear stored credentials
            sessionStorage.removeItem('astraEndpointUrl');
            sessionStorage.removeItem('astraToken');
            sessionStorage.removeItem('astraDbName');
            sessionStorage.removeItem('astraKeyspace');
            showError('No vector-enabled collections found for this keyspace.');
        } else {
            // Handle cases where the server response is unexpected or invalid
            sessionStorage.removeItem('astraEndpointUrl');
            sessionStorage.removeItem('astraToken');
            sessionStorage.removeItem('astraDbName');
            sessionStorage.removeItem('astraKeyspace');
            showError('Failed to fetch collections. Invalid response from server.');
            console.error('Invalid response format:', result);
        }

    } catch (error) {
        // On any connection error, clear stored credentials
        sessionStorage.removeItem('astraEndpointUrl');
        sessionStorage.removeItem('astraToken');
        sessionStorage.removeItem('astraDbName');
        sessionStorage.removeItem('astraKeyspace');
        console.error('Error connecting/fetching collections:', error);
        showError(`Failed to connect or fetch collections: ${error.message}`);
    }
});
// --- End Added Click Listener ---

// Populate the collections list with available vector-enabled collections
// This creates radio buttons for each collection and handles selection
function populateCollections(collectionDetails) {
    collectionsListDiv.innerHTML = '';
    if (!collectionDetails || collectionDetails.length === 0) {
        showError('No vector-enabled collections with dimensions found.');
        return;
    }

    collectionDetails.forEach(colDetail => {
        const colName = colDetail.name;
        const dimension = colDetail.dimension;
        const count = colDetail.count;

        // Format the count display based on the type of count information available
        let countDisplay = '(Count: Unknown)';
        if (typeof count === 'number') {
            countDisplay = `(Estimated: ${count.toLocaleString()} docs)`;
        } else if (count !== 'N/A' && count !== 'Error') {
            countDisplay = `(Count: ${count})`;
        } else if (count === 'Error') {
            countDisplay = '(Count: Error)';
        }

        // Collections using 'vectorize' might not have a fixed dimension specified upfront
        // For simplicity, we currently skip displaying these collections
        if (dimension === 0) {
            console.log(`Collection ${colName} uses vectorize, dimension not specified.`);
            return;
        }

        // Create radio button for collection selection
        const radio = document.createElement('input');
        radio.type = 'radio';
        radio.id = `col-${colName}`;
        radio.name = 'collection';
        radio.value = colName;
        radio.dataset.dimension = dimension;
        radio.dataset.count = count;

        // Handle collection selection
        radio.addEventListener('change', () => {
            selectedCollection = colName;
            selectedCollectionDimension = parseInt(radio.dataset.dimension, 10);
            selectedCollectionCount = radio.dataset.count;

            // Update the help text to show collection count information
            const helpTextElement = document.getElementById('doc_limit_help_text');
            if (helpTextElement) {
                let baseText = "Specify the maximum number of documents to fetch and save.";
                let countInfo = "";
                if (selectedCollectionCount !== null && selectedCollectionCount !== undefined && 
                    selectedCollectionCount !== 'Error' && selectedCollectionCount !== 'Unknown' && 
                    selectedCollectionCount !== 'N/A') {
                    try {
                        // Format count as a number if possible, otherwise use as string
                        const numericCount = parseInt(selectedCollectionCount, 10);
                        if (!isNaN(numericCount)) {
                            countInfo = ` (Collection estimated count: ${numericCount.toLocaleString()})`;
                        } else {
                            countInfo = ` (Collection count: ${selectedCollectionCount})`;
                        }
                    } catch (e) {
                        countInfo = ` (Collection count: ${selectedCollectionCount})`;
                    }
                } else if (selectedCollectionCount === 'Error') {
                    countInfo = " (Could not estimate collection count)";
                }
                helpTextElement.textContent = baseText + countInfo;
            }

            // Reset UI state for new collection selection
            sampleButton.disabled = false;
            showStatus(`Collection '${colName}' selected. Ready to fetch sample & keys.`);
            metadataSelectionSection.classList.add('hidden');
            samplingPreviewSection.classList.add('hidden');
            configSection.classList.add('hidden');
            generateConfigButton.disabled = true;
            metadataKeysListDiv.innerHTML = '';
            sampleDataContainer.innerHTML = '';
            const docLimitInput = document.getElementById('doc_limit_input');
            if(docLimitInput) {
                docLimitInput.value = '';
            }
        });

        // Create and append the collection label
        const label = document.createElement('label');
        label.htmlFor = `col-${colName}`;
        label.textContent = `${colName} ${countDisplay}`;
        label.style.display = 'inline-block';
        label.style.marginLeft = '0.5em';
        label.style.marginRight = '1.5em';

        const div = document.createElement('div');
        div.appendChild(radio);
        div.appendChild(label);
        collectionsListDiv.appendChild(div);
    });
}

// Handle the sample button click to fetch sample data and metadata keys
sampleButton.addEventListener('click', async () => {
    if (!selectedCollection || !currentConnection) {
        showError('Connection details or collection not selected.');
        return;
    }
    showStatus(`Fetching sample data & metadata keys for '${selectedCollection}'...`);
    metadataSelectionSection.classList.add('hidden');
    samplingPreviewSection.classList.add('hidden');
    configSection.classList.add('hidden');
    generateConfigButton.disabled = true;
    metadataKeysListDiv.innerHTML = '';
    sampleDataContainer.innerHTML = '';

    try {
        // Fetch sample documents from the collection
        const sampleResponse = await fetch('/api/astra/sample', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                connection: currentConnection,
                collection_name: selectedCollection
            })
        });
        if (!sampleResponse.ok) {
            const errorData = await sampleResponse.text();
            throw new Error(`Sample fetch error ${sampleResponse.status}: ${errorData}`);
        }
        const sampleResult = await sampleResponse.json();

        if (sampleResult.sample_data && sampleResult.sample_data.length > 0) {
            currentSampleData = sampleResult.sample_data;
            showStatus(`Sample data received. Fetching metadata keys...`);

            // Analyze sample documents to identify potential metadata fields
            const keysResponse = await fetch('/api/astra/metadata_keys', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ sample_data: currentSampleData })
            });
            if (!keysResponse.ok) {
                const errorData = await keysResponse.text();
                throw new Error(`Metadata keys fetch error ${keysResponse.status}: ${errorData}`);
            }
            const keysResult = await keysResponse.json();

            if (keysResult.keys) {
                availableMetadataKeys = keysResult.keys;
                populateMetadataKeys(availableMetadataKeys, currentSampleData);
                metadataSelectionSection.classList.remove('hidden');
                generateConfigButton.disabled = false;
                previewDocsButton.disabled = false;
                showStatus(`Sample data fetched for '${selectedCollection}'. Select metadata fields and generate config, or preview data.`);
            } else {
                showError('Failed to fetch metadata keys from sample data.');
            }
        } else {
            showError('Failed to fetch sample data or sample is empty.');
            console.error('Invalid response format or empty sample:', sampleResult);
        }
    } catch (error) {
        console.error('Error during sampling or metadata key fetch:', error);
        showError(`Operation failed: ${error.message}`);
    }
});

// Display sample documents in a formatted way
// This creates a structured view of the document data with special handling for vectors
function displaySampleDocs(docs) {
    const container = document.querySelector('#sample-data');
    container.innerHTML = '';

    docs.forEach((doc, index) => {
        // Create a container for each document
        const docDiv = document.createElement('div');
        docDiv.style.border = '1px solid #eee';
        docDiv.style.marginBottom = '1em';
        docDiv.style.padding = '0.5em';

        // Add document header with ID
        const header = document.createElement('h5');
        header.textContent = `Document ${index + 1} (ID: ${doc._id || 'N/A'})`;
        header.style.marginTop = '0';
        docDiv.appendChild(header);

        // Create list for document fields
        const list = document.createElement('ul');
        list.style.listStyle = 'none';
        list.style.paddingLeft = '0.5em';

        // Process each field in the document
        for (const key in doc) {
            const listItem = document.createElement('li');
            const strong = document.createElement('strong');
            strong.textContent = `${key}: `;
            listItem.appendChild(strong);

            let valueStr;
            const value = doc[key];

            // Special formatting for vector fields to show dimension and preview
            if (key === '$vector') {
                valueStr = Array.isArray(value) ? `[${value.slice(0, 3).join(', ')} ... ] (${value.length} dims)` : String(value);
            } else if (typeof value === 'object' && value !== null) {
                // Attempt to stringify objects, fallback to simple string conversion
                try {
                    valueStr = JSON.stringify(value);
                } catch {
                    valueStr = String(value);
                }
            } else {
                valueStr = String(value);
            }

            // Truncate long string values for display
            const displayValue = valueStr.length > 100 ? valueStr.substring(0, 100) + '...' : valueStr;
            listItem.appendChild(document.createTextNode(displayValue));
            list.appendChild(listItem);
        }
        docDiv.appendChild(list);
        container.appendChild(docDiv);
    });
}

// Populate the metadata key selection checkboxes
// This creates checkboxes for each potential metadata field with smart defaults
function populateMetadataKeys(keys, sampleData) {
    metadataKeysListDiv.innerHTML = '';
    if (keys.length === 0) {
        metadataKeysListDiv.textContent = 'No potential metadata fields found (excluding _id, $vector).';
        return;
    }

    // Use the first sample document to help determine default checked state
    const firstDoc = (sampleData && sampleData.length > 0) ? sampleData[0] : null;
    console.log("Populating metadata keys. First doc for checks:", firstDoc);

    keys.forEach(keyName => {
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = `meta-${keyName}`;
        checkbox.name = 'metadata_key';
        checkbox.value = keyName;

        // --- Reinstate Heuristic for Default Checking --- 
        let shouldCheckByDefault = false;
        if (firstDoc && firstDoc.hasOwnProperty(keyName)) {
            const value = firstDoc[keyName];
            const valueType = typeof value;

            if (valueType === 'string' && value.length < 50) {
                // console.log(`Checking key '${keyName}' by default: String length ${value.length} < 50`);
                shouldCheckByDefault = true;
            } else if (valueType === 'number') {
                // console.log(`Checking key '${keyName}' by default: Is a number`);
                shouldCheckByDefault = true;
            }
            // Note: This heuristic also applies to primary key columns if they meet the criteria.
        }
        checkbox.checked = shouldCheckByDefault;
        // --- End Reinstated Heuristic ---

        // Create and append the metadata key label
        const label = document.createElement('label');
        label.htmlFor = `meta-${keyName}`;
        label.textContent = keyName;
        label.style.display = 'inline-block';
        label.style.marginLeft = '0.5em';
        label.style.marginRight = '1.5em';

        const div = document.createElement('div');
        div.appendChild(checkbox);
        div.appendChild(label);
        metadataKeysListDiv.appendChild(div);
    });
}

// Handle the preview button click to display sample documents
previewDocsButton.addEventListener('click', () => {
    if (currentSampleData && currentSampleData.length > 0) {
        console.log("Displaying stored sample data.");
        displaySampleDocs(currentSampleData);
        samplingPreviewSection.classList.remove('hidden');
        showStatus("Displaying sample documents.");
    } else {
        showError("No sample data available to preview. Fetch sample data first.");
        samplingPreviewSection.classList.add('hidden');
    }
});

// Handle the generate config button click
// This manages the process of saving data and generating the projector configuration
generateConfigButton.addEventListener('click', async () => {
    if (!currentConnection || !selectedCollection || !selectedCollectionDimension) {
        showError('Connection details, collection, or dimension not available.');
        return;
    }

    // Reset UI state for the save operation
    samplingPreviewSection.classList.add('hidden');
    sampleDataContainer.innerHTML = '';
    showStatus('Initiating data save process on the server...');

    // Get selected metadata keys, ensuring _id is always included
    const selectedKeys = Array.from(metadataKeysListDiv.querySelectorAll('input[name="metadata_key"]:checked')).map(cb => cb.value);
    if (!selectedKeys.includes('_id')) {
        selectedKeys.push('_id');
    }
    console.log("Selected metadata keys for server save:", selectedKeys);

    // Handle tensor name generation and sanitization
    const tensorNameInput = tensorNameOverrideInput.value.trim();
    const defaultTensorName = `${currentConnection.db_name || 'Astra'}_${selectedCollection}`;
    let tensorName = tensorNameInput || defaultTensorName;

    // Sanitize tensor name by replacing spaces with underscores
    // This avoids potential issues with file paths and downstream processing
    const sanitizedTensorName = tensorName.replace(/ /g, '_');
    console.log(`Sanitizing tensor name: '${tensorName}' -> '${sanitizedTensorName}'`);

    if (!sanitizedTensorName) {
        showError('Tensor name cannot be empty. Please provide one or ensure DB Name/Collection are set.');
        return;
    }

    // Process document limit if specified
    const docLimitInput = document.getElementById('doc_limit_input');
    let documentLimit = null;
    if (docLimitInput && docLimitInput.value) {
        const limitVal = parseInt(docLimitInput.value, 10);
        if (!isNaN(limitVal) && limitVal > 0) {
            documentLimit = limitVal;
        }
    }
    console.log("Document limit specified:", documentLimit);

    // Construct the request body for the save operation
    const requestBody = {
        connection: currentConnection,
        collection_name: selectedCollection,
        vector_dimension: selectedCollectionDimension,
        tensor_name: sanitizedTensorName,
        metadata_keys: selectedKeys,
        document_limit: documentLimit,
        // Sample data is not needed for the save operation
        sample_data: []
    };

    try {
        console.log("Sending payload to /api/astra/save_data:", JSON.stringify(requestBody, null, 2));
        const response = await fetch('/api/astra/save_data', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody)
        });

        if (!response.ok) {
            // Try to get detailed error information from the response
            let errorMsg = `HTTP error ${response.status}`;
            try {
                const errorData = await response.json();
                errorMsg = errorData.detail || JSON.stringify(errorData);
            } catch (e) {
                errorMsg = await response.text();
            }
            throw new Error(errorMsg);
        }

        const result = await response.json();
        console.log("Server response:", result);

        // Construct a user-friendly success message
        const limitAppliedMsg = result.limit_applied ? ` (Limit: ${result.limit_applied})` : " (All documents)";
        const successMessage = `Success! ${result.message}${limitAppliedMsg}.<br>
Vectors Saved: ${result.vectors_saved}<br>
Files Created:<br>
- ${result.vector_file}<br>
- ${result.metadata_file}<br>
Config Updated: <strong>${result.config_file}</strong>`;
        resultsDiv.innerHTML = successMessage;
        errorDiv.classList.add('hidden');

        // Clear previous links if any
        const existingLink = configSection.querySelector('a.projector-link');
        if (existingLink) {
            existingLink.remove();
        }
        // Add a link to the embedding projector inside the config section
        const projectorLink = document.createElement('a');
        projectorLink.href = `/?config=${encodeURIComponent(result.config_file)}`;
        projectorLink.textContent = "Go to Embedding Projector";
        projectorLink.className = 'projector-link'; // Add class for potential styling/removal
        projectorLink.target = "_blank"; // Add target="_blank" to open in new tab
        projectorLink.style.display = "block";
        projectorLink.style.marginTop = "1em";
        projectorLink.style.fontWeight = "bold";
        configSection.appendChild(projectorLink);

        samplingPreviewSection.classList.add('hidden');

    } catch (error) {
        console.error('Error saving data via backend:', error);

        // Format error message based on response type
        const displayError = error.message.startsWith('HTTP error') || error.message.includes('{')
                            ? error.message
                            : `Failed to save data: ${error.message}`;
        showError(displayError);
        samplingPreviewSection.classList.add('hidden');
    }
});

// --- Added Event Listener for Table Connection --- 
connectTablesButton.addEventListener('click', async (event) => {
    event.preventDefault();
    isTableMode = true; // Set mode to table
    showStatus('Connecting and fetching tables...');
    collectionsSection.classList.add('hidden'); // Hide collections section
    tablesSection.classList.add('hidden');
    metadataSelectionSection.classList.add('hidden');
    samplingPreviewSection.classList.add('hidden');
    configSection.classList.add('hidden');
    sampleTableButton.disabled = true;
    generateConfigButton.disabled = true;
    tablesListDiv.innerHTML = '';
    metadataKeysListDiv.innerHTML = '';

    const formData = new FormData(connectionForm);
    const data = Object.fromEntries(formData.entries());

    // Use 'default_keyspace' if the user leaves the keyspace field blank
    if (!data.keyspace) {
        data.keyspace = 'default_keyspace';
    }

    try {
        // Fetch tables from the backend API
        const response = await fetch('/api/astra/tables', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });

        if (!response.ok) {
            // Attempt to read error body as JSON first, then text
            let errorDataText = 'Unknown error';
            try {
                const errorDataJson = await response.json();
                errorDataText = errorDataJson.error || JSON.stringify(errorDataJson);
            } catch (jsonError) {
                errorDataText = await response.text(); // Fallback to text
            }
            throw new Error(`HTTP error ${response.status}: ${errorDataText}`);
        }
        const result = await response.json();

        if (result.tables && result.tables.length > 0) {
            // On successful connection, store details for potential reuse
            currentConnection = data;
            sessionStorage.setItem('astraEndpointUrl', data.endpoint_url);
            sessionStorage.setItem('astraToken', data.token);
            sessionStorage.setItem('astraDbName', data.db_name);
            sessionStorage.setItem('astraKeyspace', data.keyspace);
            console.log("Connection successful (Tables), saved details to session storage.");

            populateTables(result.tables);
            showStatus('Connected. Select a table and vector column.');
            tablesSection.classList.remove('hidden');
        } else if (result.tables) {
            sessionStorage.removeItem('astraEndpointUrl');
            sessionStorage.removeItem('astraToken');
            sessionStorage.removeItem('astraDbName');
            sessionStorage.removeItem('astraKeyspace');
            showError('No tables with suitable vector columns found for this keyspace.');
        } else {
            sessionStorage.removeItem('astraEndpointUrl');
            sessionStorage.removeItem('astraToken');
            sessionStorage.removeItem('astraDbName');
            sessionStorage.removeItem('astraKeyspace');
            showError('Failed to fetch tables. Invalid response from server.');
            console.error('Invalid table response format:', result);
        }

    } catch (error) {
        sessionStorage.removeItem('astraEndpointUrl');
        sessionStorage.removeItem('astraToken');
        sessionStorage.removeItem('astraDbName');
        sessionStorage.removeItem('astraKeyspace');
        console.error('Error connecting/fetching tables:', error);
        showError(`Failed to connect or fetch tables: ${error.message}`);
    }
});
// --- End Added Event Listener ---

// --- Added Function to Populate Tables List ---
function populateTables(tableDetails) {
    tablesListDiv.innerHTML = ''; // Clear previous content
    if (!tableDetails || tableDetails.length === 0) {
        showError('No tables with suitable vector columns found.');
        return;
    }

    // --- Re-implementing Table Layout --- 
    const table = document.createElement('table');
    table.style.width = '100%';
    table.style.borderCollapse = 'collapse';

    // Create table header
    const thead = table.createTHead();
    const headerRow = thead.insertRow();
    
    const th1 = document.createElement('th');
    th1.textContent = 'Table Name';
    th1.style.textAlign = 'left';
    th1.style.borderBottom = '1px solid #ccc';
    th1.style.padding = '8px';
    th1.style.verticalAlign = 'top'; // Align header top
    headerRow.appendChild(th1);

    const th2 = document.createElement('th');
    th2.textContent = 'Select Vector Column';
    th2.style.textAlign = 'left';
    th2.style.borderBottom = '1px solid #ccc';
    th2.style.padding = '8px';
    th2.style.verticalAlign = 'top'; // Align header top
    headerRow.appendChild(th2);

    // Create table body
    const tbody = table.createTBody();

    tableDetails.forEach(tableDetail => {
        const tableName = tableDetail.name;
        const vectorColumns = tableDetail.vector_columns;
        const primaryKeys = tableDetail.primary_key_columns;
        const count = tableDetail.count; // Keep count data for dataset, but don't display

        // Create a table row for this database table
        const row = tbody.insertRow();
        row.style.borderBottom = '1px solid #eee';

        // Cell 1: Table Name
        const cell1 = row.insertCell();
        cell1.textContent = tableName;
        cell1.style.padding = '8px';
        cell1.style.verticalAlign = 'top'; // Align name to top

        // Cell 2: Vector Column Radio Buttons
        const cell2 = row.insertCell();
        cell2.style.padding = '8px';
        cell2.style.verticalAlign = 'top'; // Align cell content to top

        // Create radio buttons for each vector column within the table
        vectorColumns.forEach((vecCol, index) => { // Added index for line break logic
            const vecColName = vecCol.name;
            const dimension = vecCol.dimension;
            const radioId = `table-${tableName}-vec-${vecColName}`;

            const radio = document.createElement('input');
            radio.type = 'radio';
            radio.id = radioId;
            radio.name = 'table-vector-selection'; // Group all table/vector radios
            radio.value = JSON.stringify({ table: tableName, vectorCol: vecColName }); // Store both
            radio.dataset.dimension = dimension;
            radio.dataset.count = count; // Store table count
            radio.dataset.primaryKeys = JSON.stringify(primaryKeys);
            radio.style.marginRight = '0.3em'; // Space between radio and label

            radio.addEventListener('change', () => {
                const selection = JSON.parse(radio.value);
                selectedTable = selection.table;
                selectedVectorColumn = selection.vectorCol;
                selectedTableDimension = parseInt(radio.dataset.dimension, 10);
                selectedTablePrimaryKeyCols = JSON.parse(radio.dataset.primaryKeys || '[]');
                selectedTableCount = radio.dataset.count; // Store count even if not displayed

                // Update the help text for document limit (No count display)
                const helpTextElement = document.getElementById('doc_limit_help_text');
                if (helpTextElement) {
                    helpTextElement.textContent = "Specify the maximum number of rows to fetch and save.";
                }

                // Reset UI state for new table/vector selection
                sampleTableButton.disabled = false;
                showStatus(`Table '${selectedTable}', Vector Column '${selectedVectorColumn}' selected. Ready to fetch sample & keys.`);
                metadataSelectionSection.classList.add('hidden');
                samplingPreviewSection.classList.add('hidden');
                configSection.classList.add('hidden');
                generateConfigButton.disabled = true;
                metadataKeysListDiv.innerHTML = '';
                sampleDataContainer.innerHTML = '';
                const docLimitInput = document.getElementById('doc_limit_input');
                if(docLimitInput) {
                    docLimitInput.value = '';
                }
            });

            const label = document.createElement('label');
            label.htmlFor = radioId;
            label.textContent = `${vecColName} (Dim: ${dimension})`; 
            label.style.marginRight = '1em'; // Space between options if multiple exist
            label.style.display = 'inline-block'; // Ensure label is inline

            // Append radio and label directly to the cell
            cell2.appendChild(radio);
            cell2.appendChild(label);
            
            // Add a line break AFTER the label if there are multiple vector options for this table
            // AND it's not the last option.
            if (vectorColumns.length > 1 && index < vectorColumns.length - 1) {
                 cell2.appendChild(document.createElement('br'));
            }
        });
    });

    // Append the table to the container div
    tablesListDiv.appendChild(table);
    // --- End Re-implementation --- 
}
// --- End Added Function ---

// --- Added Event Listener for Table Sampling --- 
sampleTableButton.addEventListener('click', async () => {
    if (!selectedTable || !selectedVectorColumn || !currentConnection) {
        showError('Connection details or table/vector column not selected.');
        return;
    }
    showStatus(`Fetching sample data & metadata keys for table '${selectedTable}' (vector: '${selectedVectorColumn}')...`);
    metadataSelectionSection.classList.add('hidden');
    samplingPreviewSection.classList.add('hidden');
    configSection.classList.add('hidden');
    generateConfigButton.disabled = true;
    metadataKeysListDiv.innerHTML = '';
    sampleDataContainer.innerHTML = '';

    const payload = {
        connection: currentConnection,
        table_name: selectedTable,
        vector_column: selectedVectorColumn
    };

    try {
        // Fetch sample data from the backend API for tables
        const response = await fetch('/api/astra/sample_table', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
             let errorDataText = 'Unknown error';
            try {
                const errorDataJson = await response.json();
                errorDataText = errorDataJson.error || JSON.stringify(errorDataJson);
            } catch (jsonError) {
                errorDataText = await response.text(); // Fallback to text
            }
            throw new Error(`HTTP error ${response.status}: ${errorDataText}`);
        }
        const result = await response.json();

        if (result.sample_data) {
            currentSampleData = result.sample_data;
            // Proceed to get metadata keys using the sampled table data
            await getMetadataKeys(currentSampleData, true); // Pass true for table mode
        } else {
            showError('Failed to fetch sample data from table. Invalid response from server.');
            console.error('Invalid sample table data response:', result);
        }

    } catch (error) {
        console.error('Error sampling table data:', error);
        showError(`Failed to sample table data: ${error.message}`);
    }
});
// --- End Added Listener ---

// Fetch metadata keys based on sample data
// Modified to accept an isTable flag
async function getMetadataKeys(sampleData, isTable = false) { 
    if (!sampleData || sampleData.length === 0) {
        showError('No sample data available to extract metadata keys.');
        metadataSelectionSection.classList.add('hidden');
        return;
    }
    showStatus('Analyzing sample data for metadata keys...');

    const payload = { sample_data: sampleData };

    try {
        const response = await fetch('/api/astra/metadata_keys', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
             let errorDataText = 'Unknown error';
            try {
                const errorDataJson = await response.json();
                errorDataText = errorDataJson.error || JSON.stringify(errorDataJson);
            } catch (jsonError) {
                errorDataText = await response.text(); // Fallback to text
            }
            throw new Error(`HTTP error ${response.status}: ${errorDataText}`);
        }
        const result = await response.json();

        if (result.keys) {
            availableMetadataKeys = result.keys;
            // Pass the isTable flag and potentially vector column name to populateMetadataKeys
            populateMetadataKeys(availableMetadataKeys, sampleData, isTable, isTable ? selectedVectorColumn : null, isTable ? selectedTablePrimaryKeyCols : []);
            showStatus('Sample data fetched. Select metadata fields to include.');
            metadataSelectionSection.classList.remove('hidden');
            samplingPreviewSection.classList.add('hidden'); // Ensure preview is hidden initially
            previewDocsButton.disabled = false; // Enable preview button
            generateConfigButton.disabled = false; // Enable generate button
            
            // Set default tensor name based on mode
            const dbName = currentConnection.db_name || 'db';
            const targetName = isTable ? selectedTable : selectedCollection;
            tensorNameOverrideInput.placeholder = `${dbName}_${targetName}`;
            tensorNameOverrideInput.value = ''; // Clear previous override

        } else {
            showError('Failed to get metadata keys. Invalid response from server.');
            console.error('Invalid metadata keys response:', result);
        }

    } catch (error) {
        console.error('Error fetching metadata keys:', error);
        showError(`Failed to fetch metadata keys: ${error.message}`);
    }
}

// Display sample documents in a readable format
// Modified slightly to handle table data potentially missing `$vector`
function displaySampleDocs(docs, isTable = false, vectorColName = '$vector') { // Add flags
    sampleDataContainer.innerHTML = ''; // Clear previous
    if (!docs || docs.length === 0) {
        sampleDataContainer.textContent = 'No sample documents to display.';
        samplingPreviewSection.classList.add('hidden');
        return;
    }

    const pre = document.createElement('pre');
    const code = document.createElement('code');

    // Format each document for display
    const formattedDocs = docs.map(doc => {
        const displayDoc = { ...doc };
        // Handle vector display - show placeholder if it exists
        const vectorKey = isTable ? vectorColName : '$vector';
        if (displayDoc[vectorKey] && Array.isArray(displayDoc[vectorKey])) {
            displayDoc[vectorKey] = `[${displayDoc[vectorKey][0]}, ${displayDoc[vectorKey][1]}, ..., ${displayDoc[vectorKey][displayDoc[vectorKey].length - 1]}] (Dim: ${displayDoc[vectorKey].length})`;
        } else if (displayDoc[vectorKey]) {
            displayDoc[vectorKey] = '[Vector Data Present]'; // Placeholder for non-array vectors if they occur
        }
        return JSON.stringify(displayDoc, null, 2); // Pretty print
    });

    code.textContent = formattedDocs.join('\n\n---\n\n'); // Separate docs
    pre.appendChild(code);
    sampleDataContainer.appendChild(pre);
    samplingPreviewSection.classList.remove('hidden');
    showStatus('Sample data preview displayed below. Adjust metadata selection if needed.');
}

// Handle the preview button click
previewDocsButton.addEventListener('click', () => {
    if (!currentSampleData) {
        showError('No sample data has been fetched yet.');
        return;
    }
    // Pass mode and vector column name for correct display
    displaySampleDocs(currentSampleData, isTableMode, isTableMode ? selectedVectorColumn : '$vector'); 
});

// Populate the metadata keys list with checkboxes
// Modified significantly for table mode
function populateMetadataKeys(keys, sampleData, isTable = false, vectorColName = null, primaryKeys = []) {
    metadataKeysListDiv.innerHTML = '';
    availableMetadataKeys = keys; // Store for later use

    // Determine the effective primary key representation (for potential later use, not filtering)
    // let primaryKeyRepresentation = isTable ? (primaryKeys.length === 1 ? primaryKeys[0] : 'PRIMARY_KEY') : '_id';
    // let primaryKeySourceColumns = isTable ? primaryKeys : ['_id'];

    // Filter out ONLY the vector key used for this dataset
    const vectorKeyToExclude = isTable ? vectorColName : '$vector';
    const keysForSelection = keys.filter(key => key !== vectorKeyToExclude);

    if (keysForSelection.length === 0) {
        metadataKeysListDiv.innerHTML = '<p>No additional metadata fields found in sample data (excluding vector column).</p>';
    }

    keysForSelection.forEach(key => {
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = `meta-${key}`;
        checkbox.name = 'metadata_keys';
        checkbox.value = key;
        // checkbox.checked = true; // REMOVE: Don't default all to checked

        // --- Reinstate Heuristic for Default Checking --- 
        let shouldCheckByDefault = false;
        const firstDoc = (sampleData && sampleData.length > 0) ? sampleData[0] : null;
        if (firstDoc && firstDoc.hasOwnProperty(key)) {
            const value = firstDoc[key];
            const valueType = typeof value;

            if (valueType === 'string' && value.length < 50) {
                // console.log(`Checking key '${key}' by default: String length ${value.length} < 50`);
                shouldCheckByDefault = true;
            } else if (valueType === 'number') {
                // console.log(`Checking key '${key}' by default: Is a number`);
                shouldCheckByDefault = true;
            }
            // Note: This heuristic also applies to primary key columns if they meet the criteria.
        }
        checkbox.checked = shouldCheckByDefault;
        // --- End Reinstated Heuristic ---

        const label = document.createElement('label');
        label.htmlFor = `meta-${key}`;
        label.textContent = key;
        label.style.display = 'inline-block';
        label.style.marginLeft = '0.5em';
        label.style.marginRight = '1.5em';

        const div = document.createElement('div');
        div.appendChild(checkbox);
        div.appendChild(label);
        metadataKeysListDiv.appendChild(div);
    });

    generateConfigButton.disabled = false;

    // REMOVED: Do not automatically display the sample data here
    // displaySampleDocs(sampleData, isTable, vectorColName);
}


// Handle the generate config button click
// ** NEEDS SIGNIFICANT MODIFICATION for tables **
generateConfigButton.addEventListener('click', async () => {
    if (!currentConnection) {
        showError('Connection details not available.');
        return;
    }
    if (!isTableMode && !selectedCollection) {
         showError('Collection not selected.');
         return;
    }
     if (isTableMode && (!selectedTable || !selectedVectorColumn)) {
        showError('Table or vector column not selected.');
        return;
    }

    const selectedMetadataKeys = Array.from(document.querySelectorAll('input[name="metadata_keys"]:checked')).map(cb => cb.value);
    const docLimitInput = document.getElementById('doc_limit_input');
    const docLimit = docLimitInput.value ? parseInt(docLimitInput.value, 10) : null;

    if (docLimit !== null && isNaN(docLimit)) {
        showError('Invalid document limit specified.');
        return;
    }

    // Determine tensor name: use override or generate default
    const tensorNameDefault = `${currentConnection.db_name || 'db'}_${isTableMode ? selectedTable : selectedCollection}`;
    const tensorName = tensorNameOverrideInput.value.trim() || tensorNameDefault;
    const safeTensorName = tensorName.replace(/\s+/g, '_'); // Replace spaces with underscores

    showStatus(`Generating config and fetching data for tensor '${safeTensorName}'... (Limit: ${docLimit || 'All'})`);
    configSection.classList.add('hidden');

    // Log dimension values before creating payload
    console.log(`Mode: ${isTableMode ? 'Table' : 'Collection'}`);
    console.log(`Selected Table Dimension: ${selectedTableDimension}`);
    console.log(`Selected Collection Dimension: ${selectedCollectionDimension}`);

    // *** Prepare payload ***
    const payload = {
        connection: currentConnection,
        tensor_name: safeTensorName,
        metadata_keys: selectedMetadataKeys,
        document_limit: docLimit
    };

    if (isTableMode) {
        // Add table-specific details to payload
        payload.table_name = selectedTable;
        payload.vector_column = selectedVectorColumn;
        payload.vector_dimension = selectedTableDimension;
        payload.primary_key_columns = selectedTablePrimaryKeyCols;
        // Remove collection specific fields if they exist (Ensure vector_dimension is NOT deleted)
        delete payload.collection_name; // Okay to remove if present
        // delete payload.vector_dimension; // ENSURE THIS REMAINS COMMENTED OR REMOVED
    } else {
         // Add collection-specific details to payload
        payload.collection_name = selectedCollection;
        payload.vector_dimension = selectedCollectionDimension;
         // Remove table specific fields if they exist
        delete payload.table_name;
        delete payload.vector_column;
        delete payload.primary_key_columns;
    }

    // *** API Endpoint - Needs to potentially point to a new endpoint or have logic in existing one ***
    const apiUrl = '/api/astra/save_data'; // Currently points to the collection saver

    try {
        console.log("Sending payload to /api/astra/save_data:", JSON.stringify(payload, null, 2));
        // *** THIS FETCH CALL NEEDS TO GO TO AN UPDATED BACKEND THAT HANDLES BOTH MODES ***
        const response = await fetch(apiUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            let errorDataText = 'Unknown error';
            try {
                const errorDataJson = await response.json();
                errorDataText = errorDataJson.error || errorDataJson.detail || JSON.stringify(errorDataJson);
            } catch (jsonError) {
                 try {
                      errorDataText = await response.text();
                 } catch (textError) {
                      errorDataText = `Server returned status ${response.status}`;
                 }
            }
            throw new Error(`HTTP error ${response.status}: ${errorDataText}`);
        }

        const result = await response.json();

        if (result.message && result.config_file) {
            showStatus(result.message + ` Config path: ${result.config_file}`);
            
            // --- Display Generated Tensor Entry --- 
            const configDetailsCode = document.getElementById('config-details-json');
            if (configDetailsCode) {
                const tensorEntry = {
                    tensorName: result.tensor_name,
                    tensorShape: result.tensor_shape,
                    tensorPath: result.tensor_path_rel,
                    metadataPath: result.metadata_path_rel
                };
                configDetailsCode.textContent = JSON.stringify(tensorEntry, null, 2);
            }
            // --- End Tensor Entry Display ---

            configSection.classList.remove('hidden');

            // --- MOVE Link Generation Here ---
            // Clear previous links if any
            const existingLink = configSection.querySelector('a.projector-link');
            if (existingLink) {
                existingLink.remove();
            }
            // Add a link to the embedding projector inside the config section
            const projectorLink = document.createElement('a');
            projectorLink.href = `/?config=${encodeURIComponent(result.config_file)}`;
            projectorLink.textContent = "Go to Embedding Projector";
            projectorLink.className = 'projector-link'; // Add class for potential styling/removal
            projectorLink.target = "_blank"; // Add target="_blank" to open in new tab
            projectorLink.style.display = "block";
            projectorLink.style.marginTop = "1em";
            projectorLink.style.fontWeight = "bold";
            configSection.appendChild(projectorLink); 
            // --- End Moved Link ---

        } else {
             showError('Failed to save data. Invalid response from server.');
             console.error('Invalid save response:', result);
        }

    } catch (error) {
        console.error('Error saving data:', error);
        showError(`Failed to save data: ${error.message}`);
    }
});