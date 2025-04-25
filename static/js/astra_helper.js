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
const docLimitInput = document.getElementById('doc_limit_input');
const samplingStrategyGroup = document.getElementById('sampling-strategy-group');
const firstRowsRadio = document.getElementById('sampling_first_rows');
const tokenRangeRadio = document.getElementById('sampling_token_range');

// Added elements for Tables
const tablesSection = document.getElementById('tables-section');
const tablesListDiv = document.getElementById('tables-list');
const sampleTableButton = document.getElementById('sample-table-button');
const connectTablesButton = document.getElementById('connect-tables-button');

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
let selectedTablePartitionKeyCols = [];
let selectedTableCount = null;
let isTableMode = false;

// Initialize the page by checking for stored connection details
// This allows users to refresh without re-entering credentials
window.addEventListener('DOMContentLoaded', () => {
    // console.log("Page loaded. Checking session storage for connection details.");
    const storedEndpoint = sessionStorage.getItem('astraEndpointUrl');
    const storedToken = sessionStorage.getItem('astraToken');
    const storedDbName = sessionStorage.getItem('astraDbName');
    const storedKeyspace = sessionStorage.getItem('astraKeyspace');

    if (storedEndpoint && storedToken) {
        // console.log("Found stored details, populating form.");
        connectionForm.elements['endpoint_url'].value = storedEndpoint;
        connectionForm.elements['token'].value = storedToken;
        connectionForm.elements['db_name'].value = storedDbName || '';
        connectionForm.elements['keyspace'].value = storedKeyspace || '';
        showStatus("Loaded connection details from session storage. Click 'Connect & List Collections' to proceed.");
    } else {
         // console.log("No connection details found in session storage.");
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

// --- Click Listener for Collections Button ---
connectCollectionsButton.addEventListener('click', async () => {
    // Note: event.preventDefault() is NOT needed for type="button"
    isTableMode = false; // Set mode to collection
    showStatus('Connecting and fetching collections...');
    resetUIForNewConnection(); // Use helper function to reset UI
    samplingStrategyGroup.classList.add('hidden'); // Ensure sampling group is hidden for collections

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
            // console.log("Connection successful, saved details to session storage.");

            populateCollections(result.collections);
            showStatus('Connected. Select a vector-enabled collection.');
            collectionsSection.classList.remove('hidden');
            tablesSection.classList.add('hidden'); // Ensure tables section is hidden
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
// --- End Collections Click Listener ---

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

        // Skip collections using 'vectorize' or without dimension
        if (!dimension || dimension <= 0) {
            // console.log(`Collection ${colName} skipped (dimension missing or zero).`);
            return; // Skip this collection
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

            // Update help text for document limit
            updateDocLimitHelpText(selectedCollectionCount);

            // Reset UI state for new collection selection
            sampleButton.disabled = false;
            showStatus(`Collection '${colName}' selected. Ready to fetch sample & keys.`);
            metadataSelectionSection.classList.add('hidden');
            samplingPreviewSection.classList.add('hidden');
            configSection.classList.add('hidden');
            samplingStrategyGroup.classList.add('hidden'); // Hide sampling strategy for collections
            generateConfigButton.disabled = true;
            metadataKeysListDiv.innerHTML = '';
            sampleDataContainer.innerHTML = '';
            docLimitInput.value = ''; // Clear limit input
            tensorNameOverrideInput.value = ''; // Clear tensor name override
            firstRowsRadio.checked = true; // Reset sampling strategy
            tokenRangeRadio.checked = false;
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
    if (!currentConnection || !selectedCollection) {
        showError('Connection details or collection not selected.');
        return;
    }
    isTableMode = false; // Ensure mode is collection
    showStatus(`Fetching sample data for collection '${selectedCollection}'...`);
    metadataSelectionSection.classList.add('hidden');
    samplingPreviewSection.classList.add('hidden');
    configSection.classList.add('hidden');
    samplingStrategyGroup.classList.add('hidden'); // Hide sampling strategy for collections
    generateConfigButton.disabled = true;
    sampleDataContainer.innerHTML = ''; // Clear previous preview
    metadataKeysListDiv.innerHTML = '';

    try {
        const response = await fetch('/api/astra/sample', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                connection: currentConnection,
                collection_name: selectedCollection
            })
        });
        if (!response.ok) throw new Error(`HTTP error ${response.status}`);
        const result = await response.json();
        currentSampleData = result.sample_data || [];
        if (currentSampleData.length === 0) {
             showError(`No sample documents found in collection '${selectedCollection}'. Cannot proceed.`);
             return; // Stop if no sample data
        }
        showStatus(`Fetched ${currentSampleData.length} sample documents. Analyzing metadata keys...`);
        // Call the unified metadata key fetcher
        await getMetadataKeys(currentSampleData, false); // isTable = false

    } catch (error) {
        console.error('Error fetching collection sample data:', error);
        showError(`Failed to fetch collection sample data: ${error.message}`);
    }
});
// --- End Sample Button Listener (Collections) ---

// --- Click Listener for Tables Button ---
connectTablesButton.addEventListener('click', async (event) => {
    event.preventDefault();
    isTableMode = true; // Set mode to table
    showStatus('Connecting and fetching tables...');
    resetUIForNewConnection(); // Use helper function to reset UI
    samplingStrategyGroup.classList.add('hidden'); // Ensure sampling group is hidden initially

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
            // console.log("Connection successful (Tables), saved details to session storage.");

            populateTables(result.tables);
            showStatus('Connected. Select a table and vector column.');
            tablesSection.classList.remove('hidden');
            collectionsSection.classList.add('hidden'); // Ensure collections section is hidden
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
// --- End Tables Click Listener ---

// --- Function to Populate Tables List --- 
function populateTables(tableDetails) {
    tablesListDiv.innerHTML = '';
    if (!tableDetails || tableDetails.length === 0) {
        showError('No tables with vector columns found.');
        return;
    }

    // --- Use Table Layout --- 
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
    th1.style.verticalAlign = 'top'; 
    headerRow.appendChild(th1);

    const th2 = document.createElement('th');
    th2.textContent = 'Select Vector Column';
    th2.style.textAlign = 'left';
    th2.style.borderBottom = '1px solid #ccc';
    th2.style.padding = '8px';
    th2.style.verticalAlign = 'top'; 
    headerRow.appendChild(th2);

    // Create table body
    const tbody = table.createTBody();

    tableDetails.forEach(tableDetail => {
        const tableName = tableDetail.name;
        const vectorColumns = tableDetail.vector_columns;
        const primaryKeyCols = tableDetail.primary_key_columns || []; // Array of strings
        // Use partition keys if available, otherwise default to primary keys
        const partitionKeyCols = tableDetail.partition_key_columns && tableDetail.partition_key_columns.length > 0 
                                ? tableDetail.partition_key_columns 
                                : primaryKeyCols; 
        const count = tableDetail.count; // Store count (could be string like 'N/A')

        if (!vectorColumns || vectorColumns.length === 0) return; // Skip table if no vector cols

        // Create a table row for this database table
        const row = tbody.insertRow();
        row.style.borderBottom = '1px solid #eee';

        // Cell 1: Table Name & PK Info
        const cell1 = row.insertCell();
        cell1.style.padding = '8px';
        cell1.style.verticalAlign = 'top'; // Align name to top
        cell1.textContent = tableName;
        const pkSpan = document.createElement('span');
        pkSpan.textContent = ` (PK: ${primaryKeyCols.join(', ')})`;
        pkSpan.style.fontSize = '0.9em';
        pkSpan.style.color = '#555';
        pkSpan.style.display = 'block'; // Put PK below name
        cell1.appendChild(pkSpan);

        // Cell 2: Vector Column Radio Buttons
        const cell2 = row.insertCell();
        cell2.style.padding = '8px';
        cell2.style.verticalAlign = 'top'; // Align cell content to top

        vectorColumns.forEach((vCol, index) => {
            const vColName = vCol.name;
            const dimension = vCol.dimension;
            const radioId = `table-${tableName}-vcol-${vColName}`;

            const radio = document.createElement('input');
            radio.type = 'radio';
            radio.id = radioId;
            radio.name = 'table_vector_column_selection'; // Group all table/vector radios
            radio.value = `${tableName}::${vColName}`; // Combine table and column name
            radio.dataset.tableName = tableName;
            radio.dataset.vectorColumn = vColName;
            radio.dataset.dimension = dimension;
            radio.dataset.count = count; // Store count
            // Store PKs and Partition Keys as JSON strings in data attributes
            radio.dataset.primaryKeyCols = JSON.stringify(primaryKeyCols);
            radio.dataset.partitionKeyCols = JSON.stringify(partitionKeyCols);
            radio.style.marginRight = '0.3em';

            radio.addEventListener('change', () => {
                selectedTable = tableName;
                selectedVectorColumn = vColName;
                selectedTableDimension = parseInt(dimension, 10);
                selectedTableCount = radio.dataset.count; // Store count
                selectedTablePrimaryKeyCols = JSON.parse(radio.dataset.primaryKeyCols || '[]');
                selectedTablePartitionKeyCols = JSON.parse(radio.dataset.partitionKeyCols || '[]');

                // Update help text for document limit
                updateDocLimitHelpText(selectedTableCount);

                // Reset UI state for new table/vector column selection
                sampleTableButton.disabled = false;
                showStatus(`Table '${tableName}', Vector Column '${vColName}' selected. Ready to fetch sample & keys.`);
                metadataSelectionSection.classList.add('hidden');
                samplingPreviewSection.classList.add('hidden');
                configSection.classList.add('hidden');
                samplingStrategyGroup.classList.remove('hidden'); // Show sampling strategy options for tables
                generateConfigButton.disabled = true;
                metadataKeysListDiv.innerHTML = '';
                sampleDataContainer.innerHTML = ''; // Clear sample data preview
                docLimitInput.value = ''; // Clear limit input
                tensorNameOverrideInput.value = ''; // Clear tensor name override
                firstRowsRadio.checked = true; // Reset sampling strategy
                tokenRangeRadio.checked = false;
            });

            const label = document.createElement('label');
            label.htmlFor = radioId;
            label.textContent = ` ${vColName} (Dim: ${dimension})`; 
            label.style.display = 'inline-block'; 
            label.style.marginRight = '1em'; // Space between options if multiple exist

            // Append radio and label directly to the cell
            const div = document.createElement('div'); // Use a div for each radio/label pair
            div.appendChild(radio);
            div.appendChild(label);
            cell2.appendChild(div);
        });
    });

    // Append the table to the container div
    tablesListDiv.appendChild(table);
    // --- End Table Layout ---
}
// --- End populateTables ---

// --- Helper function to reset UI state ---
function resetUIForNewConnection() {
    collectionsSection.classList.add('hidden');
    tablesSection.classList.add('hidden');
    metadataSelectionSection.classList.add('hidden');
    samplingPreviewSection.classList.add('hidden');
    configSection.classList.add('hidden');
    samplingStrategyGroup.classList.add('hidden'); // Hide sampling group
    sampleButton.disabled = true;
    sampleTableButton.disabled = true;
    generateConfigButton.disabled = true;
    collectionsListDiv.innerHTML = '';
    tablesListDiv.innerHTML = '';
    metadataKeysListDiv.innerHTML = '';
    sampleDataContainer.innerHTML = ''; // Clear sample data preview

    // Reset state variables
    selectedCollection = null;
    selectedTable = null;
    selectedVectorColumn = null;
    selectedCollectionDimension = null;
    selectedTableDimension = null;
    selectedCollectionCount = null;
    selectedTableCount = null;
    selectedTablePrimaryKeyCols = [];
    selectedTablePartitionKeyCols = []; // Reset partition keys
    currentSampleData = null;
    availableMetadataKeys = [];
    tensorNameOverrideInput.value = ''; // Clear tensor name override
    docLimitInput.value = ''; // Clear limit input
    firstRowsRadio.checked = true; // Reset sampling strategy
    tokenRangeRadio.checked = false;
}
// --- End resetUIForNewConnection ---

// --- Helper function to update doc limit help text ---
function updateDocLimitHelpText(count) {
    const helpTextElement = document.getElementById('doc_limit_help_text');
    if (!helpTextElement) return;

    let baseText = "Specify the maximum number of documents to fetch and save.";
    let countInfo = "";

    if (count !== null && count !== undefined &&
        count !== 'Error' && count !== 'Unknown' && count !== 'N/A') {
        try {
            // Format count as a number if possible, otherwise use as string
            const numericCount = parseInt(count, 10);
            if (!isNaN(numericCount)) {
                countInfo = ` (Estimated source count: ${numericCount.toLocaleString()})`;
            } else {
                countInfo = ` (Source count: ${count})`;
            }
        } catch (e) {
            countInfo = ` (Source count: ${count})`; // Fallback
        }
    } else if (count === 'Error') {
        countInfo = " (Could not estimate source count)";
    } else if (count === 'N/A') {
        countInfo = " (Source count not available)";
    }
    // If count is null, undefined, or Unknown, add no extra info.

    helpTextElement.textContent = baseText + countInfo;
}
// --- End updateDocLimitHelpText ---

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
            populateMetadataKeys(
                availableMetadataKeys,
                sampleData,
                isTable,
                isTable ? selectedVectorColumn : '$vector', // Pass correct vector key name
                isTable ? selectedTablePrimaryKeyCols : ['_id'] // Pass correct primary key(s)
            );
            showStatus('Sample data fetched. Select metadata fields to include.');
            metadataSelectionSection.classList.remove('hidden');
            samplingPreviewSection.classList.add('hidden'); // Ensure preview is hidden initially
            previewDocsButton.disabled = currentSampleData.length === 0; // Enable preview button
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
function displaySampleDocs(docs, isTable = false, vectorColName = '$vector') {
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
    if (currentSampleData && currentSampleData.length > 0) {
        samplingPreviewSection.classList.remove('hidden');
        displaySampleDocs(
            currentSampleData,
            isTableMode, // Pass table mode flag
            isTableMode ? selectedVectorColumn : '$vector' // Pass correct vector key
        );
        showStatus("Displaying fetched sample documents.");
         // Scroll to the preview section for convenience
         samplingPreviewSection.scrollIntoView({ behavior: 'smooth' });
    } else {
        showError("No sample data available to preview.");
        samplingPreviewSection.classList.add('hidden');
    }
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
    // console.log(`Mode: ${isTableMode ? 'Table' : 'Collection'}`);
    // console.log(`Selected Table Dimension: ${selectedTableDimension}`);
    // console.log(`Selected Collection Dimension: ${selectedCollectionDimension}`);

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

    // console.log("Payload for /api/astra/save_data:", JSON.stringify(payload, null, 2));

    // API call to save data and generate config
    const apiUrl = '/api/astra/save_data'; // Currently points to the collection saver

    try {
        // console.log("Sending payload to /api/astra/save_data:", JSON.stringify(payload, null, 2));
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
            let errorJson = null;
            try {
                 errorJson = await response.json();
                 // FastAPI validation errors are often in response.detail which might be an array or string
                 if (errorJson.detail) {
                      if (Array.isArray(errorJson.detail)) {
                          errorDataText = errorJson.detail.map(e => `${e.loc ? e.loc.join('.')+': ' : ''}${e.msg}`).join(', ');
                      } else {
                           errorDataText = errorJson.detail;
                      }
                 } else if (errorJson.error) { // Handle custom error format if used
                      errorDataText = errorJson.error;
                 } else {
                      errorDataText = JSON.stringify(errorJson); // Fallback
                 }

            } catch (e) {
                errorDataText = await response.text(); // Fallback to text
            }
            console.error("Save data error response:", errorJson || errorDataText); // Log error details
            throw new Error(errorDataText);
        }

        const result = await response.json();
        // console.log("Save data successful response:", result);

        showStatus(result.message || 'Configuration generated successfully.');
        
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
        projectorLink.className = 'projector-link'; // Class for styling/removal
        projectorLink.target = "_blank"; // Add target="_blank" to open in new tab
        projectorLink.style.display = "block";
        projectorLink.style.marginTop = "1em";
        projectorLink.style.fontWeight = "bold";
        configSection.appendChild(projectorLink); 
        // --- End Moved Link ---

    } catch (error) {
        console.error('Error saving data:', error);
        showError(`Failed to save data: ${error.message}`);
    }
});

// --- Helper function to display the results from save_data ---
function displayConfigResults(result) {
    const detailsContainer = document.getElementById('config-details-json');
    const configSection = document.getElementById('config-section'); // Ensure we target the section

    // Clear previous results (like old links)
    detailsContainer.textContent = ''; // Clear the code block
    const existingLink = configSection.querySelector('a.projector-link');
    if (existingLink) {
        existingLink.remove();
    }

    if (!result) {
        detailsContainer.textContent = 'No results received from server.';
        return;
    }

    // Display the tensor entry details
    const tensorEntry = {
        tensorName: result.tensor_name,
        tensorShape: result.tensor_shape,
        tensorPath: result.tensor_path_rel,
        metadataPath: result.metadata_path_rel
    };
    detailsContainer.textContent = JSON.stringify(tensorEntry, null, 2);

    // Add a link to the main projector page using the generated config file path
    if (result.config_file) {
        const projectorLink = document.createElement('a');
        // Use URLSearchParams to pass the config path correctly
        const params = new URLSearchParams();
        params.set('config', result.config_file);
        projectorLink.href = `/?${params.toString()}`;
        projectorLink.textContent = 'View in Embedding Projector';
        projectorLink.target = '_blank'; // Open in new tab
        projectorLink.style.display = 'block';
        projectorLink.style.marginTop = '1em';
        projectorLink.style.fontWeight = 'bold';
        detailsContainer.parentNode.insertBefore(projectorLink, detailsContainer.nextSibling); 
    } else {
        // console.warn("Config file path not found in response, cannot create projector link.");
    }
}
// --- End displayConfigResults ---

// --- Listener for Fetch Sample (Tables) ---
sampleTableButton.addEventListener('click', async () => {
    // console.log("Sample Table Button clicked."); // DEBUG

    if (!currentConnection || !selectedTable || !selectedVectorColumn) {
         // console.error("Missing state for table sample fetch:", { currentConnection, selectedTable, selectedVectorColumn }); // DEBUG
         showError('Connection, table, or vector column not selected.');
         return;
    }
    isTableMode = true; // Ensure mode is table
    // console.log(`Fetching table sample. Mode: ${isTableMode}, Table: ${selectedTable}, Vector Col: ${selectedVectorColumn}`); // DEBUG
    showStatus(`Fetching sample data for table '${selectedTable}'...`);
    // Reset relevant UI parts
    metadataSelectionSection.classList.add('hidden');
    samplingPreviewSection.classList.add('hidden');
    configSection.classList.add('hidden');
    // Sampling strategy group should already be visible
    generateConfigButton.disabled = true;
    previewDocsButton.disabled = true; // Disable preview until sample is fetched
    sampleDataContainer.innerHTML = '';
    metadataKeysListDiv.innerHTML = '';

    try {
        const payload = {
            connection: currentConnection,
            table_name: selectedTable,
            vector_column: selectedVectorColumn
        };
        // console.log("Sending payload to /api/astra/sample_table:", payload); // DEBUG

        const response = await fetch('/api/astra/sample_table', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });

        // console.log(`Sample table fetch response status: ${response.status}`); // DEBUG

        if (!response.ok) {
             let errorDetail = `HTTP error ${response.status}`;
             let errorJson = null;
             try {
                 errorJson = await response.json();
                 errorDetail += `: ${errorJson.error || JSON.stringify(errorJson)}`;
             } catch (e) {
                 try { errorDetail += `: ${await response.text()}`; } catch (e2) { /* ignore further errors */}
             }
             // console.error("Sample table fetch failed:", errorDetail, errorJson); // DEBUG
             throw new Error(errorDetail);
         }

        const result = await response.json();
        // console.log("Sample table fetch successful. Result:", result); // DEBUG
        currentSampleData = result.sample_data || [];
         if (currentSampleData.length === 0) {
             showError(`No sample documents found in table '${selectedTable}'. Cannot proceed.`);
             return;
         }
        showStatus(`Fetched ${currentSampleData.length} sample rows. Analyzing metadata keys...`);

        // console.log("Calling getMetadataKeys with isTable = true"); // DEBUG
        await getMetadataKeys(currentSampleData, true); // isTable = true

    } catch (error) {
        // console.error('Error during table sample button execution:', error); // DEBUG
        showError(`Failed to fetch table sample data: ${error.message}`);
    }
});
// --- End Sample Table Button Listener ---