// DOM element references for UI components
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

// DOM elements for table mode
const tablesSection = document.getElementById('tables-section');
const tablesListDiv = document.getElementById('tables-list');
const sampleTableButton = document.getElementById('sample-table-button');
const connectTablesButton = document.getElementById('connect-tables-button');
const connectCollectionsButton = document.getElementById('connect-collections-button');

// Global state variables
let currentConnection = null;
let selectedCollection = null;
let selectedCollectionDimension = null;
let selectedCollectionCount = null;
let currentSampleData = null;
let availableMetadataKeys = [];

// Table mode state variables
let selectedTable = null;
let selectedVectorColumn = null;
let selectedTableDimension = null;
let selectedTablePrimaryKeyCols = [];
let selectedTablePartitionKeyCols = [];
let selectedTableCount = null;
let isTableMode = false;

// Initialize page and check for stored connection details
window.addEventListener('DOMContentLoaded', () => {
    const storedEndpoint = sessionStorage.getItem('astraEndpointUrl');
    const storedToken = sessionStorage.getItem('astraToken');
    const storedDbName = sessionStorage.getItem('astraDbName');
    const storedKeyspace = sessionStorage.getItem('astraKeyspace');

    if (storedEndpoint && storedToken) {
        connectionForm.elements['endpoint_url'].value = storedEndpoint;
        connectionForm.elements['token'].value = storedToken;
        connectionForm.elements['db_name'].value = storedDbName || '';
        connectionForm.elements['keyspace'].value = storedKeyspace || '';
        showStatus("Loaded connection details from session storage. Click the appropriate 'List' button to proceed.");
    }
});

// Display error message in UI
function showError(message) {
    errorDiv.textContent = message;
    errorDiv.classList.remove('hidden');
    resultsDiv.textContent = 'Error occurred.';
}

// Display status message in UI
function showStatus(message) {
    resultsDiv.textContent = message;
    errorDiv.classList.add('hidden');
}

// Connect to Astra DB and fetch collections
connectCollectionsButton.addEventListener('click', async () => {
    isTableMode = false;
    showStatus('Connecting and fetching collections...');
    resetUIForNewConnection();
    samplingStrategyGroup.classList.remove('hidden');
    document.getElementById('token_range_option').classList.add('hidden');
    document.getElementById('distributed_option').classList.remove('hidden');

    const formData = new FormData(connectionForm);
    const data = Object.fromEntries(formData.entries());

    if (!data.keyspace) {
        data.keyspace = 'default_keyspace';
    }

    try {
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
            currentConnection = data;
            sessionStorage.setItem('astraEndpointUrl', data.endpoint_url);
            sessionStorage.setItem('astraToken', data.token);
            sessionStorage.setItem('astraDbName', data.db_name);
            sessionStorage.setItem('astraKeyspace', data.keyspace);

            populateCollections(result.collections);
            showStatus('Connected. Select a vector-enabled collection.');
            collectionsSection.classList.remove('hidden');
            tablesSection.classList.add('hidden');
        } else if (result.collections) {
            clearStoredCredentials();
            showError('No vector-enabled collections found for this keyspace.');
        } else {
            clearStoredCredentials();
            showError('Failed to fetch collections. Invalid response from server.');
            console.error('Invalid response format:', result);
        }

    } catch (error) {
        clearStoredCredentials();
        console.error('Error connecting/fetching collections:', error);
        showError(`Failed to connect or fetch collections: ${error.message}`);
    }
});

// Populate collections list with available vector collections
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

        let countDisplay = '(Count: Unknown)';
        if (typeof count === 'number') {
            countDisplay = `(Estimated: ${count.toLocaleString()} docs)`;
        } else if (count !== 'N/A' && count !== 'Error') {
            countDisplay = `(Count: ${count})`;
        } else if (count === 'Error') {
            countDisplay = '(Count: Error)';
        }

        if (!dimension || dimension <= 0) {
            return;
        }

        const radio = document.createElement('input');
        radio.type = 'radio';
        radio.id = `col-${colName}`;
        radio.name = 'collection';
        radio.value = colName;
        radio.dataset.dimension = dimension;
        radio.dataset.count = count;

        radio.addEventListener('change', () => {
            selectedCollection = colName;
            selectedCollectionDimension = parseInt(radio.dataset.dimension, 10);
            selectedCollectionCount = radio.dataset.count;

            updateDocLimitHelpText(selectedCollectionCount);

            sampleButton.disabled = false;
            showStatus(`Collection '${colName}' selected. Ready to fetch sample & keys.`);
            metadataSelectionSection.classList.add('hidden');
            samplingPreviewSection.classList.add('hidden');
            configSection.classList.add('hidden');
            samplingStrategyGroup.classList.remove('hidden');
            document.getElementById('token_range_option').classList.add('hidden');
            document.getElementById('distributed_option').classList.remove('hidden');
            generateConfigButton.disabled = true;
            metadataKeysListDiv.innerHTML = '';
            sampleDataContainer.innerHTML = '';
            docLimitInput.value = '';
            tensorNameOverrideInput.value = '';
            firstRowsRadio.checked = true;
            tokenRangeRadio.checked = false;
        });

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

// Fetch sample data from selected collection
sampleButton.addEventListener('click', async () => {
    if (!currentConnection || !selectedCollection) {
        showError('Connection details or collection not selected.');
        return;
    }
    isTableMode = false;
    showStatus(`Fetching sample data for collection '${selectedCollection}'...`);
    metadataSelectionSection.classList.add('hidden');
    samplingPreviewSection.classList.add('hidden');
    configSection.classList.add('hidden');
    samplingStrategyGroup.classList.add('hidden');
    generateConfigButton.disabled = true;
    sampleDataContainer.innerHTML = '';
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
             return;
        }
        showStatus(`Fetched ${currentSampleData.length} sample documents. Analyzing metadata keys...`);
        await getMetadataKeys(currentSampleData, false);

    } catch (error) {
        console.error('Error fetching collection sample data:', error);
        showError(`Failed to fetch collection sample data: ${error.message}`);
    }
});

// Connect to Astra DB and fetch tables
connectTablesButton.addEventListener('click', async (event) => {
    event.preventDefault();
    isTableMode = true;
    showStatus('Connecting and fetching tables...');
    resetUIForNewConnection();
    samplingStrategyGroup.classList.remove('hidden');
    document.getElementById('token_range_option').classList.remove('hidden');
    document.getElementById('distributed_option').classList.add('hidden');

    const formData = new FormData(connectionForm);
    const data = Object.fromEntries(formData.entries());

    if (!data.keyspace) {
        data.keyspace = 'default_keyspace';
    }

    try {
        const response = await fetch('/api/astra/tables', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });

        if (!response.ok) {
            let errorDataText = 'Unknown error';
            try {
                const errorDataJson = await response.json();
                errorDataText = errorDataJson.error || JSON.stringify(errorDataJson);
            } catch (jsonError) {
                errorDataText = await response.text();
            }
            throw new Error(`HTTP error ${response.status}: ${errorDataText}`);
        }
        const result = await response.json();

        if (result.tables && result.tables.length > 0) {
            currentConnection = data;
            sessionStorage.setItem('astraEndpointUrl', data.endpoint_url);
            sessionStorage.setItem('astraToken', data.token);
            sessionStorage.setItem('astraDbName', data.db_name);
            sessionStorage.setItem('astraKeyspace', data.keyspace);

            populateTables(result.tables);
            showStatus('Connected. Select a table and vector column.');
            tablesSection.classList.remove('hidden');
            collectionsSection.classList.add('hidden');
        } else if (result.tables) {
            clearStoredCredentials();
            showError('No tables with suitable vector columns found for this keyspace.');
        } else {
            clearStoredCredentials();
            showError('Failed to fetch tables. Invalid response from server.');
            console.error('Invalid table response format:', result);
        }

    } catch (error) {
        clearStoredCredentials();
        console.error('Error connecting/fetching tables:', error);
        showError(`Failed to connect or fetch tables: ${error.message}`);
    }
});

// Populate tables list with available vector tables
function populateTables(tableDetails) {
    tablesListDiv.innerHTML = '';
    if (!tableDetails || tableDetails.length === 0) {
        showError('No tables with vector columns found.');
        return;
    }

    const table = document.createElement('table');
    table.style.width = '100%';
    table.style.borderCollapse = 'collapse';

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

    const tbody = table.createTBody();

    tableDetails.forEach(tableDetail => {
        const tableName = tableDetail.name;
        const vectorColumns = tableDetail.vector_columns;
        const primaryKeyCols = tableDetail.primary_key_columns || [];
        const partitionKeyCols = tableDetail.partition_key_columns && tableDetail.partition_key_columns.length > 0 
                                ? tableDetail.partition_key_columns 
                                : primaryKeyCols; 
        const count = tableDetail.count;

        if (!vectorColumns || vectorColumns.length === 0) return;

        const row = tbody.insertRow();
        row.style.borderBottom = '1px solid #eee';

        const cell1 = row.insertCell();
        cell1.style.padding = '8px';
        cell1.style.verticalAlign = 'top';
        cell1.textContent = tableName;
        const pkSpan = document.createElement('span');
        pkSpan.textContent = ` (PK: ${primaryKeyCols.join(', ')})`;
        pkSpan.style.fontSize = '0.9em';
        pkSpan.style.color = '#555';
        pkSpan.style.display = 'block';
        cell1.appendChild(pkSpan);

        const cell2 = row.insertCell();
        cell2.style.padding = '8px';
        cell2.style.verticalAlign = 'top';

        vectorColumns.forEach((vCol, index) => {
            const vColName = vCol.name;
            const dimension = vCol.dimension;
            const radioId = `table-${tableName}-vcol-${vColName}`;

            const radio = document.createElement('input');
            radio.type = 'radio';
            radio.id = radioId;
            radio.name = 'table_vector_column_selection';
            radio.value = `${tableName}::${vColName}`;
            radio.dataset.tableName = tableName;
            radio.dataset.vectorColumn = vColName;
            radio.dataset.dimension = dimension;
            radio.dataset.count = count;
            radio.dataset.primaryKeyCols = JSON.stringify(primaryKeyCols);
            radio.dataset.partitionKeyCols = JSON.stringify(partitionKeyCols);
            radio.style.marginRight = '0.3em';

            radio.addEventListener('change', () => {
                selectedTable = tableName;
                selectedVectorColumn = vColName;
                selectedTableDimension = parseInt(dimension, 10);
                selectedTableCount = radio.dataset.count;
                selectedTablePrimaryKeyCols = JSON.parse(radio.dataset.primaryKeyCols || '[]');
                selectedTablePartitionKeyCols = JSON.parse(radio.dataset.partitionKeyCols || '[]');

                updateDocLimitHelpText(selectedTableCount);

                sampleTableButton.disabled = false;
                showStatus(`Table '${tableName}', Vector Column '${vColName}' selected. Ready to fetch sample & keys.`);
                metadataSelectionSection.classList.add('hidden');
                samplingPreviewSection.classList.add('hidden');
                configSection.classList.add('hidden');
                samplingStrategyGroup.classList.remove('hidden');
                generateConfigButton.disabled = true;
                metadataKeysListDiv.innerHTML = '';
                sampleDataContainer.innerHTML = '';
                docLimitInput.value = '';
                tensorNameOverrideInput.value = '';
                firstRowsRadio.checked = true;
                tokenRangeRadio.checked = false;
            });

            const label = document.createElement('label');
            label.htmlFor = radioId;
            label.textContent = ` ${vColName} (Dim: ${dimension})`; 
            label.style.display = 'inline-block'; 
            label.style.marginRight = '1em';

            const div = document.createElement('div');
            div.appendChild(radio);
            div.appendChild(label);
            cell2.appendChild(div);
        });
    });

    tablesListDiv.appendChild(table);
}

// Reset UI state for new connection
function resetUIForNewConnection() {
    collectionsSection.classList.add('hidden');
    tablesSection.classList.add('hidden');
    metadataSelectionSection.classList.add('hidden');
    samplingPreviewSection.classList.add('hidden');
    configSection.classList.add('hidden');
    samplingStrategyGroup.classList.add('hidden');
    document.getElementById('token_range_option').classList.add('hidden');
    document.getElementById('distributed_option').classList.add('hidden');
    sampleButton.disabled = true;
    sampleTableButton.disabled = true;
    generateConfigButton.disabled = true;
    collectionsListDiv.innerHTML = '';
    tablesListDiv.innerHTML = '';
    metadataKeysListDiv.innerHTML = '';
    sampleDataContainer.innerHTML = '';

    selectedCollection = null;
    selectedTable = null;
    selectedVectorColumn = null;
    selectedCollectionDimension = null;
    selectedTableDimension = null;
    selectedCollectionCount = null;
    selectedTableCount = null;
    selectedTablePrimaryKeyCols = [];
    selectedTablePartitionKeyCols = [];
    currentSampleData = null;
    availableMetadataKeys = [];
    tensorNameOverrideInput.value = '';
    docLimitInput.value = '';
    firstRowsRadio.checked = true;
    tokenRangeRadio.checked = false;
    document.getElementById('sampling_distributed').checked = false;
}

// Update document limit help text based on count
function updateDocLimitHelpText(count) {
    const helpTextElement = document.getElementById('doc_limit_help_text');
    if (!helpTextElement) return;

    let baseText = "Specify the maximum number of documents to fetch and save.";
    let countInfo = "";

    if (count !== null && count !== undefined &&
        count !== 'Error' && count !== 'Unknown' && count !== 'N/A') {
        try {
            const numericCount = parseInt(count, 10);
            if (!isNaN(numericCount)) {
                countInfo = ` (Estimated source count: ${numericCount.toLocaleString()})`;
            } else {
                countInfo = ` (Source count: ${count})`;
            }
        } catch (e) {
            countInfo = ` (Source count: ${count})`;
        }
    } else if (count === 'Error') {
        countInfo = " (Could not estimate source count)";
    } else if (count === 'N/A') {
        countInfo = " (Source count not available)";
    }

    helpTextElement.textContent = baseText + countInfo;
}

// Fetch metadata keys from sample data
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
                errorDataText = await response.text();
            }
            throw new Error(`HTTP error ${response.status}: ${errorDataText}`);
        }
        const result = await response.json();

        if (result.keys) {
            availableMetadataKeys = result.keys;
            populateMetadataKeys(
                availableMetadataKeys,
                sampleData,
                isTable,
                isTable ? selectedVectorColumn : '$vector',
                isTable ? selectedTablePrimaryKeyCols : ['_id']
            );
            showStatus('Sample data fetched. Select metadata fields to include.');
            metadataSelectionSection.classList.remove('hidden');
            samplingPreviewSection.classList.add('hidden');
            samplingStrategyGroup.classList.remove('hidden');
            if (isTable) {
                document.getElementById('token_range_option').classList.remove('hidden');
                document.getElementById('distributed_option').classList.add('hidden');
            } else {
                document.getElementById('token_range_option').classList.add('hidden');
                document.getElementById('distributed_option').classList.remove('hidden');
            }
            previewDocsButton.disabled = currentSampleData.length === 0;
            generateConfigButton.disabled = false;
            
            const dbName = currentConnection.db_name || 'db';
            const targetName = isTable ? selectedTable : selectedCollection;
            tensorNameOverrideInput.placeholder = `${dbName}_${targetName}`;
            tensorNameOverrideInput.value = '';

        } else {
            showError('Failed to get metadata keys. Invalid response from server.');
            console.error('Invalid metadata keys response:', result);
        }

    } catch (error) {
        console.error('Error fetching metadata keys:', error);
        showError(`Failed to fetch metadata keys: ${error.message}`);
    }
}

// Display sample documents in readable format
function displaySampleDocs(docs, isTable = false, vectorColName = '$vector') {
    sampleDataContainer.innerHTML = '';
    if (!docs || docs.length === 0) {
        sampleDataContainer.textContent = 'No sample documents to display.';
        samplingPreviewSection.classList.add('hidden');
        return;
    }

    const pre = document.createElement('pre');
    const code = document.createElement('code');

    const formattedDocs = docs.map(doc => {
        const displayDoc = { ...doc };
        const vectorKey = isTable ? vectorColName : '$vector';
        if (displayDoc[vectorKey] && Array.isArray(displayDoc[vectorKey])) {
            displayDoc[vectorKey] = `[${displayDoc[vectorKey][0]}, ${displayDoc[vectorKey][1]}, ..., ${displayDoc[vectorKey][displayDoc[vectorKey].length - 1]}] (Dim: ${displayDoc[vectorKey].length})`;
        } else if (displayDoc[vectorKey]) {
            displayDoc[vectorKey] = '[Vector Data Present]';
        }
        return JSON.stringify(displayDoc, null, 2);
    });

    code.textContent = formattedDocs.join('\n\n---\n\n');
    pre.appendChild(code);
    sampleDataContainer.appendChild(pre);
    samplingPreviewSection.classList.remove('hidden');
    showStatus('Sample data preview displayed below. Adjust metadata selection if needed.');
}

// Preview sample documents
previewDocsButton.addEventListener('click', () => {
    if (currentSampleData && currentSampleData.length > 0) {
        samplingPreviewSection.classList.remove('hidden');
        displaySampleDocs(
            currentSampleData,
            isTableMode,
            isTableMode ? selectedVectorColumn : '$vector'
        );
        showStatus("Displaying fetched sample documents.");
        samplingPreviewSection.scrollIntoView({ behavior: 'smooth' });
    } else {
        showError("No sample data available to preview.");
        samplingPreviewSection.classList.add('hidden');
    }
});

// Populate metadata keys list with checkboxes
function populateMetadataKeys(keys, sampleData, isTable = false, vectorColName = null, primaryKeys = []) {
    metadataKeysListDiv.innerHTML = '';
    availableMetadataKeys = keys;

    const vectorKeyToExclude = isTable ? vectorColName : '$vector';
    const keysForSelection = keys.filter(key => key !== vectorKeyToExclude);

    if (keysForSelection.length === 0) {
        metadataKeysListDiv.innerHTML = '<p>No additional metadata fields found in sample data (excluding vector column).</p>';
        return;
    }

    // Create table element
    const table = document.createElement('table');
    table.style.width = '100%';
    table.style.borderCollapse = 'collapse';
    table.style.marginTop = '10px';

    // Create table header
    const thead = table.createTHead();
    const headerRow = thead.insertRow();
    
    const th1 = document.createElement('th');
    th1.textContent = 'Field';
    th1.style.textAlign = 'left';
    th1.style.borderBottom = '1px solid #ccc';
    th1.style.padding = '8px';
    headerRow.appendChild(th1);

    const th2 = document.createElement('th');
    th2.textContent = 'Preview Value';
    th2.style.textAlign = 'left';
    th2.style.borderBottom = '1px solid #ccc';
    th2.style.padding = '8px';
    headerRow.appendChild(th2);

    const tbody = table.createTBody();

    // Get first document for preview values
    const firstDoc = sampleData && sampleData.length > 0 ? sampleData[0] : null;

    keysForSelection.forEach(key => {
        const row = tbody.insertRow();
        row.style.borderBottom = '1px solid #eee';

        // Create checkbox cell
        const cell1 = row.insertCell();
        cell1.style.padding = '8px';
        cell1.style.verticalAlign = 'top';
        cell1.style.display = 'flex';
        cell1.style.alignItems = 'center';
        cell1.style.gap = '6px';

        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = `meta-${key}`;
        checkbox.name = 'metadata_keys';
        checkbox.value = key;
        checkbox.style.margin = '0';

        let shouldCheckByDefault = false;
        if (firstDoc && firstDoc.hasOwnProperty(key)) {
            const value = firstDoc[key];
            const valueType = typeof value;

            if (valueType === 'string' && value.length < 50) {
                shouldCheckByDefault = true;
            } else if (valueType === 'number') {
                shouldCheckByDefault = true;
            }
        }
        checkbox.checked = shouldCheckByDefault;

        const label = document.createElement('label');
        label.htmlFor = `meta-${key}`;
        label.textContent = key;
        label.style.margin = '0';
        label.style.whiteSpace = 'nowrap';
        label.style.display = 'flex';
        label.style.alignItems = 'center';

        cell1.appendChild(checkbox);
        cell1.appendChild(label);

        // Create preview value cell
        const cell2 = row.insertCell();
        cell2.style.padding = '8px';
        cell2.style.verticalAlign = 'top';
        cell2.style.maxWidth = '300px';
        cell2.style.overflow = 'hidden';
        cell2.style.textOverflow = 'ellipsis';
        cell2.style.whiteSpace = 'nowrap';

        if (firstDoc && firstDoc.hasOwnProperty(key)) {
            const value = firstDoc[key];
            let displayValue = value;
            
            if (Array.isArray(value)) {
                displayValue = `[${value.slice(0, 3).join(', ')}${value.length > 3 ? ', ...' : ''}]`;
            } else if (typeof value === 'object' && value !== null) {
                displayValue = JSON.stringify(value);
                if (displayValue.length > 50) {
                    displayValue = displayValue.substring(0, 47) + '...';
                }
            } else if (typeof value === 'string' && value.length > 50) {
                displayValue = value.substring(0, 47) + '...';
            }
            
            cell2.textContent = displayValue;
        } else {
            cell2.textContent = '(No value)';
        }
    });

    metadataKeysListDiv.appendChild(table);
    generateConfigButton.disabled = false;
}

// Generate configuration and save data
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

    const tensorNameDefault = `${currentConnection.db_name || 'db'}_${isTableMode ? selectedTable : selectedCollection}`;
    const tensorName = tensorNameOverrideInput.value.trim() || tensorNameDefault;
    const safeTensorName = tensorName.replace(/\s+/g, '_');

    showStatus(`Generating config and fetching data for tensor '${safeTensorName}'... (Limit: ${docLimit || 'All'})`);
    configSection.classList.add('hidden');

    const payload = {
        connection: currentConnection,
        tensor_name: safeTensorName,
        metadata_keys: selectedMetadataKeys,
        document_limit: docLimit
    };

    if (isTableMode) {
        payload.table_name = selectedTable;
        payload.vector_column = selectedVectorColumn;
        payload.vector_dimension = selectedTableDimension;
        payload.primary_key_columns = selectedTablePrimaryKeyCols;
        delete payload.collection_name;
    } else {
         payload.collection_name = selectedCollection;
        payload.vector_dimension = selectedCollectionDimension;
        delete payload.table_name;
        delete payload.vector_column;
        delete payload.primary_key_columns;
    }

    const apiUrl = '/api/astra/save_data';

    try {
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
                 if (errorJson.detail) {
                      if (Array.isArray(errorJson.detail)) {
                          errorDataText = errorJson.detail.map(e => `${e.loc ? e.loc.join('.')+': ' : ''}${e.msg}`).join(', ');
                      } else {
                           errorDataText = errorJson.detail;
                      }
                 } else if (errorJson.error) {
                      errorDataText = errorJson.error;
                 } else {
                      errorDataText = JSON.stringify(errorJson);
                 }

            } catch (e) {
                errorDataText = await response.text();
            }
            console.error("Save data error response:", errorJson || errorDataText);
            throw new Error(errorDataText);
        }

        const result = await response.json();

        showStatus(result.message || 'Configuration generated successfully.');
        
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

        configSection.classList.remove('hidden');

        const existingLink = configSection.querySelector('a.projector-link');
        if (existingLink) {
            existingLink.remove();
        }
        const projectorLink = document.createElement('a');
        projectorLink.href = `/?config=${encodeURIComponent(result.config_file)}`;
        projectorLink.textContent = "Go to Embedding Projector";
        projectorLink.className = 'projector-link';
        projectorLink.target = "_blank";
        projectorLink.style.display = "block";
        projectorLink.style.marginTop = "1em";
        projectorLink.style.fontWeight = "bold";
        configSection.appendChild(projectorLink);

    } catch (error) {
        console.error('Error saving data:', error);
        showError(`Failed to save data: ${error.message}`);
    }
});

// Display configuration results
function displayConfigResults(result) {
    const detailsContainer = document.getElementById('config-details-json');
    const configSection = document.getElementById('config-section');

    detailsContainer.textContent = '';
    const existingLink = configSection.querySelector('a.projector-link');
    if (existingLink) {
        existingLink.remove();
    }

    if (!result) {
        detailsContainer.textContent = 'No results received from server.';
        return;
    }

    const tensorEntry = {
        tensorName: result.tensor_name,
        tensorShape: result.tensor_shape,
        tensorPath: result.tensor_path_rel,
        metadataPath: result.metadata_path_rel
    };
    detailsContainer.textContent = JSON.stringify(tensorEntry, null, 2);

    if (result.config_file) {
        const projectorLink = document.createElement('a');
        const params = new URLSearchParams();
        params.set('config', result.config_file);
        projectorLink.href = `/?${params.toString()}`;
        projectorLink.textContent = 'View in Embedding Projector';
        projectorLink.target = '_blank';
        projectorLink.style.display = 'block';
        projectorLink.style.marginTop = '1em';
        projectorLink.style.fontWeight = 'bold';
        detailsContainer.parentNode.insertBefore(projectorLink, detailsContainer.nextSibling); 
    }
}

// Fetch sample data from selected table
sampleTableButton.addEventListener('click', async () => {
    if (!currentConnection || !selectedTable || !selectedVectorColumn) {
         showError('Connection, table, or vector column not selected.');
         return;
    }
    isTableMode = true;
    showStatus(`Fetching sample data for table '${selectedTable}'...`);
    metadataSelectionSection.classList.add('hidden');
    samplingPreviewSection.classList.add('hidden');
    configSection.classList.add('hidden');
    generateConfigButton.disabled = true;
    previewDocsButton.disabled = true;
    sampleDataContainer.innerHTML = '';
    metadataKeysListDiv.innerHTML = '';

    try {
        const payload = {
            connection: currentConnection,
            table_name: selectedTable,
            vector_column: selectedVectorColumn
        };

        const response = await fetch('/api/astra/sample_table', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
             let errorDetail = `HTTP error ${response.status}`;
             let errorJson = null;
             try {
                 errorJson = await response.json();
                 errorDetail += `: ${errorJson.error || JSON.stringify(errorJson)}`;
             } catch (e) {
                 try { errorDetail += `: ${await response.text()}`; } catch (e2) { }
             }
             throw new Error(errorDetail);
         }

        const result = await response.json();
        currentSampleData = result.sample_data || [];
         if (currentSampleData.length === 0) {
             showError(`No sample documents found in table '${selectedTable}'. Cannot proceed.`);
             return;
         }
        showStatus(`Fetched ${currentSampleData.length} sample rows. Analyzing metadata keys...`);

        await getMetadataKeys(currentSampleData, true);

    } catch (error) {
        showError(`Failed to fetch table sample data: ${error.message}`);
    }
});

// Clear stored credentials from session storage
function clearStoredCredentials() {
    sessionStorage.removeItem('astraEndpointUrl');
    sessionStorage.removeItem('astraToken');
    sessionStorage.removeItem('astraDbName');
    sessionStorage.removeItem('astraKeyspace');
}