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

let currentConnection = null;
let selectedCollection = null;
let selectedCollectionDimension = null;
let selectedCollectionCount = null;
let currentSampleData = null;
let availableMetadataKeys = [];

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

// Handle the connection form submission
// This manages the connection to Astra DB and fetches available collections
connectionForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    showStatus('Connecting and fetching collections...');
    collectionsSection.classList.add('hidden');
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

        // Heuristic for default metadata key selection:
        // 1. Always include '_id'
        // 2. Include keys that are likely useful metadata:
        //    - Strings shorter than 50 characters (likely identifiers, categories)
        //    - Numbers (likely counts, scores)
        let shouldCheck = false;
        if (keyName === '_id') {
            shouldCheck = true;
        } else if (firstDoc && firstDoc.hasOwnProperty(keyName)) {
            const value = firstDoc[keyName];
            const valueType = typeof value;

            if (valueType === 'string' && value.length < 50) {
                console.log(`Checking key '${keyName}': String length ${value.length} < 50`);
                shouldCheck = true;
            } else if (valueType === 'number') {
                console.log(`Checking key '${keyName}': Is a number`);
                shouldCheck = true;
            }
        }
        checkbox.checked = shouldCheck;

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
        console.log("Sending save request to /api/astra/save_data:", requestBody);
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

        // Add a link to the embedding projector
        const projectorLink = document.createElement('a');
        projectorLink.href = `/?config=${encodeURIComponent(result.config_file)}`;
        projectorLink.textContent = "Go to Embedding Projection";
        projectorLink.style.display = "block";
        projectorLink.style.marginTop = "1em";
        resultsDiv.appendChild(projectorLink);

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