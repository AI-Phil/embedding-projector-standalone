const connectionForm = document.getElementById('connection-form');
const collectionsSection = document.getElementById('collections-section');
const collectionsListDiv = document.getElementById('collections-list');
const sampleButton = document.getElementById('sample-button');
const samplingSection = document.getElementById('sampling-section');
const sampleDataPre = document.querySelector('#sample-data pre code');
const generateConfigButton = document.getElementById('generate-config-button');
const configSection = document.getElementById('config-section');
const configJsonTextarea = document.getElementById('config-json');
const dataJsonTextarea = document.getElementById('data-json');
const resultsDiv = document.getElementById('results');
const errorDiv = document.getElementById('error');

let currentConnection = null;
let selectedCollection = null;
let currentSampleData = null;

function showError(message) {
    errorDiv.textContent = message;
    errorDiv.classList.remove('hidden');
    resultsDiv.textContent = 'Error occurred.';
}

function showStatus(message) {
    resultsDiv.textContent = message;
    errorDiv.classList.add('hidden');
}

// --- 1. Handle Connection --- 
connectionForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    showStatus('Connecting and fetching collections...');
    collectionsSection.classList.add('hidden');
    samplingSection.classList.add('hidden');
    configSection.classList.add('hidden');
    sampleButton.disabled = true;
    generateConfigButton.disabled = true;
    collectionsListDiv.innerHTML = '';

    const formData = new FormData(connectionForm);
    const data = Object.fromEntries(formData.entries());
    currentConnection = data; // Store connection details (consider security)

    try {
        // Call the backend API
        const response = await fetch('/api/astra/collections', {
            method: 'POST', // Send credentials via POST
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data) // Send credentials
        });

        if (!response.ok) {
            const errorData = await response.text();
            throw new Error(`HTTP error ${response.status}: ${errorData}`);
        }
        const result = await response.json();
        
        if (result.collections && result.collections.length > 0) {
            populateCollections(result.collections);
            showStatus('Connected. Select a collection.');
            collectionsSection.classList.remove('hidden');
        } else if (result.collections) { // Empty list returned
             showError('No collections found for this database/keyspace.');
        } else { // Unexpected response
             showError('Failed to fetch collections. Invalid response from server.');
             console.error('Invalid response format:', result);
        }

    } catch (error) {
        console.error('Error connecting/fetching collections:', error);
        showError(`Failed to connect or fetch collections: ${error.message}`);
    }
});

// --- 2. Populate and Handle Collection Selection ---
function populateCollections(collections) {
    collectionsListDiv.innerHTML = ''; // Clear previous
    collections.forEach(colName => {
        const radio = document.createElement('input');
        radio.type = 'radio';
        radio.id = `col-${colName}`;
        radio.name = 'collection';
        radio.value = colName;
        radio.addEventListener('change', () => {
            selectedCollection = colName;
            sampleButton.disabled = false;
            showStatus(`Collection '${colName}' selected. Ready to sample.`);
            samplingSection.classList.add('hidden'); // Hide sample if changing collection
            configSection.classList.add('hidden');
            generateConfigButton.disabled = true;
        });

        const label = document.createElement('label');
        label.htmlFor = `col-${colName}`;
        label.textContent = colName;
        label.style.display = 'inline-block';
        label.style.marginLeft = '0.5em';
        label.style.marginRight = '1.5em';

        const div = document.createElement('div');
        div.appendChild(radio);
        div.appendChild(label);
        collectionsListDiv.appendChild(div);
    });
}

// --- 3. Handle Sampling --- 
sampleButton.addEventListener('click', async () => {
    if (!selectedCollection || !currentConnection) {
        showError('Connection details or collection not selected.');
        return;
    }
    showStatus(`Sampling data from '${selectedCollection}'...`);
    samplingSection.classList.add('hidden');
    configSection.classList.add('hidden');
    generateConfigButton.disabled = true;

    try {
        const response = await fetch('/api/astra/sample', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            // Pass connection details and collection name to backend
            body: JSON.stringify({ 
                connection: currentConnection, 
                collection_name: selectedCollection 
            })
        });
        if (!response.ok) {
            const errorData = await response.text();
            throw new Error(`HTTP error ${response.status}: ${errorData}`);
        }
        const result = await response.json();

        if (result.sample_data) {
            currentSampleData = result.sample_data;
            sampleDataPre.textContent = JSON.stringify(result.sample_data, null, 2);
            samplingSection.classList.remove('hidden');
            generateConfigButton.disabled = false;
            showStatus(`Sample data fetched for '${selectedCollection}'. Ready to generate config.`);
        } else {
            showError('Failed to fetch sample data. Invalid response from server.');
            console.error('Invalid response format for sample data:', result);
        }

    } catch (error) {
        console.error('Error sampling data:', error);
        showError(`Failed to sample data: ${error.message}`);
    }
});

// --- 4. Handle Config Generation --- 
generateConfigButton.addEventListener('click', () => {
    if (!currentSampleData || !selectedCollection) {
        showError('Sample data not available.');
        return;
    }
    showStatus('Generating configuration files...');
    
    try {
        // Generate data.json (just the sample)
        const dataJsonContent = JSON.stringify(currentSampleData, null, 2);
        dataJsonTextarea.value = dataJsonContent;

        // --- Attempt to detect vector dimension --- 
        let vectorDim = 1; // Default
        if (currentSampleData.length > 0) {
            // Look for a field named '$vector' or 'vector' in the first document
            const firstDoc = currentSampleData[0];
            let vectorField = null;
            if (Array.isArray(firstDoc?.$vector)) {
                vectorField = firstDoc.$vector;
            } else if (Array.isArray(firstDoc?.vector)) {
                vectorField = firstDoc.vector;
            }
            
            if (vectorField) {
                vectorDim = vectorField.length;
            } else {
                console.warn("Could not automatically detect vector field ('$vector' or 'vector') in the first sample document. Using dimension 1.");
            }
        } else {
             console.warn("Sample data is empty. Using dimension 1.");
        }
        // ----------------------------------------- 

        // Generate basic config.json
        const configJsonContent = JSON.stringify({
            tensorData: [
                {
                    name: `${currentConnection.db_name} - ${selectedCollection}`,
                    tensorShape: [currentSampleData.length, vectorDim ], // Use detected or default dim
                    tensorPath: "data.json", // Link to the data file
                    metadataPath: "metadata.tsv" // Standard projector metadata file (optional)
                }
            ]
        }, null, 2);
        configJsonTextarea.value = configJsonContent;

        configSection.classList.remove('hidden');
        showStatus('Configuration generated. Please copy the content into files.');
    } catch (error) {
         console.error('Error generating config:', error);
         showError(`Failed to generate config: ${error.message}`);
    }
}); 