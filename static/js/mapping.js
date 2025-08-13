// Mapping Interface JavaScript
let currentMapping = {
    oracle_connection_id: null,
    elasticsearch_connection_id: null,
    oracle_query: '',
    elasticsearch_index: '',
    field_mappings: [],
    transformation_rules: []
};

let oracleFields = [];
let elasticsearchFields = [];
let existingMappingId = null;

// Initialize the mapping interface
async function initializeMappingInterface() {
    try {
        await loadConnections();
        await loadExistingMappings();
        
        // Check for pre-populated data from session storage
        const oracleConnectionId = sessionStorage.getItem('oracleConnectionId');
        const oracleQuery = sessionStorage.getItem('oracleQuery');
        const esConnectionId = sessionStorage.getItem('elasticsearchConnectionId');
        const esIndex = sessionStorage.getItem('elasticsearchIndex');
        
        if (oracleConnectionId) {
            document.getElementById('oracleConnectionSelect').value = oracleConnectionId;
            currentMapping.oracle_connection_id = parseInt(oracleConnectionId);
        }
        
        if (oracleQuery) {
            document.getElementById('oracleQuery').value = oracleQuery;
            currentMapping.oracle_query = oracleQuery;
        }
        
        if (esConnectionId) {
            document.getElementById('esConnectionSelect').value = esConnectionId;
            currentMapping.elasticsearch_connection_id = parseInt(esConnectionId);
            await loadESIndices();
            
            if (esIndex) {
                document.getElementById('esIndexSelect').value = esIndex;
                currentMapping.elasticsearch_index = esIndex;
                await loadESFields();
            }
        }
        
        // Clear session storage
        sessionStorage.removeItem('oracleConnectionId');
        sessionStorage.removeItem('oracleQuery');
        sessionStorage.removeItem('elasticsearchConnectionId');
        sessionStorage.removeItem('elasticsearchIndex');
        
        initializeDragAndDrop();
    } catch (error) {
        console.error('Error initializing mapping interface:', error);
    }
}

// Load Oracle and Elasticsearch connections
async function loadConnections() {
    try {
        const [oracleResponse, esResponse] = await Promise.all([
            axios.get('/api/oracle/connections'),
            axios.get('/api/elasticsearch/connections')
        ]);
        
        const oracleSelect = document.getElementById('oracleConnectionSelect');
        const esSelect = document.getElementById('esConnectionSelect');
        
        // Populate Oracle connections
        oracleSelect.innerHTML = '<option value="">Select Oracle connection...</option>';
        oracleResponse.data.forEach(conn => {
            oracleSelect.innerHTML += `<option value="${conn.id}">${conn.name}</option>`;
        });
        
        // Populate Elasticsearch connections
        esSelect.innerHTML = '<option value="">Select ES connection...</option>';
        esResponse.data.forEach(conn => {
            esSelect.innerHTML += `<option value="${conn.id}">${conn.name} (${conn.environment})</option>`;
        });
    } catch (error) {
        console.error('Error loading connections:', error);
    }
}

// Load existing mapping configurations
async function loadExistingMappings() {
    try {
        const response = await axios.get('/api/mapping/configurations');
        const select = document.getElementById('existingMappingSelect');
        
        select.innerHTML = '<option value="">Create new mapping...</option>';
        response.data.forEach(mapping => {
            select.innerHTML += `<option value="${mapping.id}">${mapping.name}</option>`;
        });
    } catch (error) {
        console.error('Error loading existing mappings:', error);
    }
}

// Load Oracle query and update mapping
function loadOracleQuery() {
    const connectionId = document.getElementById('oracleConnectionSelect').value;
    currentMapping.oracle_connection_id = connectionId ? parseInt(connectionId) : null;
}

// Load Elasticsearch indices for selected connection
async function loadESIndices() {
    const connectionId = document.getElementById('esConnectionSelect').value;
    currentMapping.elasticsearch_connection_id = connectionId ? parseInt(connectionId) : null;
    
    const indexSelect = document.getElementById('esIndexSelect');
    indexSelect.innerHTML = '<option value="">Select ES index...</option>';
    
    if (!connectionId) return;
    
    try {
        const response = await axios.get(`/api/elasticsearch/connections/${connectionId}/indices`);
        response.data.forEach(index => {
            indexSelect.innerHTML += `<option value="${index.index_name}">${index.index_name}</option>`;
        });
    } catch (error) {
        console.error('Error loading ES indices:', error);
    }
}

// Load Elasticsearch fields for selected index
async function loadESFields() {
    const indexName = document.getElementById('esIndexSelect').value;
    currentMapping.elasticsearch_index = indexName;
    
    if (!indexName || !currentMapping.elasticsearch_connection_id) return;
    
    try {
        const response = await axios.get(`/api/elasticsearch/connections/${currentMapping.elasticsearch_connection_id}/indices/${indexName}/fields`);
        elasticsearchFields = response.data;
        renderESFields();
    } catch (error) {
        console.error('Error loading ES fields:', error);
        document.getElementById('es-fields').innerHTML = 
            '<div class="text-danger text-center p-4">Error loading ES fields</div>';
    }
}

// Analyze Oracle query and extract fields
async function analyzeOracleQuery() {
    const query = document.getElementById('oracleQuery').value.trim();
    currentMapping.oracle_query = query;
    
    if (!query || !currentMapping.oracle_connection_id) {
        alert('Please select a connection and enter a query');
        return;
    }
    
    try {
        const response = await axios.post(`/api/oracle/connections/${currentMapping.oracle_connection_id}/query/analyze`, {
            query: query
        });
        
        oracleFields = response.data.columns;
        renderOracleFields();
        
        // Show success message
        showNotification('Query analyzed successfully!', 'success');
    } catch (error) {
        console.error('Error analyzing Oracle query:', error);
        showNotification('Error analyzing query: ' + (error.response?.data?.error || 'Unknown error'), 'error');
    }
}

// Generate automatic mapping suggestions
async function generateAutoMapping() {
    if (!currentMapping.oracle_connection_id || !currentMapping.elasticsearch_connection_id || 
        !currentMapping.oracle_query || !currentMapping.elasticsearch_index) {
        alert('Please complete all connection and query configurations first');
        return;
    }
    
    try {
        const response = await axios.post('/api/mapping/auto-suggest', {
            oracle_connection_id: currentMapping.oracle_connection_id,
            elasticsearch_connection_id: currentMapping.elasticsearch_connection_id,
            oracle_query: currentMapping.oracle_query,
            elasticsearch_index: currentMapping.elasticsearch_index
        });
        
        const suggestions = response.data;
        
        // Apply suggested mappings
        currentMapping.field_mappings = suggestions.suggested_mappings.map(suggestion => ({
            oracle_field: suggestion.oracle_field,
            oracle_type: suggestion.oracle_type,
            es_field: suggestion.suggested_es_field,
            es_type: suggestion.suggested_es_type,
            confidence: suggestion.confidence,
            mapping_type: suggestion.mapping_type
        }));
        
        currentMapping.transformation_rules = suggestions.transformation_rules;
        
        // Update UI
        renderActiveMappings();
        renderTransformationRules();
        
        showNotification('Auto-mapping generated successfully!', 'success');
    } catch (error) {
        console.error('Error generating auto mapping:', error);
        showNotification('Error generating auto mapping: ' + (error.response?.data?.error || 'Unknown error'), 'error');
    }
}

// Render Oracle fields
function renderOracleFields() {
    const container = document.getElementById('oracle-fields');
    
    if (oracleFields.length === 0) {
        container.innerHTML = '<div class="text-muted text-center p-4">No fields found</div>';
        return;
    }
    
    let html = '';
    oracleFields.forEach((field, index) => {
        html += `
            <div class="field-item oracle-field" draggable="true" data-field-index="${index}">
                <div class="field-header">
                    <span class="field-name">${field.field}</span>
                    <span class="badge bg-primary">${field.oracle_type}</span>
                </div>
                <div class="field-details">
                    <small class="text-muted">ES Type: ${field.elasticsearch_type}</small>
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
    initializeDragAndDrop();
}

// Render Elasticsearch fields
function renderESFields() {
    const container = document.getElementById('es-fields');
    
    if (elasticsearchFields.length === 0) {
        container.innerHTML = '<div class="text-muted text-center p-4">No fields found</div>';
        return;
    }
    
    let html = '';
    elasticsearchFields.forEach((field, index) => {
        html += `
            <div class="field-item es-field" data-field-index="${index}">
                <div class="field-header">
                    <span class="field-name">${field.field_name}</span>
                    <span class="badge bg-info">${field.type}</span>
                </div>
                <div class="field-details">
                    ${field.format ? `<small class="text-muted">Format: ${field.format}</small>` : ''}
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
    initializeDropZones();
}

// Initialize drag and drop functionality
function initializeDragAndDrop() {
    const oracleFields = document.querySelectorAll('.oracle-field');
    
    oracleFields.forEach(field => {
        field.addEventListener('dragstart', function(e) {
            e.dataTransfer.setData('text/plain', this.dataset.fieldIndex);
            this.classList.add('dragging');
        });
        
        field.addEventListener('dragend', function(e) {
            this.classList.remove('dragging');
        });
    });
}

// Initialize drop zones
function initializeDropZones() {
    const esFields = document.querySelectorAll('.es-field');
    
    esFields.forEach(field => {
        field.addEventListener('dragover', function(e) {
            e.preventDefault();
            this.classList.add('drag-over');
        });
        
        field.addEventListener('dragleave', function(e) {
            this.classList.remove('drag-over');
        });
        
        field.addEventListener('drop', function(e) {
            e.preventDefault();
            this.classList.remove('drag-over');
            
            const oracleFieldIndex = e.dataTransfer.getData('text/plain');
            const esFieldIndex = this.dataset.fieldIndex;
            
            createFieldMapping(parseInt(oracleFieldIndex), parseInt(esFieldIndex));
        });
    });
}

// Create field mapping
function createFieldMapping(oracleFieldIndex, esFieldIndex) {
    const oracleField = oracleFields[oracleFieldIndex];
    const esField = elasticsearchFields[esFieldIndex];
    
    if (!oracleField || !esField) return;
    
    // Check if mapping already exists
    const existingIndex = currentMapping.field_mappings.findIndex(
        mapping => mapping.oracle_field === oracleField.field
    );
    
    const newMapping = {
        oracle_field: oracleField.field,
        oracle_type: oracleField.oracle_type,
        es_field: esField.field_name,
        es_type: esField.type,
        confidence: 100,
        mapping_type: 'manual'
    };
    
    if (existingIndex >= 0) {
        currentMapping.field_mappings[existingIndex] = newMapping;
    } else {
        currentMapping.field_mappings.push(newMapping);
    }
    
    renderActiveMappings();
    showNotification('Field mapping created!', 'success');
}

// Render active mappings
function renderActiveMappings() {
    const container = document.getElementById('active-mappings');
    
    if (currentMapping.field_mappings.length === 0) {
        container.innerHTML = '<div class="text-muted text-center p-4">No field mappings configured yet</div>';
        return;
    }
    
    let html = '<div class="table-responsive"><table class="table table-striped">';
    html += `
        <thead>
            <tr>
                <th>Oracle Field</th>
                <th>Oracle Type</th>
                <th>ES Field</th>
                <th>ES Type</th>
                <th>Confidence</th>
                <th>Actions</th>
            </tr>
        </thead>
        <tbody>
    `;
    
    currentMapping.field_mappings.forEach((mapping, index) => {
        const confidenceBadge = getConfidenceBadge(mapping.confidence);
        html += `
            <tr>
                <td><strong>${mapping.oracle_field}</strong></td>
                <td><span class="badge bg-primary">${mapping.oracle_type}</span></td>
                <td><strong>${mapping.es_field}</strong></td>
                <td><span class="badge bg-info">${mapping.es_type}</span></td>
                <td>${confidenceBadge}</td>
                <td>
                    <button class="btn btn-sm btn-outline-danger" onclick="removeMapping(${index})">
                        <i class="fas fa-trash"></i>
                    </button>
                </td>
            </tr>
        `;
    });
    
    html += '</tbody></table></div>';
    container.innerHTML = html;
}

// Render transformation rules
function renderTransformationRules() {
    const container = document.getElementById('transformation-rules');
    
    if (currentMapping.transformation_rules.length === 0) {
        container.innerHTML = '<div class="text-muted text-center p-4">No transformation rules configured</div>';
        return;
    }
    
    let html = '<div class="list-group">';
    currentMapping.transformation_rules.forEach((rule, index) => {
        html += `
            <div class="list-group-item">
                <div class="d-flex justify-content-between align-items-start">
                    <div>
                        <h6 class="mb-1">${rule.target}</h6>
                        <p class="mb-1">${rule.description}</p>
                        <small class="text-muted">Rule: ${rule.rule}</small>
                    </div>
                    <button class="btn btn-sm btn-outline-danger" onclick="removeTransformationRule(${index})">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </div>
        `;
    });
    html += '</div>';
    
    container.innerHTML = html;
}

// Get confidence badge
function getConfidenceBadge(confidence) {
    if (confidence >= 90) {
        return `<span class="badge bg-success">${confidence}%</span>`;
    } else if (confidence >= 70) {
        return `<span class="badge bg-warning">${confidence}%</span>`;
    } else {
        return `<span class="badge bg-danger">${confidence}%</span>`;
    }
}

// Remove field mapping
function removeMapping(index) {
    currentMapping.field_mappings.splice(index, 1);
    renderActiveMappings();
    showNotification('Mapping removed', 'warning');
}

// Remove transformation rule
function removeTransformationRule(index) {
    currentMapping.transformation_rules.splice(index, 1);
    renderTransformationRules();
    showNotification('Transformation rule removed', 'warning');
}

// Validate mappings
async function validateMappings() {
    if (currentMapping.field_mappings.length === 0) {
        alert('No field mappings to validate');
        return;
    }
    
    try {
        const response = await axios.post('/api/mapping/validate', {
            oracle_connection_id: currentMapping.oracle_connection_id,
            elasticsearch_connection_id: currentMapping.elasticsearch_connection_id,
            field_mappings: currentMapping.field_mappings
        });
        
        const validation = response.data;
        
        let message = 'Validation Results:\n\n';
        
        if (validation.valid) {
            message += '✅ All mappings are valid!\n';
        } else {
            message += '❌ Validation failed:\n';
            validation.errors.forEach(error => {
                message += `• ${error}\n`;
            });
        }
        
        if (validation.warnings.length > 0) {
            message += '\n⚠️ Warnings:\n';
            validation.warnings.forEach(warning => {
                message += `• ${warning}\n`;
            });
        }
        
        alert(message);
    } catch (error) {
        console.error('Error validating mappings:', error);
        alert('Error validating mappings: ' + (error.response?.data?.error || 'Unknown error'));
    }
}

// Save mapping configuration
function saveMappingConfiguration() {
    if (!currentMapping.oracle_connection_id || !currentMapping.elasticsearch_connection_id ||
        !currentMapping.oracle_query || !currentMapping.elasticsearch_index) {
        alert('Please complete all configuration fields');
        return;
    }
    
    if (currentMapping.field_mappings.length === 0) {
        alert('Please create at least one field mapping');
        return;
    }
    
    const modal = new bootstrap.Modal(document.getElementById('saveMappingModal'));
    modal.show();
}

// Confirm save mapping
async function confirmSaveMapping() {
    const name = document.getElementById('mappingName').value.trim();
    
    if (!name) {
        alert('Please enter a mapping name');
        return;
    }
    
    const mappingData = {
        name: name,
        oracle_connection_id: currentMapping.oracle_connection_id,
        elasticsearch_connection_id: currentMapping.elasticsearch_connection_id,
        oracle_query: currentMapping.oracle_query,
        elasticsearch_index: currentMapping.elasticsearch_index,
        field_mappings: currentMapping.field_mappings,
        transformation_rules: currentMapping.transformation_rules
    };
    
    try {
        if (existingMappingId) {
            await axios.put(`/api/mapping/configurations/${existingMappingId}`, mappingData);
        } else {
            await axios.post('/api/mapping/configurations', mappingData);
        }
        
        bootstrap.Modal.getInstance(document.getElementById('saveMappingModal')).hide();
        document.getElementById('saveMappingForm').reset();
        
        showNotification('Mapping configuration saved successfully!', 'success');
        
        // Reload existing mappings
        await loadExistingMappings();
    } catch (error) {
        console.error('Error saving mapping:', error);
        alert('Error saving mapping: ' + (error.response?.data?.error || 'Unknown error'));
    }
}

// Load existing mapping configuration
async function loadExistingMapping() {
    const mappingId = document.getElementById('existingMappingSelect').value;
    
    if (!mappingId) {
        // Reset to new mapping
        existingMappingId = null;
        return;
    }
    
    existingMappingId = parseInt(mappingId);
    
    try {
        const response = await axios.get(`/api/mapping/configurations/${mappingId}`);
        const mapping = response.data;
        
        // Load configuration
        currentMapping = {
            oracle_connection_id: mapping.oracle_connection_id,
            elasticsearch_connection_id: mapping.elasticsearch_connection_id,
            oracle_query: mapping.oracle_query,
            elasticsearch_index: mapping.elasticsearch_index,
            field_mappings: mapping.field_mappings,
            transformation_rules: mapping.transformation_rules
        };
        
        // Update UI
        document.getElementById('oracleConnectionSelect').value = mapping.oracle_connection_id;
        document.getElementById('esConnectionSelect').value = mapping.elasticsearch_connection_id;
        document.getElementById('oracleQuery').value = mapping.oracle_query;
        
        await loadESIndices();
        document.getElementById('esIndexSelect').value = mapping.elasticsearch_index;
        await loadESFields();
        
        // Analyze query to get Oracle fields
        await analyzeOracleQuery();
        
        // Render mappings
        renderActiveMappings();
        renderTransformationRules();
        
        showNotification('Mapping configuration loaded successfully!', 'success');
    } catch (error) {
        console.error('Error loading mapping:', error);
        showNotification('Error loading mapping: ' + (error.response?.data?.error || 'Unknown error'), 'error');
    }
}

// Preview migration data
async function previewMigration() {
    if (!existingMappingId && (!currentMapping.oracle_connection_id || !currentMapping.elasticsearch_connection_id ||
        !currentMapping.oracle_query || !currentMapping.elasticsearch_index)) {
        alert('Please save the mapping configuration first');
        return;
    }
    
    let configId = existingMappingId;
    
    // If no existing mapping, save current configuration temporarily
    if (!configId) {
        try {
            const tempMapping = {
                name: 'Temporary Preview Mapping',
                oracle_connection_id: currentMapping.oracle_connection_id,
                elasticsearch_connection_id: currentMapping.elasticsearch_connection_id,
                oracle_query: currentMapping.oracle_query,
                elasticsearch_index: currentMapping.elasticsearch_index,
                field_mappings: currentMapping.field_mappings,
                transformation_rules: currentMapping.transformation_rules
            };
            
            const response = await axios.post('/api/mapping/configurations', tempMapping);
            configId = response.data.id;
        } catch (error) {
            console.error('Error creating temporary mapping:', error);
            alert('Error creating preview mapping');
            return;
        }
    }
    
    try {
        const response = await axios.post('/api/migration/preview', {
            mapping_configuration_id: configId,
            limit: 5
        });
        
        const preview = response.data;
        renderPreview(preview);
        
        const modal = new bootstrap.Modal(document.getElementById('previewModal'));
        modal.show();
    } catch (error) {
        console.error('Error previewing migration:', error);
        alert('Error previewing migration: ' + (error.response?.data?.error || 'Unknown error'));
    }
}

// Render preview data
function renderPreview(preview) {
    const container = document.getElementById('preview-content');
    
    let html = `
        <div class="row">
            <div class="col-md-6">
                <h6>Original Oracle Data</h6>
                <div class="table-responsive">
                    <table class="table table-sm table-striped">
                        <thead>
    `;
    
    if (preview.original_data.length > 0) {
        const columns = Object.keys(preview.original_data[0]);
        html += '<tr>';
        columns.forEach(col => {
            html += `<th>${col}</th>`;
        });
        html += '</tr></thead><tbody>';
        
        preview.original_data.forEach(row => {
            html += '<tr>';
            columns.forEach(col => {
                html += `<td>${row[col] || ''}</td>`;
            });
            html += '</tr>';
        });
    }
    
    html += `
                        </tbody>
                    </table>
                </div>
            </div>
            <div class="col-md-6">
                <h6>Transformed Elasticsearch Data</h6>
                <div class="table-responsive">
                    <pre class="bg-dark p-3 rounded" style="max-height: 400px; overflow-y: auto;">
    `;
    
    preview.transformed_data.forEach(doc => {
        html += JSON.stringify(doc, null, 2) + '\n\n';
    });
    
    html += `
                    </pre>
                </div>
            </div>
        </div>
    `;
    
    container.innerHTML = html;
}

// Proceed to migration
function proceedToMigration() {
    bootstrap.Modal.getInstance(document.getElementById('previewModal')).hide();
    window.location.href = '/migration-status';
}

// Show notification
function showNotification(message, type) {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `alert alert-${type === 'error' ? 'danger' : type} alert-dismissible fade show`;
    notification.style.position = 'fixed';
    notification.style.top = '20px';
    notification.style.right = '20px';
    notification.style.zIndex = '9999';
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    document.body.appendChild(notification);
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
        if (notification.parentNode) {
            notification.parentNode.removeChild(notification);
        }
    }, 5000);
}
