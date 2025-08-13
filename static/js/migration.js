// Migration Status JavaScript
let migrationJobs = [];
let selectedJobId = null;
let autoRefreshInterval = null;

// Initialize migration status interface
function initializeMigrationStatus() {
    loadJobs();
    loadMappingConfigurations();
    
    // Setup auto-refresh
    const autoRefreshCheckbox = document.getElementById('autoRefresh');
    if (autoRefreshCheckbox.checked) {
        startAutoRefresh();
    }
    
    autoRefreshCheckbox.addEventListener('change', function() {
        if (this.checked) {
            startAutoRefresh();
        } else {
            stopAutoRefresh();
        }
    });
}

// Start auto-refresh
function startAutoRefresh() {
    autoRefreshInterval = setInterval(() => {
        loadJobs();
    }, 30000); // 30 seconds
}

// Stop auto-refresh
function stopAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
        autoRefreshInterval = null;
    }
}

// Load migration jobs
async function loadJobs() {
    try {
        const response = await axios.get('/api/migration/jobs');
        migrationJobs = response.data;
        
        renderJobs();
        updateJobCounts();
    } catch (error) {
        console.error('Error loading jobs:', error);
        document.getElementById('jobs-list').innerHTML = 
            '<div class="text-danger text-center p-4">Error loading migration jobs</div>';
    }
}

// Update job counts in overview cards
function updateJobCounts() {
    const counts = {
        running: 0,
        pending: 0,
        completed: 0,
        failed: 0
    };
    
    migrationJobs.forEach(job => {
        if (counts.hasOwnProperty(job.status)) {
            counts[job.status]++;
        }
    });
    
    document.getElementById('running-jobs-count').textContent = counts.running;
    document.getElementById('pending-jobs-count').textContent = counts.pending;
    document.getElementById('completed-jobs-count').textContent = counts.completed;
    document.getElementById('failed-jobs-count').textContent = counts.failed;
}

// Render jobs list
function renderJobs() {
    const container = document.getElementById('jobs-list');
    
    if (migrationJobs.length === 0) {
        container.innerHTML = '<div class="text-muted text-center p-4">No migration jobs found</div>';
        return;
    }
    
    let html = '<div class="table-responsive">';
    html += `
        <table class="table table-hover">
            <thead>
                <tr>
                    <th>Job ID</th>
                    <th>Mapping Configuration</th>
                    <th>Status</th>
                    <th>Progress</th>
                    <th>Records</th>
                    <th>Duration</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    migrationJobs.forEach(job => {
        const statusBadge = getStatusBadge(job.status);
        const progressBar = getProgressBar(job);
        const duration = getDuration(job);
        const actions = getJobActions(job);
        
        html += `
            <tr>
                <td><strong>#${job.id}</strong></td>
                <td>${job.mapping_configuration_name}</td>
                <td>${statusBadge}</td>
                <td>${progressBar}</td>
                <td>
                    <small class="text-muted">
                        ${job.processed_records}/${job.total_records}
                        ${job.failed_records > 0 ? `<br><span class="text-danger">${job.failed_records} failed</span>` : ''}
                    </small>
                </td>
                <td><small class="text-muted">${duration}</small></td>
                <td>${actions}</td>
            </tr>
        `;
    });
    
    html += '</tbody></table></div>';
    container.innerHTML = html;
}

// Get status badge HTML
function getStatusBadge(status) {
    const badges = {
        'pending': 'bg-warning',
        'running': 'bg-primary',
        'completed': 'bg-success',
        'failed': 'bg-danger',
        'stopped': 'bg-secondary'
    };
    
    return `<span class="badge ${badges[status] || 'bg-secondary'}">${status}</span>`;
}

// Get progress bar HTML
function getProgressBar(job) {
    if (job.status === 'running' && job.total_records > 0) {
        const percentage = Math.round(job.progress_percentage);
        return `
            <div class="progress" style="height: 20px;">
                <div class="progress-bar" role="progressbar" style="width: ${percentage}%" 
                     aria-valuenow="${percentage}" aria-valuemin="0" aria-valuemax="100">
                    ${percentage}%
                </div>
            </div>
        `;
    } else if (job.status === 'completed') {
        return '<div class="progress" style="height: 20px;"><div class="progress-bar bg-success" style="width: 100%">100%</div></div>';
    } else {
        return '<span class="text-muted">-</span>';
    }
}

// Get duration string
function getDuration(job) {
    if (!job.start_time) return '-';
    
    const start = new Date(job.start_time);
    const end = job.end_time ? new Date(job.end_time) : new Date();
    const duration = end - start;
    
    const hours = Math.floor(duration / (1000 * 60 * 60));
    const minutes = Math.floor((duration % (1000 * 60 * 60)) / (1000 * 60));
    const seconds = Math.floor((duration % (1000 * 60)) / 1000);
    
    if (hours > 0) {
        return `${hours}h ${minutes}m`;
    } else if (minutes > 0) {
        return `${minutes}m ${seconds}s`;
    } else {
        return `${seconds}s`;
    }
}

// Get job actions HTML
function getJobActions(job) {
    let actions = `
        <button class="btn btn-sm btn-outline-primary" onclick="showJobDetails(${job.id})">
            <i class="fas fa-eye"></i>
        </button>
    `;
    
    if (job.status === 'running') {
        actions += `
            <button class="btn btn-sm btn-outline-warning ms-1" onclick="stopJobConfirm(${job.id})">
                <i class="fas fa-stop"></i>
            </button>
        `;
    } else if (job.status === 'failed') {
        actions += `
            <button class="btn btn-sm btn-outline-success ms-1" onclick="retryJobConfirm(${job.id})">
                <i class="fas fa-redo"></i>
            </button>
        `;
    }
    
    return actions;
}

// Show job details modal
async function showJobDetails(jobId) {
    selectedJobId = jobId;
    
    try {
        const response = await axios.get(`/api/migration/jobs/${jobId}`);
        const job = response.data;
        
        renderJobDetails(job);
        
        // Show/hide action buttons based on status
        const stopBtn = document.getElementById('stopJobBtn');
        const retryBtn = document.getElementById('retryJobBtn');
        
        stopBtn.style.display = job.status === 'running' ? 'inline-block' : 'none';
        retryBtn.style.display = job.status === 'failed' ? 'inline-block' : 'none';
        
        const modal = new bootstrap.Modal(document.getElementById('jobDetailsModal'));
        modal.show();
    } catch (error) {
        console.error('Error loading job details:', error);
        alert('Error loading job details');
    }
}

// Render job details
function renderJobDetails(job) {
    const container = document.getElementById('job-details-content');
    
    const statusBadge = getStatusBadge(job.status);
    const duration = getDuration(job);
    
    let html = `
        <div class="row">
            <div class="col-md-6">
                <h6>Job Information</h6>
                <table class="table table-sm">
                    <tr><td><strong>Job ID:</strong></td><td>#${job.id}</td></tr>
                    <tr><td><strong>Status:</strong></td><td>${statusBadge}</td></tr>
                    <tr><td><strong>Mapping:</strong></td><td>${job.mapping_configuration_name}</td></tr>
                    <tr><td><strong>Created:</strong></td><td>${new Date(job.created_at).toLocaleString()}</td></tr>
                    <tr><td><strong>Started:</strong></td><td>${job.start_time ? new Date(job.start_time).toLocaleString() : '-'}</td></tr>
                    <tr><td><strong>Ended:</strong></td><td>${job.end_time ? new Date(job.end_time).toLocaleString() : '-'}</td></tr>
                    <tr><td><strong>Duration:</strong></td><td>${duration}</td></tr>
                </table>
            </div>
            <div class="col-md-6">
                <h6>Progress Information</h6>
                <table class="table table-sm">
                    <tr><td><strong>Total Records:</strong></td><td>${job.total_records.toLocaleString()}</td></tr>
                    <tr><td><strong>Processed:</strong></td><td>${job.processed_records.toLocaleString()}</td></tr>
                    <tr><td><strong>Failed:</strong></td><td>${job.failed_records.toLocaleString()}</td></tr>
                    <tr><td><strong>Success Rate:</strong></td><td>${job.total_records > 0 ? Math.round(((job.processed_records - job.failed_records) / job.total_records) * 100) : 0}%</td></tr>
                </table>
                
                ${job.status === 'running' && job.total_records > 0 ? `
                    <div class="mt-3">
                        <label class="form-label">Progress</label>
                        <div class="progress">
                            <div class="progress-bar" style="width: ${job.progress_percentage}%">
                                ${Math.round(job.progress_percentage)}%
                            </div>
                        </div>
                    </div>
                ` : ''}
            </div>
        </div>
    `;
    
    if (job.error_message) {
        html += `
            <div class="row mt-3">
                <div class="col-12">
                    <h6>Error Details</h6>
                    <div class="alert alert-danger">
                        <pre>${job.error_message}</pre>
                    </div>
                </div>
            </div>
        `;
    }
    
    container.innerHTML = html;
}

// Stop job
async function stopJob() {
    if (!selectedJobId) return;
    
    try {
        await axios.post(`/api/migration/jobs/${selectedJobId}/stop`);
        
        bootstrap.Modal.getInstance(document.getElementById('jobDetailsModal')).hide();
        loadJobs();
        
        showNotification('Job stopped successfully', 'warning');
    } catch (error) {
        console.error('Error stopping job:', error);
        alert('Error stopping job: ' + (error.response?.data?.error || 'Unknown error'));
    }
}

// Retry job
async function retryJob() {
    if (!selectedJobId) return;
    
    try {
        await axios.post(`/api/migration/jobs/${selectedJobId}/retry`);
        
        bootstrap.Modal.getInstance(document.getElementById('jobDetailsModal')).hide();
        loadJobs();
        
        showNotification('Job restarted successfully', 'success');
    } catch (error) {
        console.error('Error retrying job:', error);
        alert('Error retrying job: ' + (error.response?.data?.error || 'Unknown error'));
    }
}

// Stop job with confirmation
function stopJobConfirm(jobId) {
    if (confirm('Are you sure you want to stop this migration job?')) {
        selectedJobId = jobId;
        stopJob();
    }
}

// Retry job with confirmation
function retryJobConfirm(jobId) {
    if (confirm('Are you sure you want to retry this failed migration job?')) {
        selectedJobId = jobId;
        retryJob();
    }
}

// Refresh jobs
function refreshJobs() {
    loadJobs();
    showNotification('Jobs refreshed', 'info');
}

// Stop all running jobs
async function stopAllJobs() {
    const runningJobs = migrationJobs.filter(job => job.status === 'running');
    
    if (runningJobs.length === 0) {
        alert('No running jobs to stop');
        return;
    }
    
    if (!confirm(`Are you sure you want to stop all ${runningJobs.length} running jobs?`)) {
        return;
    }
    
    try {
        const promises = runningJobs.map(job => 
            axios.post(`/api/migration/jobs/${job.id}/stop`)
        );
        
        await Promise.all(promises);
        loadJobs();
        
        showNotification(`${runningJobs.length} jobs stopped`, 'warning');
    } catch (error) {
        console.error('Error stopping jobs:', error);
        alert('Error stopping some jobs');
    }
}

// Clear completed jobs (this would need backend implementation)
function clearCompletedJobs() {
    const completedJobs = migrationJobs.filter(job => job.status === 'completed');
    
    if (completedJobs.length === 0) {
        alert('No completed jobs to clear');
        return;
    }
    
    if (confirm(`Are you sure you want to clear ${completedJobs.length} completed jobs from the list?`)) {
        // This would require backend implementation
        alert('Feature not yet implemented');
    }
}

// Show start job modal
async function showStartJobModal() {
    await loadMappingConfigurations();
    
    const modal = new bootstrap.Modal(document.getElementById('startJobModal'));
    modal.show();
}

// Load mapping configurations for job creation
async function loadMappingConfigurations() {
    try {
        const response = await axios.get('/api/mapping/configurations');
        const select = document.getElementById('mappingConfigSelect');
        
        select.innerHTML = '<option value="">Select a mapping configuration...</option>';
        response.data.forEach(config => {
            select.innerHTML += `<option value="${config.id}">${config.name}</option>`;
        });
    } catch (error) {
        console.error('Error loading mapping configurations:', error);
    }
}

// Start new migration job
async function startNewJob() {
    const mappingConfigId = document.getElementById('mappingConfigSelect').value;
    const previewFirst = document.getElementById('previewBeforeStart').checked;
    
    if (!mappingConfigId) {
        alert('Please select a mapping configuration');
        return;
    }
    
    try {
        if (previewFirst) {
            // Show preview first
            const previewResponse = await axios.post('/api/migration/preview', {
                mapping_configuration_id: parseInt(mappingConfigId),
                limit: 5
            });
            
            if (confirm('Preview looks good? Start the migration job?')) {
                await createAndStartJob(mappingConfigId);
            }
        } else {
            await createAndStartJob(mappingConfigId);
        }
    } catch (error) {
        console.error('Error starting job:', error);
        alert('Error starting migration job: ' + (error.response?.data?.error || 'Unknown error'));
    }
}

// Create and start job
async function createAndStartJob(mappingConfigId) {
    const response = await axios.post('/api/migration/jobs', {
        mapping_configuration_id: parseInt(mappingConfigId)
    });
    
    bootstrap.Modal.getInstance(document.getElementById('startJobModal')).hide();
    document.getElementById('startJobForm').reset();
    
    loadJobs();
    showNotification('Migration job started successfully!', 'success');
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
