// Product Management
let editingProductId = null;
// Removed allProducts array - we now use server-side pagination
let currentPage = 1;
const itemsPerPage = 10;

// Modal functions
function openModal(modalId) {
    document.getElementById(modalId).classList.add('show');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('show');
}

// Fetch and display products with server-side pagination
async function fetchProducts() {
    // Get current filter values
    const searchTerm = document.getElementById('search-input').value;
    const statusFilter = document.getElementById('status-filter').value;
    const skip = (currentPage - 1) * itemsPerPage;

    // Build the query string
    const params = new URLSearchParams();
    params.append('skip', skip);
    params.append('limit', itemsPerPage);
    
    if (searchTerm) {
        params.append('search', searchTerm);
    }
    if (statusFilter) {
        // Convert string "true"/"false" to boolean for API
        // FastAPI will parse the boolean string correctly
        params.append('active', statusFilter === 'true');
    }

    try {
        const response = await fetch(`/products/?${params.toString()}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        // data is now { total: 12345, products: [...] }
        const products = data.products;
        const totalItems = data.total;
        
        const totalPages = Math.ceil(totalItems / itemsPerPage);
        
        populateProductsTable(products);
        renderPagination(totalPages);
        
    } catch (error) {
        console.error('Error fetching products:', error);
        alert('Failed to fetch products: ' + error.message);
    }
}

// Apply filters and pagination
// This function now just tells fetchProducts to get the new filtered data from the server
function applyFilters() {
    fetchProducts();
}

// Populate products table
function populateProductsTable(products) {
    const tbody = document.querySelector('#products-table tbody');
    tbody.innerHTML = '';

    if (products.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align: center;">No products found</td></tr>';
        return;
    }

    products.forEach(product => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${product.sku}</td>
            <td>${product.name}</td>
            <td>${product.description || ''}</td>
            <td>${product.active ? 'Yes' : 'No'}</td>
            <td>
                <button class="edit-btn" data-id="${product.id}">Edit</button>
                <button class="delete-btn" data-id="${product.id}">Delete</button>
            </td>
        `;
        tbody.appendChild(row);
    });

    // Add event listeners
    document.querySelectorAll('.edit-btn').forEach(btn => {
        btn.addEventListener('click', handleEdit);
    });
    document.querySelectorAll('.delete-btn').forEach(btn => {
        btn.addEventListener('click', handleDelete);
    });
}

// Render pagination with smart page window
function renderPagination(totalPages) {
    const paginationDiv = document.getElementById('pagination');
    paginationDiv.innerHTML = '';
    
    if (totalPages <= 1) return;
    
    // Previous button
    const prevBtn = document.createElement('button');
    prevBtn.textContent = '<< Previous';
    prevBtn.disabled = currentPage === 1;
    prevBtn.classList.toggle('disabled', currentPage === 1);
    prevBtn.addEventListener('click', () => {
        if (currentPage > 1) {
            currentPage--;
            applyFilters();
        }
    });
    paginationDiv.appendChild(prevBtn);
    
    // Smart pagination: show first, last, current page ± 2, and ellipsis
    const maxVisiblePages = 7; // Show max 7 page numbers
    const pagesToShow = [];
    
    if (totalPages <= maxVisiblePages) {
        // Show all pages if total is small
        for (let i = 1; i <= totalPages; i++) {
            pagesToShow.push(i);
        }
    } else {
        // Always show first page
        pagesToShow.push(1);
        
        // Calculate window around current page
        const windowSize = 2; // Show 2 pages on each side of current
        let start = Math.max(2, currentPage - windowSize);
        let end = Math.min(totalPages - 1, currentPage + windowSize);
        
        // Adjust window if we're near the start or end
        if (currentPage <= windowSize + 2) {
            end = Math.min(totalPages - 1, maxVisiblePages - 1);
        } else if (currentPage >= totalPages - windowSize - 1) {
            start = Math.max(2, totalPages - (maxVisiblePages - 2));
        }
        
        // Add ellipsis after first page if needed
        if (start > 2) {
            pagesToShow.push('...');
        }
        
        // Add pages in window
        for (let i = start; i <= end; i++) {
            pagesToShow.push(i);
        }
        
        // Add ellipsis before last page if needed
        if (end < totalPages - 1) {
            pagesToShow.push('...');
        }
        
        // Always show last page
        pagesToShow.push(totalPages);
    }
    
    // Render page buttons
    pagesToShow.forEach((page) => {
        if (page === '...') {
            const ellipsis = document.createElement('span');
            ellipsis.textContent = '...';
            ellipsis.style.padding = '8px 12px';
            ellipsis.style.color = '#000';
            paginationDiv.appendChild(ellipsis);
        } else {
            const pageBtn = document.createElement('button');
            pageBtn.textContent = page;
            pageBtn.classList.toggle('active', page === currentPage);
            pageBtn.addEventListener('click', () => {
                currentPage = page;
                applyFilters();
            });
            paginationDiv.appendChild(pageBtn);
        }
    });
    
    // Next button
    const nextBtn = document.createElement('button');
    nextBtn.textContent = 'Next >>';
    nextBtn.disabled = currentPage === totalPages;
    nextBtn.classList.toggle('disabled', currentPage === totalPages);
    nextBtn.addEventListener('click', () => {
        if (currentPage < totalPages) {
            currentPage++;
            applyFilters();
        }
    });
    paginationDiv.appendChild(nextBtn);
}

// Handle edit button click
function handleEdit(event) {
    const productId = parseInt(event.target.getAttribute('data-id'));
    editingProductId = productId;

    fetch(`/products/${productId}`)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(product => {
            document.getElementById('product-sku').value = product.sku;
            document.getElementById('product-name').value = product.name;
            document.getElementById('product-description').value = product.description || '';
            document.getElementById('product-active').checked = product.active;
            document.getElementById('product-modal-title').textContent = 'Edit Product';
            openModal('product-modal');
        })
        .catch(error => {
            console.error('Error fetching product:', error);
            alert('Failed to fetch product: ' + error.message);
        });
}

// Handle delete button click
async function handleDelete(event) {
    const productId = parseInt(event.target.getAttribute('data-id'));
    
    if (!confirm(`Are you sure you want to delete product ${productId}?`)) {
        return;
    }

    try {
        const response = await fetch(`/products/${productId}`, {
            method: 'DELETE'
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        await fetchProducts();
    } catch (error) {
        console.error('Error deleting product:', error);
        alert('Failed to delete product: ' + error.message);
    }
}

// Handle product form submission
document.getElementById('product-form').addEventListener('submit', async (e) => {
    e.preventDefault();

    const formData = {
        sku: document.getElementById('product-sku').value,
        name: document.getElementById('product-name').value,
        description: document.getElementById('product-description').value || null,
        active: document.getElementById('product-active').checked
    };

    try {
        let response;
        if (editingProductId) {
            response = await fetch(`/products/${editingProductId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(formData)
            });
        } else {
            response = await fetch('/products/', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(formData)
            });
        }

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: 'Unknown error' }));
            throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
        }

        closeModal('product-modal');
        resetProductForm();
        await fetchProducts();
    } catch (error) {
        console.error('Error saving product:', error);
        alert('Failed to save product: ' + error.message);
    }
});

// Reset product form
function resetProductForm() {
    document.getElementById('product-form').reset();
    document.getElementById('product-active').checked = true;
    editingProductId = null;
    document.getElementById('product-modal-title').textContent = 'Add New Product';
}

// Add Product button
document.getElementById('add-product-btn').addEventListener('click', () => {
    resetProductForm();
    openModal('product-modal');
});

// Product modal close buttons
document.getElementById('product-modal-close').addEventListener('click', () => {
    closeModal('product-modal');
    resetProductForm();
});

document.getElementById('product-cancel-btn').addEventListener('click', () => {
    closeModal('product-modal');
    resetProductForm();
});

// Search and filter
document.getElementById('search-input').addEventListener('input', () => {
    currentPage = 1; // Reset to page 1 for new search
    applyFilters();
});

document.getElementById('status-filter').addEventListener('change', () => {
    currentPage = 1; // Reset to page 1 for new filter
    applyFilters();
});

// File Upload - Show selected filename
document.getElementById('file-input').addEventListener('change', (e) => {
    const fileInput = e.target;
    const fileNameDisplay = document.getElementById('file-name-display');
    
    if (fileInput.files && fileInput.files.length > 0) {
        fileNameDisplay.textContent = `Selected: ${fileInput.files[0].name}`;
        fileNameDisplay.style.display = 'inline-block';
    } else {
        fileNameDisplay.textContent = '';
        fileNameDisplay.style.display = 'none';
    }
});

// File Upload
document.getElementById('upload-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const fileInput = document.getElementById('file-input');
    const file = fileInput.files[0];
    
    if (!file) {
        alert('Please select a file to upload');
        return;
    }
    
    const progressContainer = document.getElementById('progress-container');
    const progressFill = document.querySelector('#upload-progress .progress-fill');
    const statusDiv = document.getElementById('upload-status');
    
    progressContainer.style.display = 'block';
    progressFill.style.width = '0%';
    statusDiv.textContent = 'Preparing upload...';
    
    try {
        const formData = new FormData();
        formData.append('file', file);
        
        const result = await fetch('/upload/csv', {
            method: 'POST',
            body: formData
        });
        
        if (!result.ok) {
            const errorData = await result.json().catch(() => ({ detail: `HTTP error! status: ${result.status}` }));
            throw new Error(errorData.detail || `HTTP error! status: ${result.status}`);
        }
        
        const resultData = await result.json();
        const jobId = resultData.job_id;
        
        const eventSource = new EventSource(`/upload/progress/${jobId}`);
        
        // Track last refresh time to avoid too frequent updates
        let lastProductRefresh = 0;
        const PRODUCT_REFRESH_INTERVAL = 3000; // Refresh product list every 3 seconds during upload
        
        eventSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                const status = data.status || 'unknown';
                const message = data.message || '';
                const progress = data.progress || 0;
                
                // Update progress bar with smooth animation (CSS transition handles this)
                progressFill.style.width = `${progress}%`;
                
                // Update status message with progress percentage for better feedback
                if (message) {
                    statusDiv.textContent = `${message} (${progress}%)`;
                } else {
                    statusDiv.textContent = `Status: ${status} (${progress}%)`;
                }
                
                // Refresh product list periodically during upload to show newly added products
                if (status === 'processing') {
                    const now = Date.now();
                    if (now - lastProductRefresh >= PRODUCT_REFRESH_INTERVAL) {
                        fetchProducts();
                        lastProductRefresh = now;
                    }
                }
                
                if (status === 'complete' || status === 'failed') {
                    eventSource.close();
                    
                    if (status === 'complete') {
                        statusDiv.textContent = 'Upload completed successfully!';
                        // Final refresh to show all products
                        fetchProducts();
                        // Clear file input
                        fileInput.value = '';
                        document.getElementById('file-name-display').style.display = 'none';
                    } else {
                        statusDiv.innerHTML = `Upload failed: ${message || 'Unknown error'}<br><button id="retry-upload-btn" class="btn-primary" style="margin-top: 10px;">Retry Upload</button>`;
                        // Store file for retry
                        window.lastUploadFile = file;
                        
                        // Add retry button listener (remove old one first if exists)
                        const oldRetryBtn = document.getElementById('retry-upload-btn');
                        if (oldRetryBtn) {
                            oldRetryBtn.replaceWith(oldRetryBtn.cloneNode(true));
                        }
                        const retryBtn = document.getElementById('retry-upload-btn');
                        if (retryBtn) {
                            retryBtn.addEventListener('click', async () => {
                                if (window.lastUploadFile) {
                                    // Create a new FileList-like object and set it to the input
                                    const dataTransfer = new DataTransfer();
                                    dataTransfer.items.add(window.lastUploadFile);
                                    fileInput.files = dataTransfer.files;
                                    
                                    // Trigger upload again
                                    const uploadForm = document.getElementById('upload-form');
                                    const fakeEvent = new Event('submit', { bubbles: true, cancelable: true });
                                    uploadForm.dispatchEvent(fakeEvent);
                                } else {
                                    alert('No file available to retry. Please select a new file.');
                                }
                            });
                        }
                    }
                }
            } catch (error) {
                console.error('Error parsing progress data:', error);
                statusDiv.textContent = 'Error parsing progress update';
            }
        };
        
        eventSource.onerror = (error) => {
            console.error('EventSource error:', error);
            eventSource.close();
            statusDiv.textContent = 'Connection to progress stream lost';
        };
        
    } catch (error) {
        console.error('Error uploading file:', error);
        alert('Failed to upload file: ' + error.message);
        statusDiv.textContent = 'Upload failed: ' + error.message;
    }
});

// Bulk Delete
document.getElementById('delete-all-btn').addEventListener('click', async () => {
    if (confirm('Are you sure? This cannot be undone.')) {
        try {
            const response = await fetch('/products/all', {
                method: 'DELETE'
            });

            if (!response.ok) {
                let errorMessage = `HTTP error! status: ${response.status}`;
                try {
                    const errorData = await response.json();
                    errorMessage = errorData.detail || errorData.message || JSON.stringify(errorData) || errorMessage;
                } catch (e) {
                    // If response is not JSON, use status text
                    errorMessage = response.statusText || errorMessage;
                }
                throw new Error(errorMessage);
            }

            const result = await response.json();
            alert(result.message || 'All products deleted successfully');
            await fetchProducts();
        } catch (error) {
            console.error('Error deleting all products:', error);
            const errorMessage = error.message || String(error) || 'Unknown error occurred';
            alert('Failed to delete all products: ' + errorMessage);
        }
    }
});

// Webhook Management
let editingWebhookId = null;

async function fetchWebhooks() {
    try {
        const response = await fetch('/webhooks/');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const webhooks = await response.json();
        populateWebhooksTable(webhooks);
    } catch (error) {
        console.error('Error fetching webhooks:', error);
        alert('Failed to fetch webhooks: ' + error.message);
    }
}

function populateWebhooksTable(webhooks) {
    const tbody = document.querySelector('#webhooks-table tbody');
    tbody.innerHTML = '';

    if (webhooks.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" style="text-align: center;">No webhooks found</td></tr>';
        return;
    }

    webhooks.forEach(webhook => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${webhook.url}</td>
            <td>${webhook.event_type}</td>
            <td>${webhook.is_active ? 'Yes' : 'No'}</td>
            <td>
                <button class="test-webhook-btn" data-id="${webhook.id}">Test</button>
                <button class="edit-webhook-btn" data-id="${webhook.id}">Edit</button>
                <button class="delete-webhook-btn" data-id="${webhook.id}">Delete</button>
            </td>
        `;
        tbody.appendChild(row);
    });

    document.querySelectorAll('.test-webhook-btn').forEach(btn => {
        btn.addEventListener('click', handleTestWebhook);
    });
    document.querySelectorAll('.edit-webhook-btn').forEach(btn => {
        btn.addEventListener('click', handleEditWebhook);
    });
    document.querySelectorAll('.delete-webhook-btn').forEach(btn => {
        btn.addEventListener('click', handleDeleteWebhook);
    });
}

async function handleDeleteWebhook(event) {
    const webhookId = parseInt(event.target.getAttribute('data-id'));
    
    if (!confirm(`Are you sure you want to delete webhook ${webhookId}?`)) {
        return;
    }

    try {
        const response = await fetch(`/webhooks/${webhookId}`, {
            method: 'DELETE'
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        await fetchWebhooks();
    } catch (error) {
        console.error('Error deleting webhook:', error);
        alert('Failed to delete webhook: ' + error.message);
    }
}

async function handleEditWebhook(event) {
    const webhookId = parseInt(event.target.getAttribute('data-id'));
    
    try {
        const response = await fetch(`/webhooks/${webhookId}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const webhook = await response.json();
        
        // Populate form
        document.getElementById('webhook-url').value = webhook.url;
        document.getElementById('webhook-event-type').value = webhook.event_type;
        document.getElementById('webhook-active').checked = webhook.is_active;
        
        editingWebhookId = webhookId;
        document.getElementById('webhook-modal-title').textContent = 'Edit Webhook';
        openModal('webhook-modal');
    } catch (error) {
        console.error('Error fetching webhook:', error);
        alert('Failed to load webhook: ' + error.message);
    }
}

async function handleTestWebhook(event) {
    const webhookId = parseInt(event.target.getAttribute('data-id'));
    const button = event.target;
    const originalText = button.textContent;
    
    button.disabled = true;
    button.textContent = 'Testing...';
    
    try {
        const response = await fetch(`/webhooks/${webhookId}/test`, {
            method: 'POST'
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const result = await response.json();
        
        let message = `Status: ${result.status_code || 'N/A'}\n`;
        message += `Response Time: ${result.response_time_ms ? result.response_time_ms + 'ms' : 'N/A'}\n`;
        message += `Message: ${result.message}`;
        
        if (result.success) {
            alert(`✅ Webhook test successful!\n\n${message}`);
        } else {
            alert(`❌ Webhook test failed!\n\n${message}`);
        }
    } catch (error) {
        console.error('Error testing webhook:', error);
        alert('Failed to test webhook: ' + error.message);
    } finally {
        button.disabled = false;
        button.textContent = originalText;
    }
}

document.getElementById('webhook-form').addEventListener('submit', async (e) => {
    e.preventDefault();

    const formData = {
        url: document.getElementById('webhook-url').value,
        event_type: document.getElementById('webhook-event-type').value,
        is_active: document.getElementById('webhook-active').checked
    };

    try {
        const url = editingWebhookId ? `/webhooks/${editingWebhookId}` : '/webhooks/';
        const method = editingWebhookId ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method: method,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: 'Unknown error' }));
            throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
        }

        closeModal('webhook-modal');
        resetWebhookForm();
        await fetchWebhooks();
    } catch (error) {
        console.error('Error saving webhook:', error);
        alert('Failed to save webhook: ' + error.message);
    }
});

function resetWebhookForm() {
    document.getElementById('webhook-form').reset();
    document.getElementById('webhook-active').checked = true;
    editingWebhookId = null;
    document.getElementById('webhook-modal-title').textContent = 'Add New Webhook';
}

document.getElementById('add-webhook-btn').addEventListener('click', () => {
    resetWebhookForm();
    openModal('webhook-modal');
});

document.getElementById('webhook-modal-close').addEventListener('click', () => {
    closeModal('webhook-modal');
    resetWebhookForm();
});

document.getElementById('webhook-cancel-btn').addEventListener('click', () => {
    closeModal('webhook-modal');
    resetWebhookForm();
});

// Close modals when clicking outside
window.addEventListener('click', (e) => {
    const productModal = document.getElementById('product-modal');
    const webhookModal = document.getElementById('webhook-modal');
    
    if (e.target === productModal) {
        closeModal('product-modal');
        resetProductForm();
    }
    if (e.target === webhookModal) {
        closeModal('webhook-modal');
        resetWebhookForm();
    }
});

// Load on page load
document.addEventListener('DOMContentLoaded', () => {
    fetchProducts();
    fetchWebhooks();
});
