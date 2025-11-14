// Product Management
let editingProductId = null;

// Fetch and display products
async function fetchProducts() {
    try {
        const response = await fetch('/products/');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const products = await response.json();
        populateProductsTable(products);
    } catch (error) {
        console.error('Error fetching products:', error);
        alert('Failed to fetch products: ' + error.message);
    }
}

// Populate products table
function populateProductsTable(products) {
    const tbody = document.querySelector('#products-table tbody');
    tbody.innerHTML = '';

    if (products.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" style="text-align: center;">No products found</td></tr>';
        return;
    }

    products.forEach(product => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${product.id}</td>
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

    // Add event listeners to edit and delete buttons
    document.querySelectorAll('.edit-btn').forEach(btn => {
        btn.addEventListener('click', handleEdit);
    });
    document.querySelectorAll('.delete-btn').forEach(btn => {
        btn.addEventListener('click', handleDelete);
    });
}

// Handle edit button click
function handleEdit(event) {
    const productId = parseInt(event.target.getAttribute('data-id'));
    editingProductId = productId;

    // Fetch product details
    fetch(`/products/${productId}`)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(product => {
            // Populate form
            document.getElementById('product-sku').value = product.sku;
            document.getElementById('product-name').value = product.name;
            document.getElementById('product-description').value = product.description || '';
            document.getElementById('product-active').checked = product.active;

            // Update form button text and show cancel button
            document.getElementById('product-submit-btn').textContent = 'Update Product';
            document.getElementById('product-cancel-btn').style.display = 'inline-block';

            // Scroll to form
            document.getElementById('product-form').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
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

        // Refresh products table
        await fetchProducts();
    } catch (error) {
        console.error('Error deleting product:', error);
        alert('Failed to delete product: ' + error.message);
    }
}

// Handle form submission
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
            // Update existing product
            response = await fetch(`/products/${editingProductId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(formData)
            });
        } else {
            // Create new product
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

        // Reset form and refresh table
        resetProductForm();
        await fetchProducts();
    } catch (error) {
        console.error('Error saving product:', error);
        alert('Failed to save product: ' + error.message);
    }
});

// Handle cancel button
document.getElementById('product-cancel-btn').addEventListener('click', () => {
    resetProductForm();
});

// Reset product form
function resetProductForm() {
    document.getElementById('product-form').reset();
    document.getElementById('product-active').checked = true;
    editingProductId = null;
    document.getElementById('product-submit-btn').textContent = 'Create Product';
    document.getElementById('product-cancel-btn').style.display = 'none';
}

// File Upload
document.getElementById('upload-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const fileInput = document.getElementById('file-input');
    const file = fileInput.files[0];
    
    if (!file) {
        alert('Please select a file to upload');
        return;
    }
    
    // Reset progress and status
    const progressFill = document.querySelector('#upload-progress .progress-fill');
    const statusDiv = document.getElementById('upload-status');
    progressFill.style.width = '0%';
    statusDiv.textContent = 'Uploading file...';
    
    try {
        // Create FormData and send file
        const formData = new FormData();
        formData.append('file', file);
        
        const response = await fetch('/upload/csv', {
            method: 'POST',
            body: formData
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: 'Unknown error' }));
            throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
        }
        
        const result = await response.json();
        const jobId = result.job_id;
        
        // Create EventSource for progress updates
        const eventSource = new EventSource(`/upload/progress/${jobId}`);
        
        eventSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                const status = data.status || 'unknown';
                const message = data.message || '';
                const progress = data.progress || 0;
                
                // Update progress bar
                progressFill.style.width = `${progress}%`;
                
                // Update status message
                if (message) {
                    statusDiv.textContent = message;
                } else {
                    statusDiv.textContent = `Status: ${status}`;
                }
                
                // Close EventSource if job is complete or failed
                if (status === 'complete' || status === 'failed') {
                    eventSource.close();
                    
                    if (status === 'complete') {
                        statusDiv.textContent = 'Upload completed successfully!';
                        // Refresh products table to show new products
                        fetchProducts();
                    } else {
                        statusDiv.textContent = `Upload failed: ${message || 'Unknown error'}`;
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
                const errorData = await response.json().catch(() => ({ detail: 'Unknown error' }));
                throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
            }

            alert('All products deleted successfully');
            // Refresh products table
            await fetchProducts();
        } catch (error) {
            console.error('Error deleting all products:', error);
            alert('Failed to delete all products: ' + error.message);
        }
    }
});

// Webhook Management
let editingWebhookId = null;

// Fetch and display webhooks
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

// Populate webhooks table
function populateWebhooksTable(webhooks) {
    const tbody = document.querySelector('#webhooks-table tbody');
    tbody.innerHTML = '';

    if (webhooks.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align: center;">No webhooks found</td></tr>';
        return;
    }

    webhooks.forEach(webhook => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${webhook.id}</td>
            <td>${webhook.url}</td>
            <td>${webhook.event_type}</td>
            <td>${webhook.is_active ? 'Yes' : 'No'}</td>
            <td>
                <button class="delete-webhook-btn" data-id="${webhook.id}">Delete</button>
            </td>
        `;
        tbody.appendChild(row);
    });

    // Add event listeners to delete buttons
    document.querySelectorAll('.delete-webhook-btn').forEach(btn => {
        btn.addEventListener('click', handleDeleteWebhook);
    });
}

// Handle delete webhook button click
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

        // Refresh webhooks table
        await fetchWebhooks();
    } catch (error) {
        console.error('Error deleting webhook:', error);
        alert('Failed to delete webhook: ' + error.message);
    }
}

// Handle webhook form submission
document.getElementById('webhook-form').addEventListener('submit', async (e) => {
    e.preventDefault();

    const formData = {
        url: document.getElementById('webhook-url').value,
        event_type: document.getElementById('webhook-event-type').value,
        is_active: document.getElementById('webhook-active').checked
    };

    try {
        const response = await fetch('/webhooks/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                url: formData.url,
                event_type: formData.event_type
            })
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: 'Unknown error' }));
            throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
        }

        // Reset form and refresh table
        resetWebhookForm();
        await fetchWebhooks();
    } catch (error) {
        console.error('Error saving webhook:', error);
        alert('Failed to save webhook: ' + error.message);
    }
});

// Handle cancel button
document.getElementById('webhook-cancel-btn').addEventListener('click', () => {
    resetWebhookForm();
});

// Reset webhook form
function resetWebhookForm() {
    document.getElementById('webhook-form').reset();
    document.getElementById('webhook-active').checked = true;
    editingWebhookId = null;
    document.getElementById('webhook-submit-btn').textContent = 'Create Webhook';
    document.getElementById('webhook-cancel-btn').style.display = 'none';
}

// Load products and webhooks on page load
document.addEventListener('DOMContentLoaded', () => {
    fetchProducts();
    fetchWebhooks();
});

