// Product Management
let editingProductId = null;
let allProducts = [];
let currentPage = 1;
const itemsPerPage = 10;

// Modal functions
function openModal(modalId) {
    document.getElementById(modalId).classList.add('show');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('show');
}

// Fetch and display products
async function fetchProducts() {
    try {
        const response = await fetch('/products/');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        allProducts = await response.json();
        applyFilters();
    } catch (error) {
        console.error('Error fetching products:', error);
        alert('Failed to fetch products: ' + error.message);
    }
}

// Apply filters and pagination
function applyFilters() {
    const searchTerm = document.getElementById('search-input').value.toLowerCase();
    const statusFilter = document.getElementById('status-filter').value;
    
    let filtered = allProducts.filter(product => {
        const matchesSearch = !searchTerm || 
            product.sku.toLowerCase().includes(searchTerm) ||
            product.name.toLowerCase().includes(searchTerm);
        const matchesStatus = !statusFilter || 
            (statusFilter === 'true' && product.active) ||
            (statusFilter === 'false' && !product.active);
        return matchesSearch && matchesStatus;
    });
    
    const totalPages = Math.ceil(filtered.length / itemsPerPage);
    const start = (currentPage - 1) * itemsPerPage;
    const end = start + itemsPerPage;
    const paginated = filtered.slice(start, end);
    
    populateProductsTable(paginated);
    renderPagination(totalPages);
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

// Render pagination
function renderPagination(totalPages) {
    const paginationDiv = document.getElementById('pagination');
    paginationDiv.innerHTML = '';
    
    if (totalPages <= 1) return;
    
    // Previous button
    const prevBtn = document.createElement('button');
    prevBtn.textContent = '<< Previous';
    prevBtn.disabled = currentPage === 1;
    prevBtn.addEventListener('click', () => {
        if (currentPage > 1) {
            currentPage--;
            applyFilters();
        }
    });
    paginationDiv.appendChild(prevBtn);
    
    // Page numbers
    for (let i = 1; i <= totalPages; i++) {
        const pageBtn = document.createElement('button');
        pageBtn.textContent = i;
        pageBtn.classList.toggle('active', i === currentPage);
        pageBtn.addEventListener('click', () => {
            currentPage = i;
            applyFilters();
        });
        paginationDiv.appendChild(pageBtn);
    }
    
    // Next button
    const nextBtn = document.createElement('button');
    nextBtn.textContent = 'Next >>';
    nextBtn.disabled = currentPage === totalPages;
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
    currentPage = 1;
    applyFilters();
});

document.getElementById('status-filter').addEventListener('change', () => {
    currentPage = 1;
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
    statusDiv.textContent = 'Uploading file...';
    
    try {
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
        
        const eventSource = new EventSource(`/upload/progress/${jobId}`);
        
        eventSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                const status = data.status || 'unknown';
                const message = data.message || '';
                const progress = data.progress || 0;
                
                progressFill.style.width = `${progress}%`;
                
                if (message) {
                    statusDiv.textContent = message;
                } else {
                    statusDiv.textContent = `Status: ${status}`;
                }
                
                if (status === 'complete' || status === 'failed') {
                    eventSource.close();
                    
                    if (status === 'complete') {
                        statusDiv.textContent = 'Upload completed successfully!';
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
            await fetchProducts();
        } catch (error) {
            console.error('Error deleting all products:', error);
            alert('Failed to delete all products: ' + error.message);
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
                <button class="delete-webhook-btn" data-id="${webhook.id}">Delete</button>
            </td>
        `;
        tbody.appendChild(row);
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

document.getElementById('webhook-form').addEventListener('submit', async (e) => {
    e.preventDefault();

    const formData = {
        url: document.getElementById('webhook-url').value,
        event_type: document.getElementById('webhook-event-type').value
    };

    try {
        const response = await fetch('/webhooks/', {
            method: 'POST',
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
