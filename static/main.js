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

// Load products on page load
document.addEventListener('DOMContentLoaded', () => {
    fetchProducts();
});

