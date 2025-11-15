// Product Management
let editingProductId = null;
// Removed allProducts array - we now use server-side pagination
let currentPage = 1;
const itemsPerPage = 10;

// Search optimization: debouncing and request cancellation
let searchDebounceTimer = null;
let currentSearchAbortController = null;
const SEARCH_DEBOUNCE_MS = 400; // Wait 400ms after user stops typing
const MIN_SEARCH_LENGTH = 2; // Minimum characters before searching

// Prefetching: cache for next page data
let prefetchCache = null;
let prefetchAbortController = null;
let currentQueryKey = null; // Track current query to invalidate cache on filter changes
let isLoadMoreMode = false; // Track if we're in "Load More" mode (when totalPages === null)

// Modal functions
function openModal(modalId) {
    document.getElementById(modalId).classList.add('show');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('show');
}

// Show loading state
function showLoadingState() {
    const tbody = document.querySelector('#products-table tbody');
    tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 20px;"><div class="loading-spinner"></div><div style="margin-top: 10px;">Loading products...</div></td></tr>';
    
    // Add loading indicator to search input (CSS handles the styling)
    const searchInput = document.getElementById('search-input');
    searchInput.classList.add('search-loading');
}

// Hide loading state
function hideLoadingState() {
    const searchInput = document.getElementById('search-input');
    searchInput.classList.remove('search-loading');
}

// Generate query key for cache invalidation
function getQueryKey(page) {
    const searchTerm = document.getElementById('search-input').value.trim();
    const searchType = document.getElementById('search-type-filter').value;
    const statusFilter = document.getElementById('status-filter').value;
    const skip = (page - 1) * itemsPerPage;
    
    const params = new URLSearchParams();
    params.append('skip', skip);
    params.append('limit', itemsPerPage);
    
    if (searchTerm && searchTerm.length >= MIN_SEARCH_LENGTH) {
        params.append('search', searchTerm);
        params.append('search_type', searchType || 'sku');
    }
    if (statusFilter) {
        params.append('active', statusFilter === 'true');
    }
    
    return params.toString();
}

// Prefetch next page in background
async function prefetchNextPage() {
    // Cancel any existing prefetch
    if (prefetchAbortController) {
        prefetchAbortController.abort();
    }
    
    // Get current filter values
    const searchTerm = document.getElementById('search-input').value.trim();
    const searchType = document.getElementById('search-type-filter').value;
    const statusFilter = document.getElementById('status-filter').value;
    const nextPage = currentPage + 1;
    const skip = (nextPage - 1) * itemsPerPage;
    
    // Don't prefetch if search is too short
    if (searchTerm && searchTerm.length < MIN_SEARCH_LENGTH) {
        return;
    }
    
    // Build the query string for next page
    const params = new URLSearchParams();
    params.append('skip', skip);
    params.append('limit', itemsPerPage);
    
    if (searchTerm && searchTerm.length >= MIN_SEARCH_LENGTH) {
        params.append('search', searchTerm);
        params.append('search_type', searchType || 'sku');
    }
    if (statusFilter) {
        params.append('active', statusFilter === 'true');
    }
    
    const queryKey = params.toString();
    
    // Only prefetch if we don't already have it cached
    if (prefetchCache && prefetchCache.queryKey === queryKey) {
        return; // Already cached
    }
    
    // Create new AbortController for prefetch
    prefetchAbortController = new AbortController();
    
    try {
        const response = await fetch(`/products/?${queryKey}`, {
            signal: prefetchAbortController.signal
        });
        
        if (!response.ok) {
            return; // Silently fail prefetch
        }
        
        const data = await response.json();
        
        // Cache the prefetched data
        prefetchCache = {
            queryKey: queryKey,
            data: data,
            page: nextPage
        };
        
        console.log('Next page prefetched');
    } catch (error) {
        // Silently ignore prefetch errors (aborted, network issues, etc.)
        if (error.name !== 'AbortError') {
            console.log('Prefetch failed (non-critical):', error.message);
        }
    } finally {
        prefetchAbortController = null;
    }
}

// Fetch and display products with server-side pagination
async function fetchProducts(showLoading = true, useCache = true) {
    // Cancel any pending search request
    if (currentSearchAbortController) {
        currentSearchAbortController.abort();
    }
    
    // Create new AbortController for this request
    currentSearchAbortController = new AbortController();
    
    // Get current filter values
    const searchTerm = document.getElementById('search-input').value.trim();
    const searchType = document.getElementById('search-type-filter').value;
    const statusFilter = document.getElementById('status-filter').value;
    const skip = (currentPage - 1) * itemsPerPage;
    
    // Check cache first if enabled
    const queryKey = getQueryKey(currentPage);
    if (useCache && prefetchCache && prefetchCache.queryKey === queryKey) {
        // Use cached data immediately
        const data = prefetchCache.data;
        const products = data.products;
        const totalItems = data.total;
        
        // Clear cache after use
        prefetchCache = null;
        
        // Handle null total
        if (totalItems === null) {
            isLoadMoreMode = true;
            const hasMore = products.length === itemsPerPage;
            const shouldAppend = currentPage > 1 && isLoadMoreMode;
            populateProductsTable(products, shouldAppend);
            renderPagination(null, hasMore);
        } else {
            isLoadMoreMode = false;
            const totalPages = Math.ceil(totalItems / itemsPerPage);
            const hasMore = currentPage < totalPages;
            populateProductsTable(products, false);
            renderPagination(totalPages, hasMore);
        }
        
        // Prefetch next page in background
        prefetchNextPage();
        return;
    }
    
    // Update current query key
    currentQueryKey = queryKey;

    // Validate minimum search length
    if (searchTerm && searchTerm.length < MIN_SEARCH_LENGTH) {
        // Don't search if less than minimum length, but allow empty search
        if (showLoading) {
            showLoadingState();
        }
        // Wait a bit to show loading, then show message
        setTimeout(() => {
            const tbody = document.querySelector('#products-table tbody');
            tbody.innerHTML = `<tr><td colspan="5" style="text-align: center; padding: 20px; color: #666;">Please enter at least ${MIN_SEARCH_LENGTH} characters to search</td></tr>`;
            hideLoadingState();
        }, 100);
        return;
    }

    // Build the query string
    const params = new URLSearchParams();
    params.append('skip', skip);
    params.append('limit', itemsPerPage);
    
    if (searchTerm && searchTerm.length >= MIN_SEARCH_LENGTH) {
        params.append('search', searchTerm);
        // Always include search_type (defaults to "sku" if not set)
        params.append('search_type', searchType || 'sku');
    }
    if (statusFilter) {
        // Convert string "true"/"false" to boolean for API
        // FastAPI will parse the boolean string correctly
        params.append('active', statusFilter === 'true');
    }

    if (showLoading) {
        showLoadingState();
    }

    try {
        const url = `/products/?${params.toString()}`;
        console.log('Fetching products from:', url);
        const response = await fetch(url, {
            signal: currentSearchAbortController.signal
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        console.log('Received data:', { total: data.total, productsCount: data.products?.length, products: data.products });
        
        // data is now { total: 12345, products: [...] } or { total: null, products: [...] }
        const products = data.products || [];
        const totalItems = data.total;
        
        // Handle null total (when count query times out on large datasets)
        if (totalItems === null) {
            // Use "Load More" pattern: append products instead of replacing
            isLoadMoreMode = true;
            const hasMore = products.length === itemsPerPage;
            // Append products if we're loading more, replace if it's a new search/filter
            const shouldAppend = currentPage > 1 && isLoadMoreMode;
            populateProductsTable(products, shouldAppend);
            renderPagination(null, hasMore); // Pass null for totalPages, hasMore to indicate if there are more pages
            
            // Prefetch next page if there might be more
            if (hasMore) {
                prefetchNextPage();
            }
        } else {
            // Standard pagination mode: replace products
            isLoadMoreMode = false;
            const totalPages = Math.ceil(totalItems / itemsPerPage);
            const hasMore = currentPage < totalPages;
            populateProductsTable(products, false); // Always replace in standard pagination
            renderPagination(totalPages, hasMore);
            
            // Prefetch next page if there are more pages
            if (hasMore) {
                prefetchNextPage();
            }
        }
        
        hideLoadingState();
        
    } catch (error) {
        // Don't show error if request was aborted (user typed new search)
        if (error.name === 'AbortError') {
            console.log('Search request cancelled');
            return;
        }
        
        console.error('Error fetching products:', error);
        hideLoadingState();
        
        const tbody = document.querySelector('#products-table tbody');
        tbody.innerHTML = `<tr><td colspan="5" style="text-align: center; padding: 20px; color: #d00;">Failed to fetch products: ${error.message}</td></tr>`;
    } finally {
        currentSearchAbortController = null;
    }
}

// Apply filters and pagination
// This function now just tells fetchProducts to get the new filtered data from the server
function applyFilters() {
    // Cancel any pending debounced search when pagination/filter changes
    if (searchDebounceTimer) {
        clearTimeout(searchDebounceTimer);
        searchDebounceTimer = null;
    }
    
    // Invalidate prefetch cache when filters change
    const newQueryKey = getQueryKey(currentPage);
    if (newQueryKey !== currentQueryKey) {
        prefetchCache = null;
        if (prefetchAbortController) {
            prefetchAbortController.abort();
        }
        // Reset Load More mode when filters change
        isLoadMoreMode = false;
    }
    
    fetchProducts(true); // Show loading state for pagination/filter changes
}

// Load more products (for Load More button)
function loadMore() {
    currentPage++;
    fetchProducts(false); // Don't show loading state for Load More (smoother UX)
}

// Populate products table
function populateProductsTable(products, append = false) {
    const tbody = document.querySelector('#products-table tbody');
    console.log('populateProductsTable called with:', { productsCount: products?.length, append, products });
    
    // Clear table if not appending
    if (!append) {
        tbody.innerHTML = '';
    }

    if (!products || products.length === 0) {
        // Only show "No products found" if we're not appending (first load)
        if (!append) {
            console.log('No products found, showing message');
            tbody.innerHTML = '<tr><td colspan="5" style="text-align: center;">No products found</td></tr>';
        }
        return;
    }

    products.forEach(product => {
        const row = document.createElement('tr');
        const description = product.description || '';
        // Escape HTML to prevent XSS
        const escapedDescription = description
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
        
        row.innerHTML = `
            <td>${product.sku}</td>
            <td>${product.name}</td>
            <td class="description-cell">${escapedDescription}</td>
            <td>${product.active ? 'Yes' : 'No'}</td>
            <td>
                <button class="edit-btn" data-id="${product.id}">Edit</button>
                <button class="delete-btn" data-id="${product.id}">Delete</button>
            </td>
        `;
        
        // Add click handler to description cell
        const descriptionCell = row.querySelector('.description-cell');
        if (description && description.length > 0) {
            descriptionCell.addEventListener('click', (e) => {
                e.stopPropagation(); // Prevent row click events
                const fullDescription = description; // Use original, unescaped description
                document.getElementById('description-modal-text').textContent = fullDescription;
                openModal('description-modal');
            });
        }
        
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
// This function now has two modes:
// 1. "Standard" (if totalPages is known): [Prev] [1] [...] [4] [5] [6] [...] [10] [Next]
// 2. "Load More" (if totalPages is null): [Load More] button that appends products
function renderPagination(totalPages, hasMore = true) {
    const paginationDiv = document.getElementById('pagination');
    paginationDiv.innerHTML = '';

    // --- CASE 1: totalPages is NULL (Fast count timed out) ---
    // Use "Load More" button pattern
    if (totalPages === null) {
        // Only show "Load More" button if there are more results
        if (!hasMore) {
            return; // No pagination needed - we've reached the end
        }
        
        const loadMoreBtn = document.createElement('button');
        loadMoreBtn.textContent = 'Load More';
        loadMoreBtn.classList.add('btn-primary');
        loadMoreBtn.style.margin = '20px auto';
        loadMoreBtn.style.display = 'block';
        loadMoreBtn.addEventListener('click', () => {
            loadMore();
        });
        paginationDiv.appendChild(loadMoreBtn);
        
        return; // We're done
    }

    // --- CASE 2: totalPages is KNOWN ---
    // We can render the "standard" numbered pagination.
    
    if (totalPages <= 1) {
        return; // No pagination needed
    }

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

    // --- Page Number Logic ---
    const windowSize = 2; // Pages to show on each side of the current page
    const pagesToShow = [];

    // Add page 1
    pagesToShow.push(1);

    // Add ellipsis after 1 if needed
    if (currentPage - windowSize > 2) {
        pagesToShow.push('...');
    }

    // Add pages in the "window"
    for (let i = Math.max(2, currentPage - windowSize); i <= Math.min(totalPages - 1, currentPage + windowSize); i++) {
        pagesToShow.push(i);
    }

    // Add ellipsis before last page if needed
    if (currentPage + windowSize < totalPages - 1) {
        pagesToShow.push('...');
    }

    // Add last page (if it's not page 1)
    if (totalPages > 1) {
        pagesToShow.push(totalPages);
    }
    
    // De-duplicate array (in case '1' or 'totalPages' was in the window)
    const uniquePagesToShow = [...new Set(pagesToShow)];

    // Render the page buttons
    uniquePagesToShow.forEach((page) => {
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
    nextBtn.disabled = currentPage === totalPages; // We know the total
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

        // Invalidate cache after delete
        prefetchCache = null;
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
        // Invalidate cache after create/update
        prefetchCache = null;
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

// Debounced search function
function debouncedSearch() {
    // Clear existing timer
    if (searchDebounceTimer) {
        clearTimeout(searchDebounceTimer);
    }
    
    // Reset to page 1 for new search
    currentPage = 1;
    
    // Invalidate prefetch cache when search changes
    prefetchCache = null;
    if (prefetchAbortController) {
        prefetchAbortController.abort();
    }
    
    // Reset Load More mode when search changes
    isLoadMoreMode = false;
    
    // Set new timer - wait for user to stop typing
    searchDebounceTimer = setTimeout(() => {
        fetchProducts(true); // Show loading state
        searchDebounceTimer = null;
    }, SEARCH_DEBOUNCE_MS);
}

// Search and filter with debouncing
document.getElementById('search-input').addEventListener('input', () => {
    debouncedSearch();
});

// Search type filter changes immediately (no debounce needed)
document.getElementById('search-type-filter').addEventListener('change', () => {
    currentPage = 1; // Reset to page 1 for new filter
    // Cancel any pending search
    if (searchDebounceTimer) {
        clearTimeout(searchDebounceTimer);
        searchDebounceTimer = null;
    }
    // Invalidate prefetch cache when filter changes
    prefetchCache = null;
    if (prefetchAbortController) {
        prefetchAbortController.abort();
    }
    // Reset Load More mode when filter changes
    isLoadMoreMode = false;
    fetchProducts(true); // Show loading state
});

// Status filter changes immediately (no debounce needed)
document.getElementById('status-filter').addEventListener('change', () => {
    currentPage = 1; // Reset to page 1 for new filter
    // Cancel any pending search
    if (searchDebounceTimer) {
        clearTimeout(searchDebounceTimer);
        searchDebounceTimer = null;
    }
    // Invalidate prefetch cache when filter changes
    prefetchCache = null;
    if (prefetchAbortController) {
        prefetchAbortController.abort();
    }
    // Reset Load More mode when filter changes
    isLoadMoreMode = false;
    fetchProducts(true); // Show loading state
});

// File Upload - Enhanced with drag-and-drop
const fileInput = document.getElementById('file-input');
const fileDropZone = document.getElementById('file-drop-zone');
const fileNameDisplay = document.getElementById('file-name-display');
const uploadBtn = document.getElementById('upload-btn');

// Function to handle file selection
function handleFileSelection(file) {
    if (file) {
        fileNameDisplay.textContent = file.name;
        fileNameDisplay.classList.add('show');
        fileDropZone.classList.add('has-file');
        uploadBtn.disabled = false;
    } else {
        fileNameDisplay.textContent = '';
        fileNameDisplay.classList.remove('show');
        fileDropZone.classList.remove('has-file');
        uploadBtn.disabled = true;
    }
}

// File input change event
fileInput.addEventListener('change', (e) => {
    const file = e.target.files && e.target.files.length > 0 ? e.target.files[0] : null;
    handleFileSelection(file);
});

// Drag and drop functionality
fileDropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    e.stopPropagation();
    fileDropZone.classList.add('drag-over');
});

fileDropZone.addEventListener('dragleave', (e) => {
    e.preventDefault();
    e.stopPropagation();
    fileDropZone.classList.remove('drag-over');
});

fileDropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    e.stopPropagation();
    fileDropZone.classList.remove('drag-over');
    
    const files = e.dataTransfer.files;
    if (files && files.length > 0) {
        const file = files[0];
        // Check if it's a CSV file
        if (file.name.toLowerCase().endsWith('.csv')) {
            // Create a new FileList and set it to the input
            const dataTransfer = new DataTransfer();
            dataTransfer.items.add(file);
            fileInput.files = dataTransfer.files;
            handleFileSelection(file);
        } else {
            alert('Please select a CSV file');
        }
    }
});

// File Upload
document.getElementById('upload-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const fileInput = document.getElementById('file-input');
    let file = fileInput.files[0];
    
    // If no file in input but we have a stored file (from failed upload), use that
    if (!file && window.lastUploadFile) {
        file = window.lastUploadFile;
        // Restore file to input
        const dataTransfer = new DataTransfer();
        dataTransfer.items.add(file);
        fileInput.files = dataTransfer.files;
        handleFileSelection(file);
    }
    
    if (!file) {
        alert('Please select a file to upload');
        return;
    }
    
    // Reset button text if it was set to "Retry Upload"
    uploadBtn.textContent = 'Upload CSV';
    
    const progressContainer = document.getElementById('progress-container');
    const progressFill = document.querySelector('#upload-progress .progress-fill');
    const statusDiv = document.getElementById('upload-status');
    const percentageDiv = document.getElementById('upload-percentage');
    
    progressContainer.style.display = 'block';
    progressFill.style.width = '0%';
    statusDiv.textContent = 'Preparing upload...';
    percentageDiv.textContent = '0%';
    uploadBtn.disabled = true;
    
    try {
        // Step 1: Initialize upload and get Cloudinary credentials
        statusDiv.textContent = 'Initializing upload...';
        const initResult = await fetch('/upload/csv/init', {
            method: 'POST'
        });
        
        if (!initResult.ok) {
            const errorData = await initResult.json().catch(() => ({ detail: `HTTP error! status: ${initResult.status}` }));
            throw new Error(errorData.detail || `HTTP error! status: ${initResult.status}`);
        }
        
        const initData = await initResult.json();
        const jobId = initData.job_id;
        const cloudinaryConfig = initData.cloudinary;
        
        let fileUrl = null;
        
        // Step 2: Upload to Cloudinary if configured, otherwise fallback to server upload
        if (cloudinaryConfig) {
            // Upload directly to Cloudinary (bypasses Vercel size limits)
            statusDiv.textContent = 'Uploading to cloud...';
            const cloudinaryFormData = new FormData();
            cloudinaryFormData.append('file', file);
            cloudinaryFormData.append('api_key', cloudinaryConfig.api_key);
            cloudinaryFormData.append('timestamp', cloudinaryConfig.timestamp);
            cloudinaryFormData.append('signature', cloudinaryConfig.signature);
            cloudinaryFormData.append('folder', cloudinaryConfig.folder);
            cloudinaryFormData.append('public_id', cloudinaryConfig.public_id);
            cloudinaryFormData.append('resource_type', 'raw');
            
            const cloudinaryResult = await fetch(cloudinaryConfig.upload_url, {
                method: 'POST',
                body: cloudinaryFormData
            });
            
            if (!cloudinaryResult.ok) {
                const errorText = await cloudinaryResult.text();
                throw new Error(`Cloudinary upload failed: ${errorText}`);
            }
            
            const cloudinaryData = await cloudinaryResult.json();
            fileUrl = cloudinaryData.secure_url || cloudinaryData.url;
            
            // Step 3: Notify backend that upload is complete
            statusDiv.textContent = 'Finalizing upload...';
            const completeResult = await fetch('/upload/csv/complete', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    job_id: jobId,
                    file_url: fileUrl
                })
            });
            
            if (!completeResult.ok) {
                const errorData = await completeResult.json().catch(() => ({ detail: `HTTP error! status: ${completeResult.status}` }));
                throw new Error(errorData.detail || `HTTP error! status: ${completeResult.status}`);
            }
        } else {
            // Fallback: Upload through server (will hit size limits on Vercel)
            statusDiv.textContent = 'Uploading file...';
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
            // jobId already set from init
        }
        
        const eventSource = new EventSource(`/upload/progress/${jobId}`);
        
        // Track last refresh time to avoid too frequent updates
        let lastProductRefresh = 0;
        const PRODUCT_REFRESH_INTERVAL = 2000; // Refresh product list every 2 seconds during upload (reduced for more real-time updates)
        
        // Start periodic refresh immediately when upload starts (even if status is queued)
        const refreshInterval = setInterval(() => {
            const now = Date.now();
            if (now - lastProductRefresh >= PRODUCT_REFRESH_INTERVAL) {
                prefetchCache = null; // Invalidate cache during upload
                fetchProducts(false, false); // Don't show loading, don't use cache
                lastProductRefresh = now;
            }
        }, PRODUCT_REFRESH_INTERVAL);
        
        eventSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                const status = data.status || 'unknown';
                const message = data.message || '';
                const progress = data.progress || 0;
                
                // Update progress bar with smooth animation (CSS transition handles this)
                progressFill.style.width = `${progress}%`;
                
                // Update status message and percentage separately
                if (message) {
                    statusDiv.textContent = message;
                } else {
                    statusDiv.textContent = `Status: ${status}`;
                }
                percentageDiv.textContent = `${progress}%`;
                
                // Refresh product list immediately when status changes to processing or when progress updates
                if (status === 'processing' || status === 'queued') {
                    const now = Date.now();
                    // Refresh more frequently when processing (every progress update or every 2 seconds)
                    if (status === 'processing' && progress > 0) {
                        // Refresh immediately on progress updates during processing
                        if (now - lastProductRefresh >= 1000) { // At least 1 second between refreshes
                            prefetchCache = null;
                            fetchProducts(false, false);
                            lastProductRefresh = now;
                        }
                    }
                }
                
                if (status === 'complete' || status === 'failed') {
                    // Stop the periodic refresh interval
                    clearInterval(refreshInterval);
                    eventSource.close();
                    
                    if (status === 'complete') {
                        statusDiv.textContent = 'Upload completed successfully!';
                        percentageDiv.textContent = '100%';
                        // Final refresh to show all products
                        prefetchCache = null; // Invalidate cache after upload
                        fetchProducts();
                        // Clear file input and reset UI
                        fileInput.value = '';
                        handleFileSelection(null);
                        // Hide progress after a delay
                        setTimeout(() => {
                            progressContainer.style.display = 'none';
                        }, 3000);
                    } else {
                        statusDiv.textContent = `Upload failed: ${message || 'Unknown error'}`;
                        percentageDiv.textContent = '';
                        // Store file for retry
                        window.lastUploadFile = file;
                        uploadBtn.disabled = false;
                        uploadBtn.textContent = 'Retry Upload';
                    }
                }
            } catch (error) {
                console.error('Error parsing progress data:', error);
                statusDiv.textContent = 'Error parsing progress update';
            }
        };
        
        eventSource.onerror = (error) => {
            console.error('EventSource error:', error);
            clearInterval(refreshInterval); // Stop periodic refresh
            eventSource.close();
            statusDiv.textContent = 'Connection to progress stream lost';
        };
        
    } catch (error) {
        console.error('Error uploading file:', error);
        alert('Failed to upload file: ' + error.message);
        statusDiv.textContent = 'Upload failed: ' + error.message;
        const percentageDiv = document.getElementById('upload-percentage');
        if (percentageDiv) {
            percentageDiv.textContent = '';
        }
        uploadBtn.disabled = false;
        uploadBtn.textContent = 'Upload CSV';
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
            // Invalidate cache after bulk delete
            prefetchCache = null;
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

// Description modal close buttons
document.getElementById('description-modal-close').addEventListener('click', () => {
    closeModal('description-modal');
});

document.getElementById('description-modal-close-btn').addEventListener('click', () => {
    closeModal('description-modal');
});

// Close modals when clicking outside
window.addEventListener('click', (e) => {
    const productModal = document.getElementById('product-modal');
    const webhookModal = document.getElementById('webhook-modal');
    const descriptionModal = document.getElementById('description-modal');
    
    if (e.target === productModal) {
        closeModal('product-modal');
        resetProductForm();
    }
    if (e.target === webhookModal) {
        closeModal('webhook-modal');
        resetWebhookForm();
    }
    if (e.target === descriptionModal) {
        closeModal('description-modal');
    }
});

// Load on page load
document.addEventListener('DOMContentLoaded', () => {
    fetchProducts(true); // Show loading on initial page load
    fetchWebhooks();
});
