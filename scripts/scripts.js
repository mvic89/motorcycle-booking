let allShops = [];
let allCountries = {};
let filteredShops = [];

// Load data
async function loadData() {
    try {
        const response = await fetch('./data/directory_data.json');
        const data = await response.json();
        allShops = data.shops;
        allCountries = data.countries;
        
        // Calculate total cities
        const totalCities = Object.values(allCountries).reduce((sum, cities) => sum + cities.length, 0);
        document.getElementById('totalCities').textContent = totalCities.toLocaleString();
        
        populateCountryFilter();
        applyFilters();
    } catch (error) {
        console.error('Error loading data:', error);
        document.getElementById('shopsContainer').innerHTML = 
            '<div class="no-results"><div class="no-results-icon">⚠️</div><h2>Error Loading Data</h2><p>Please make sure directory_data.json is in the data/ folder.</p></div>';
    }
}

function populateCountryFilter() {
    const countryFilter = document.getElementById('countryFilter');
    const countries = Object.keys(allCountries).sort();
    
    countries.forEach(country => {
        const option = document.createElement('option');
        option.value = country;
        option.textContent = country;
        countryFilter.appendChild(option);
    });
}

function updateCityFilter() {
    const countryFilter = document.getElementById('countryFilter');
    const cityFilter = document.getElementById('cityFilter');
    const selectedCountry = countryFilter.value;
    
    // Clear existing options
    cityFilter.innerHTML = '<option value="">All Cities</option>';
    
    if (selectedCountry && allCountries[selectedCountry]) {
        allCountries[selectedCountry].forEach(city => {
            const option = document.createElement('option');
            option.value = city;
            option.textContent = city;
            cityFilter.appendChild(option);
        });
        cityFilter.disabled = false;
    } else {
        cityFilter.disabled = true;
    }
}

// Update city filter when country changes
document.getElementById('countryFilter').addEventListener('change', updateCityFilter);

function applyFilters() {
    const searchTerm = document.getElementById('searchInput').value.toLowerCase();
    const selectedCountry = document.getElementById('countryFilter').value;
    const selectedCity = document.getElementById('cityFilter').value;
    const minRating = parseFloat(document.getElementById('ratingFilter').value);
    const sortBy = document.getElementById('sortBy').value;

    filteredShops = allShops.filter(shop => {
        // Search filter
        if (searchTerm && !shop.name.toLowerCase().includes(searchTerm)) {
            return false;
        }

        // Country filter
        if (selectedCountry) {
            const shopCountry = shop.city.split(', ').pop();
            if (shopCountry !== selectedCountry) {
                return false;
            }
        }

        // City filter
        if (selectedCity) {
            const shopCity = shop.city.split(', ')[0];
            if (shopCity !== selectedCity) {
                return false;
            }
        }

        // Rating filter
        if (minRating > 0) {
            const rating = parseFloat(shop.rating);
            if (isNaN(rating) || rating < minRating) {
                return false;
            }
        }

        return true;
    });

    // Sort
    filteredShops.sort((a, b) => {
        if (sortBy === 'rating') {
            const ratingA = parseFloat(a.rating) || 0;
            const ratingB = parseFloat(b.rating) || 0;
            return ratingB - ratingA;
        } else if (sortBy === 'reviews') {
            const reviewsA = parseInt(a.reviews_count) || 0;
            const reviewsB = parseInt(b.reviews_count) || 0;
            return reviewsB - reviewsA;
        } else {
            return a.name.localeCompare(b.name);
        }
    });

    displayShops();
}

function displayShops() {
    const container = document.getElementById('shopsContainer');
    const resultsCount = document.getElementById('resultsCount');
    
    resultsCount.textContent = filteredShops.length.toLocaleString();

    if (filteredShops.length === 0) {
        container.innerHTML = `
            <div class="no-results">
                <div class="no-results-icon">🔍</div>
                <h2>No shops found</h2>
                <p>Try adjusting your filters or search terms</p>
            </div>
        `;
        return;
    }

    container.className = 'shops-grid';
    container.innerHTML = filteredShops.map(shop => createShopCard(shop)).join('');
}

function createShopCard(shop) {
    const rating = parseFloat(shop.rating);
    const hasRating = !isNaN(rating);
    const stars = hasRating ? '⭐'.repeat(Math.round(rating)) : 'N/A';
    const reviewsCount = parseInt(shop.reviews_count) || 0;
    
    const hasPhone = shop.phone && shop.phone !== 'N/A';
    const hasWebsite = shop.website && shop.website !== 'N/A';
    const hasCoords = shop.latitude && shop.longitude;
    
    const hoursText = shop.hours || 'Hours not available';
    const isClosed = hoursText.toLowerCase().includes('closed');
    const hoursClass = isClosed ? 'hours-closed' : 'hours-open';
    
    return `
        <div class="shop-card">
            <div class="business-type-badge">${shop.business_type || 'Motorcycle Shop'}</div>
            <div class="shop-name">${shop.name}</div>
            
            ${hasRating ? `
                <div class="rating">
                    <span class="stars">${stars}</span>
                    <span class="rating-number">${rating.toFixed(1)}</span>
                    ${reviewsCount > 0 ? `<span class="reviews">(${reviewsCount} reviews)</span>` : ''}
                </div>
            ` : ''}
            
            <div class="shop-info">
                <div class="info-row">
                    <span class="info-icon">📍</span>
                    <span class="info-text">${shop.address}</span>
                </div>
                
                <div class="info-row">
                    <span class="info-icon">🏙️</span>
                    <span class="info-text">${shop.city}</span>
                </div>
                
                ${hasPhone ? `
                    <div class="info-row">
                        <span class="info-icon">📞</span>
                        <span class="info-text">${shop.phone}</span>
                    </div>
                ` : ''}
                
                <div class="info-row">
                    <span class="info-icon">🕐</span>
                    <span class="info-text">
                        ${hoursText}
                        <span class="hours-badge ${hoursClass}">
                            ${isClosed ? 'Closed' : 'Open'}
                        </span>
                    </span>
                </div>
            </div>
            
            <div class="shop-actions">
                ${hasPhone ? `
                    <a href="tel:${shop.phone}" class="action-btn btn-phone">📞 Call</a>
                ` : ''}
                ${hasWebsite ? `
                    <a href="${shop.website}" target="_blank" class="action-btn btn-website">🌐 Website</a>
                ` : ''}
                ${hasCoords ? `
                    <a href="https://www.google.com/maps?q=${shop.latitude},${shop.longitude}" target="_blank" class="action-btn btn-map">🗺️ Map</a>
                ` : ''}
            </div>
        </div>
    `;
}

function resetFilters() {
    document.getElementById('searchInput').value = '';
    document.getElementById('countryFilter').value = '';
    document.getElementById('cityFilter').value = '';
    document.getElementById('cityFilter').disabled = true;
    document.getElementById('ratingFilter').value = '0';
    document.getElementById('sortBy').value = 'rating';
    applyFilters();
}

// Search on typing
document.getElementById('searchInput').addEventListener('input', applyFilters);

// Load data on page load
loadData();