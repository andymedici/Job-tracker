<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Submit Company - Job Intelligence Platform</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-50">
    <!-- Navigation -->
    <nav class="bg-white shadow-sm border-b border-gray-200">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="flex justify-between h-16">
                <div class="flex items-center space-x-8">
                    <h1 class="text-2xl font-bold text-gray-900">📊 Job Intelligence Platform</h1>
                    <div class="hidden md:flex items-center space-x-1">
                        <a href="/dashboard" class="px-3 py-2 rounded-md text-sm font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100">📈 Dashboard</a>
                        <a href="/analytics" class="px-3 py-2 rounded-md text-sm font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100">📊 Analytics</a>
                        <a href="/companies" class="px-3 py-2 rounded-md text-sm font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100">🏢 Companies</a>
                        <a href="/jobs" class="px-3 py-2 rounded-md text-sm font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100">💼 Jobs</a>
                    </div>
                </div>
            </div>
        </div>
    </nav>

    <!-- Main Content -->
    <div class="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div class="bg-white rounded-lg shadow-lg p-8">
            <h2 class="text-3xl font-bold text-gray-900 mb-2">🌱 Submit a Company</h2>
            <p class="text-gray-600 mb-8">Add a company to track their job postings. We'll automatically detect their ATS and start collecting jobs.</p>

            <!-- Alert -->
            <div id="alert" class="hidden mb-6 rounded-lg p-4"></div>

            <!-- Form -->
            <form id="submit-form" class="space-y-6">
                <!-- Company Name -->
                <div>
                    <label for="company-name" class="block text-sm font-medium text-gray-700 mb-2">
                        Company Name <span class="text-red-500">*</span>
                    </label>
                    <input 
                        type="text" 
                        id="company-name" 
                        name="company_name"
                        required
                        placeholder="e.g., Stripe, Netflix, Airbnb"
                        class="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    >
                    <p class="mt-1 text-sm text-gray-500">Just the company name - not a URL</p>
                </div>

                <!-- Website URL (Optional) -->
                <div>
                    <label for="website-url" class="block text-sm font-medium text-gray-700 mb-2">
                        Website URL (Optional)
                    </label>
                    <input 
                        type="url" 
                        id="website-url" 
                        name="website_url"
                        placeholder="https://example.com"
                        class="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    >
                    <p class="mt-1 text-sm text-gray-500">Optional - helps with validation</p>
                </div>

                <!-- ATS Hint -->
                <div>
                    <label for="ats-hint" class="block text-sm font-medium text-gray-700 mb-2">
                        ATS Platform (Optional)
                    </label>
                    <select 
                        id="ats-hint" 
                        name="ats_hint"
                        class="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    >
                        <option value="">Auto-detect (recommended)</option>
                        <option value="greenhouse">Greenhouse</option>
                        <option value="lever">Lever</option>
                        <option value="workday">Workday</option>
                        <option value="ashby">Ashby</option>
                    </select>
                    <p class="mt-1 text-sm text-gray-500">If you know their ATS, selecting it speeds up discovery</p>
                </div>

                <!-- Test Immediately -->
                <div class="flex items-start">
                    <div class="flex items-center h-5">
                        <input 
                            type="checkbox" 
                            id="test-immediately" 
                            name="test_immediately"
                            checked
                            class="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                        >
                    </div>
                    <div class="ml-3">
                        <label for="test-immediately" class="font-medium text-gray-700">Test Immediately</label>
                        <p class="text-sm text-gray-500">Discover and scrape jobs right away (recommended)</p>
                    </div>
                </div>

                <!-- Submit Button -->
                <div class="pt-4">
                    <button 
                        type="submit" 
                        id="submit-btn"
                        class="w-full bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition font-medium"
                    >
                        🚀 Submit Company
                    </button>
                </div>
            </form>

            <!-- Examples -->
            <div class="mt-8 pt-8 border-t border-gray-200">
                <h3 class="font-semibold text-gray-900 mb-3">💡 Quick Examples</h3>
                <div class="grid grid-cols-2 gap-3">
                    <button class="example-btn text-left px-4 py-2 border border-gray-200 rounded-lg hover:bg-gray-50 transition" data-company="Stripe" data-ats="greenhouse">
                        <div class="font-medium">Stripe</div>
                        <div class="text-xs text-gray-500">Greenhouse • 80+ jobs</div>
                    </button>
                    <button class="example-btn text-left px-4 py-2 border border-gray-200 rounded-lg hover:bg-gray-50 transition" data-company="Netflix" data-ats="greenhouse">
                        <div class="font-medium">Netflix</div>
                        <div class="text-xs text-gray-500">Greenhouse • 60+ jobs</div>
                    </button>
                    <button class="example-btn text-left px-4 py-2 border border-gray-200 rounded-lg hover:bg-gray-50 transition" data-company="Airbnb" data-ats="greenhouse">
                        <div class="font-medium">Airbnb</div>
                        <div class="text-xs text-gray-500">Greenhouse • 120+ jobs</div>
                    </button>
                    <button class="example-btn text-left px-4 py-2 border border-gray-200 rounded-lg hover:bg-gray-50 transition" data-company="Anthropic" data-ats="greenhouse">
                        <div class="font-medium">Anthropic</div>
                        <div class="text-xs text-gray-500">Greenhouse • 25+ jobs</div>
                    </button>
                </div>
            </div>
        </div>
    </div>

    <script>
        const API_BASE = window.location.origin;
        let API_KEY = localStorage.getItem('api_key') || '';

        // Show Alert
        function showAlert(message, type = 'info') {
            const alert = document.getElementById('alert');
            const colors = {
                success: 'bg-green-100 text-green-800 border border-green-200',
                error: 'bg-red-100 text-red-800 border border-red-200',
                info: 'bg-blue-100 text-blue-800 border border-blue-200',
                warning: 'bg-yellow-100 text-yellow-800 border border-yellow-200'
            };
            
            alert.className = `mb-6 rounded-lg p-4 ${colors[type]}`;
            alert.textContent = message;
            alert.classList.remove('hidden');
            
            if (type === 'success') {
                setTimeout(() => {
                    window.location.href = '/companies';
                }, 3000);
            }
        }

        // Form Submit
        document.getElementById('submit-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            
            const submitBtn = document.getElementById('submit-btn');
            submitBtn.disabled = true;
            submitBtn.textContent = '⏳ Submitting...';
            
            const formData = {
                company_name: document.getElementById('company-name').value.trim(),
                website_url: document.getElementById('website-url').value.trim() || null,
                ats_hint: document.getElementById('ats-hint').value || null,
                test_immediately: document.getElementById('test-immediately').checked
            };
            
            try {
                const response = await fetch(`${API_BASE}/api/seeds/manual`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-API-Key': API_KEY
                    },
                    body: JSON.stringify(formData)
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    let message = `✅ ${formData.company_name} added successfully!`;
                    
                    if (result.found && result.jobs > 0) {
                        message += ` Found ${result.jobs} jobs via ${result.ats_type}.`;
                    } else if (result.found) {
                        message += ` Discovered on ${result.ats_type} but no jobs found.`;
                    } else if (formData.test_immediately) {
                        message += ` Added to seeds. ATS not detected yet - will be tested in next discovery run.`;
                    }
                    
                    showAlert(message, 'success');
                } else {
                    showAlert(result.error || 'Failed to submit company', 'error');
                    submitBtn.disabled = false;
                    submitBtn.textContent = '🚀 Submit Company';
                }
            } catch (error) {
                showAlert(`Error: ${error.message}`, 'error');
                submitBtn.disabled = false;
                submitBtn.textContent = '🚀 Submit Company';
            }
        });

        // Example Buttons
        document.querySelectorAll('.example-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.getElementById('company-name').value = btn.dataset.company;
                document.getElementById('ats-hint').value = btn.dataset.ats;
                document.getElementById('test-immediately').checked = true;
            });
        });

        // Check API key
        if (!API_KEY) {
            showAlert('Please set your API key in the dashboard first', 'warning');
        }
    </script>
</body>
</html>
