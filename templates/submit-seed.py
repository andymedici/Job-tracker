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
                        <a href="/dashboard" class="nav-link px-3 py-2 rounded-md text-sm font-medium">📈 Dashboard</a>
                        <a href="/analytics" class="nav-link px-3 py-2 rounded-md text-sm font-medium">📊 Analytics</a>
                        <a href="/companies" class="nav-link px-3 py-2 rounded-md text-sm font-medium">🏢 Companies</a>
                        <a href="/jobs" class="nav-link px-3 py-2 rounded-md text-sm font-medium">💼 Jobs</a>
                        <a href="/submit-seed" class="nav-link active px-3 py-2 rounded-md text-sm font-medium">➕ Submit</a>
                    </div>
                </div>
            </div>
        </div>
    </nav>

    <div class="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div class="bg-white rounded-lg shadow-lg p-8">
            <h2 class="text-2xl font-bold text-gray-900 mb-2">Submit a Company</h2>
            <p class="text-gray-600 mb-6">
                Add a company to our seed database. We'll test if they use Greenhouse, Lever, Workday, or other ATS platforms.
            </p>

            <div id="alert" class="hidden mb-6 p-4 rounded-lg"></div>

            <form id="submit-form" class="space-y-6">
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-2">
                        Company Name <span class="text-red-500">*</span>
                    </label>
                    <input 
                        type="text" 
                        id="company-name" 
                        required
                        placeholder="e.g., Acme Corporation"
                        class="w-full border-gray-300 rounded-md shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    >
                    <p class="mt-1 text-sm text-gray-500">Official company name</p>
                </div>

                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-2">
                        Website URL (Optional)
                    </label>
                    <input 
                        type="url" 
                        id="website-url" 
                        placeholder="https://example.com"
                        class="w-full border-gray-300 rounded-md shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    >
                    <p class="mt-1 text-sm text-gray-500">Company website or careers page URL</p>
                </div>

                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-2">
                        ATS Type Hint (Optional)
                    </label>
                    <select 
                        id="ats-hint"
                        class="w-full border-gray-300 rounded-md shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    >
                        <option value="">Unknown - Test All</option>
                        <option value="greenhouse">Greenhouse</option>
                        <option value="lever">Lever</option>
                        <option value="workday">Workday</option>
                        <option value="ashby">Ashby</option>
                        <option value="jobvite">Jobvite</option>
                        <option value="smartrecruiters">SmartRecruiters</option>
                    </select>
                    <p class="mt-1 text-sm text-gray-500">If you know which platform they use</p>
                </div>

                <div class="flex items-center">
                    <input 
                        type="checkbox" 
                        id="test-immediately"
                        class="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    >
                    <label for="test-immediately" class="ml-2 block text-sm text-gray-700">
                        Test immediately (otherwise will be tested in next scheduled discovery)
                    </label>
                </div>

                <div class="flex space-x-4">
                    <button 
                        type="submit"
                        class="flex-1 bg-blue-600 text-white px-6 py-3 rounded-md hover:bg-blue-700 font-medium transition"
                    >
                        Submit Company
                    </button>
                    <button 
                        type="button"
                        onclick="window.location.href='/dashboard'"
                        class="px-6 py-3 border border-gray-300 rounded-md hover:bg-gray-50 font-medium transition"
                    >
                        Cancel
                    </button>
                </div>
            </form>

            <div class="mt-8 p-4 bg-blue-50 rounded-lg">
                <h3 class="font-semibold text-blue-900 mb-2">💡 Tips</h3>
                <ul class="text-sm text-blue-800 space-y-1">
                    <li>• We'll automatically detect which ATS platform they use</li>
                    <li>• Submissions are added to the seed database for testing</li>
                    <li>• If immediate testing is enabled, results appear within minutes</li>
                    <li>• Check the Companies page to see if we found their jobs</li>
                </ul>
            </div>
        </div>
    </div>

    <style>
        .nav-link {
            color: #6b7280;
            transition: all 0.2s;
        }
        .nav-link:hover {
            color: #1f2937;
            background-color: #f3f4f6;
        }
        .nav-link.active {
            color: #3b82f6;
            background-color: #eff6ff;
        }
    </style>

    <script>
        const API_BASE = window.location.origin;
        const ADMIN_API_KEY = localStorage.getItem('admin_api_key') || '';

        function showAlert(message, type = 'info') {
            const alert = document.getElementById('alert');
            const colors = {
                success: 'bg-green-100 border-green-400 text-green-800',
                error: 'bg-red-100 border-red-400 text-red-800',
                info: 'bg-blue-100 border-blue-400 text-blue-800'
            };
            
            alert.className = `mb-6 p-4 rounded-lg border ${colors[type]}`;
            alert.textContent = message;
            alert.classList.remove('hidden');
            
            setTimeout(() => {
                alert.classList.add('hidden');
            }, 5000);
        }

        document.getElementById('submit-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            
            if (!ADMIN_API_KEY) {
                showAlert('Admin API key required. Please set it in the dashboard.', 'error');
                setTimeout(() => window.location.href = '/dashboard', 2000);
                return;
            }

            const companyName = document.getElementById('company-name').value.trim();
            const websiteUrl = document.getElementById('website-url').value.trim();
            const atsHint = document.getElementById('ats-hint').value;
            const testImmediately = document.getElementById('test-immediately').checked;

            if (!companyName) {
                showAlert('Company name is required', 'error');
                return;
            }

            try {
                const response = await fetch(`${API_BASE}/api/seeds/manual`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-API-Key': ADMIN_API_KEY
                    },
                    body: JSON.stringify({
                        company_name: companyName,
                        website_url: websiteUrl,
                        ats_hint: atsHint,
                        test_immediately: testImmediately
                    })
                });

                const data = await response.json();

                if (response.ok) {
                    showAlert(data.message, 'success');
                    document.getElementById('submit-form').reset();
                    
                    if (testImmediately) {
                        showAlert('Company submitted and testing started! Check the Companies page in a few minutes.', 'success');
                    }
                } else {
                    showAlert(data.error || 'Failed to submit company', 'error');
                }
            } catch (error) {
                showAlert('Network error: ' + error.message, 'error');
            }
        });

        // Check for admin key on load
        if (!ADMIN_API_KEY) {
            showAlert('Admin access required. Redirecting to dashboard...', 'error');
            setTimeout(() => window.location.href = '/dashboard', 2000);
        }
    </script>
</body>
</html>
