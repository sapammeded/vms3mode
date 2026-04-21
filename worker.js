// ==================== VMS WORKER v3.0 - FULL ENTERPRISE SYSTEM ====================
// Cloudflare Worker untuk VMS SAPAM MEDED
// KV Namespace: VMS_STORAGE

export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);
        const path = url.pathname;
        
        // CORS headers untuk semua response
        const corsHeaders = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, x-token',
            'Content-Type': 'application/json'
        };
        
        // Handle OPTIONS preflight
        if (request.method === 'OPTIONS') {
            return new Response(null, { headers: corsHeaders });
        }
        
        try {
            // ==================== LICENSE VALIDATION ====================
            if (path === '/validate-license' && request.method === 'POST') {
                const body = await request.json();
                const { licenseKey, deviceId, deviceName, meta } = body;
                
                if (!licenseKey) {
                    return new Response(JSON.stringify({ ok: false, message: 'License key required' }), { headers: corsHeaders });
                }
                
                // Cari company berdasarkan license key
                const companies = await getData(env, 'companies');
                const company = companies.find(c => c.licenseKey === licenseKey);
                
                if (!company) {
                    return new Response(JSON.stringify({ ok: false, message: 'Invalid license key' }), { headers: corsHeaders });
                }
                
                // Check expired
                const isExpired = company.expiredAt < Date.now();
                if (isExpired) {
                    return new Response(JSON.stringify({ 
                        ok: false, 
                        message: 'License expired',
                        company: { ...company, status: 'EXPIRED' }
                    }), { headers: corsHeaders });
                }
                
                // Cek device limit
                const devices = await getData(env, 'devices');
                const companyDevices = devices.filter(d => d.licenseKey === licenseKey && d.status !== 'DELETED');
                const currentDeviceCount = companyDevices.length;
                
                let status = 'ACTIVE';
                if (currentDeviceCount >= company.maxDevices) {
                    status = 'PENDING_APPROVAL';
                }
                
                // Register atau update device
                let device = devices.find(d => d.deviceId === deviceId && d.licenseKey === licenseKey);
                if (device) {
                    device.lastSeen = Date.now();
                    device.deviceName = deviceName || device.deviceName;
                    device.meta = meta;
                } else {
                    device = {
                        deviceId: deviceId,
                        deviceName: deviceName || deviceId,
                        licenseKey: licenseKey,
                        companyId: company.id,
                        companyName: company.companyName,
                        status: status,
                        firstSeen: Date.now(),
                        lastSeen: Date.now(),
                        meta: meta,
                        violations: [],
                        sessions: []
                    };
                    devices.push(device);
                    await saveData(env, 'devices', devices);
                }
                
                await saveData(env, 'devices', devices);
                
                // Update current devices count di company
                company.currentDevices = companyDevices.filter(d => d.status === 'ACTIVE').length;
                await saveData(env, 'companies', companies);
                
                return new Response(JSON.stringify({
                    ok: true,
                    status: status,
                    company: {
                        id: company.id,
                        name: company.companyName,
                        package: company.package,
                        maxDevices: company.maxDevices,
                        currentDevices: company.currentDevices,
                        expiredAt: company.expiredAt
                    },
                    device: device
                }), { headers: corsHeaders });
            }
            
            // ==================== CHECK-IN / CHECK-OUT ====================
            if (path === '/checkin' && request.method === 'POST') {
                const body = await request.json();
                const { licenseKey, deviceId, action, location } = body;
                
                // Validasi license
                const companies = await getData(env, 'companies');
                const company = companies.find(c => c.licenseKey === licenseKey);
                if (!company || company.expiredAt < Date.now()) {
                    return new Response(JSON.stringify({ ok: false, message: 'License invalid or expired' }), { headers: corsHeaders });
                }
                
                // Cari device
                const devices = await getData(env, 'devices');
                const device = devices.find(d => d.deviceId === deviceId && d.licenseKey === licenseKey);
                if (!device || device.status !== 'ACTIVE') {
                    return new Response(JSON.stringify({ ok: false, message: 'Device not active' }), { headers: corsHeaders });
                }
                
                // Catat activity
                const activities = await getData(env, 'activities');
                const activity = {
                    id: generateId(),
                    deviceId: deviceId,
                    deviceName: device.deviceName,
                    licenseKey: licenseKey,
                    companyId: company.id,
                    companyName: company.companyName,
                    action: action,
                    location: location || null,
                    timestamp: Date.now(),
                    type: action === 'IN' ? 'CHECK_IN' : 'CHECK_OUT'
                };
                activities.unshift(activity);
                await saveData(env, 'activities', activities.slice(0, 5000));
                
                // Update device last seen
                device.lastSeen = Date.now();
                await saveData(env, 'devices', devices);
                
                return new Response(JSON.stringify({ ok: true, activity: activity }), { headers: corsHeaders });
            }
            
            // ==================== REPORT VIOLATION ====================
            if (path === '/report-violation' && request.method === 'POST') {
                const body = await request.json();
                const { licenseKey, deviceId, violationType, details, location } = body;
                
                const companies = await getData(env, 'companies');
                const company = companies.find(c => c.licenseKey === licenseKey);
                if (!company) {
                    return new Response(JSON.stringify({ ok: false, message: 'Invalid license' }), { headers: corsHeaders });
                }
                
                const devices = await getData(env, 'devices');
                const device = devices.find(d => d.deviceId === deviceId);
                if (!device) {
                    return new Response(JSON.stringify({ ok: false, message: 'Device not found' }), { headers: corsHeaders });
                }
                
                // Catat violation
                const violation = {
                    id: generateId(),
                    deviceId: deviceId,
                    deviceName: device.deviceName,
                    licenseKey: licenseKey,
                    companyId: company.id,
                    companyName: company.companyName,
                    violationType: violationType,
                    details: details,
                    location: location,
                    timestamp: Date.now()
                };
                
                if (!device.violations) device.violations = [];
                device.violations.unshift(violation);
                
                // Hitung jumlah violation
                const violationCount = device.violations.length;
                let deviceStatus = device.status;
                
                if (violationCount >= 5) {
                    deviceStatus = 'BANNED';
                } else if (violationCount >= 3) {
                    deviceStatus = 'SUSPENDED';
                }
                
                device.status = deviceStatus;
                await saveData(env, 'devices', devices);
                
                // Simpan ke activities
                const activities = await getData(env, 'activities');
                activities.unshift({
                    id: generateId(),
                    ...violation,
                    type: 'VIOLATION_REPORTED'
                });
                await saveData(env, 'activities', activities.slice(0, 5000));
                
                return new Response(JSON.stringify({
                    ok: true,
                    violation: violation,
                    deviceStatus: deviceStatus,
                    violationCount: violationCount
                }), { headers: corsHeaders });
            }
            
            // ==================== REQUEST ADDITIONAL DEVICE (FEE) ====================
            if (path === '/request-device' && request.method === 'POST') {
                const body = await request.json();
                const { licenseKey, deviceName, reason } = body;
                
                const companies = await getData(env, 'companies');
                const company = companies.find(c => c.licenseKey === licenseKey);
                if (!company) {
                    return new Response(JSON.stringify({ ok: false, message: 'Invalid license' }), { headers: corsHeaders });
                }
                
                // Hitung fee (BASIC: 50k per device, PRO: gratis)
                let fee = 0;
                if (company.package === 'BASIC') {
                    const pricing = await getData(env, 'settings');
                    fee = (pricing?.extraDeviceFee || 50000);
                }
                
                const requests = await getData(env, 'device_requests');
                const newRequest = {
                    id: generateId(),
                    licenseKey: licenseKey,
                    companyId: company.id,
                    companyName: company.companyName,
                    deviceName: deviceName,
                    reason: reason,
                    fee: fee,
                    status: 'PENDING',
                    requestedAt: Date.now()
                };
                requests.push(newRequest);
                await saveData(env, 'device_requests', requests);
                
                return new Response(JSON.stringify({
                    ok: true,
                    requestId: newRequest.id,
                    fee: fee,
                    message: fee > 0 ? `Fee Rp ${fee.toLocaleString()} akan ditagihkan` : 'Request sent, waiting approval'
                }), { headers: corsHeaders });
            }
            
            // ==================== APPROVE DEVICE REQUEST (ADMIN) ====================
            if (path === '/approve-device-request' && request.method === 'POST') {
                const body = await request.json();
                const { requestId, approve, notes } = body;
                
                // Auth check (dari token)
                const auth = await checkAuth(request.headers, env);
                if (!auth || auth.role !== 'SUPER_ADMIN') {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const requests = await getData(env, 'device_requests');
                const request = requests.find(r => r.id === requestId);
                if (!request) {
                    return new Response(JSON.stringify({ ok: false, error: 'Request not found' }), { headers: corsHeaders });
                }
                
                if (!approve) {
                    request.status = 'REJECTED';
                    request.notes = notes;
                    request.processedAt = Date.now();
                    await saveData(env, 'device_requests', requests);
                    return new Response(JSON.stringify({ ok: true, status: 'REJECTED' }), { headers: corsHeaders });
                }
                
                // Approve - generate invoice
                request.status = 'WAITING_PAYMENT';
                request.processedAt = Date.now();
                
                // Buat invoice
                const invoices = await getData(env, 'invoices');
                const invoice = {
                    id: generateId(),
                    companyId: request.companyId,
                    companyName: request.companyName,
                    type: 'DEVICE_ADDITION',
                    requestId: requestId,
                    amount: request.fee,
                    months: 1,
                    status: 'UNPAID',
                    createdAt: Date.now()
                };
                invoices.push(invoice);
                await saveData(env, 'invoices', invoices);
                await saveData(env, 'device_requests', requests);
                
                return new Response(JSON.stringify({
                    ok: true,
                    status: 'WAITING_PAYMENT',
                    invoiceId: invoice.id,
                    amount: request.fee
                }), { headers: corsHeaders });
            }
            
            // ==================== ADMIN LOGIN ====================
            if (path === '/login' && request.method === 'POST') {
                const body = await request.json();
                const { username, password } = body;
                
                const admins = await getData(env, 'admins');
                const admin = admins.find(a => a.username === username);
                
                if (!admin) {
                    return new Response(JSON.stringify({ ok: false, error: 'Invalid credentials' }), { headers: corsHeaders, status: 401 });
                }
                
                // Verify password (simple hash)
                const hash = await sha256(password);
                if (admin.password !== hash) {
                    return new Response(JSON.stringify({ ok: false, error: 'Invalid credentials' }), { headers: corsHeaders, status: 401 });
                }
                
                // Generate token
                const token = generateToken();
                admin.lastLogin = Date.now();
                admin.token = token;
                await saveData(env, 'admins', admins);
                
                return new Response(JSON.stringify({
                    ok: true,
                    token: token,
                    username: admin.username,
                    role: admin.role
                }), { headers: corsHeaders });
            }
            
            // ==================== ADMIN STATS ====================
            if (path === '/admin/stats' && request.method === 'GET') {
                const auth = await checkAuth(request.headers, env);
                if (!auth) {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const companies = await getData(env, 'companies');
                const devices = await getData(env, 'devices');
                const activities = await getData(env, 'activities');
                const invoices = await getData(env, 'invoices');
                
                const now = Date.now();
                const last30Days = now - 30 * 86400000;
                
                const stats = {
                    companies: {
                        total: companies.length,
                        active: companies.filter(c => c.expiredAt > now).length,
                        byPackage: {
                            DEMO: companies.filter(c => c.package === 'DEMO').length,
                            BASIC: companies.filter(c => c.package === 'BASIC').length,
                            PRO: companies.filter(c => c.package === 'PRO').length
                        }
                    },
                    devices: {
                        total: devices.length,
                        active: devices.filter(d => d.status === 'ACTIVE').length,
                        pending: devices.filter(d => d.status === 'PENDING_APPROVAL').length,
                        suspended: devices.filter(d => d.status === 'SUSPENDED').length,
                        banned: devices.filter(d => d.status === 'BANNED').length
                    },
                    violations: {
                        total: activities.filter(a => a.type === 'VIOLATION_REPORTED').length,
                        last7Days: activities.filter(a => a.type === 'VIOLATION_REPORTED' && a.timestamp > now - 7 * 86400000).length,
                        last30Days: activities.filter(a => a.type === 'VIOLATION_REPORTED' && a.timestamp > last30Days).length
                    },
                    revenue: {
                        last30Days: invoices.filter(i => i.status === 'PAID' && i.paidAt > last30Days).reduce((sum, i) => sum + i.amount, 0)
                    }
                };
                
                return new Response(JSON.stringify(stats), { headers: corsHeaders });
            }
            
            // ==================== ADMIN COMPANIES ====================
            if (path === '/admin/companies' && request.method === 'GET') {
                const auth = await checkAuth(request.headers, env);
                if (!auth) {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const companies = await getData(env, 'companies');
                return new Response(JSON.stringify(companies), { headers: corsHeaders });
            }
            
            // ==================== ADMIN DEVICES ====================
            if (path === '/admin/devices' && request.method === 'GET') {
                const auth = await checkAuth(request.headers, env);
                if (!auth) {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const devices = await getData(env, 'devices');
                return new Response(JSON.stringify(devices), { headers: corsHeaders });
            }
            
            // ==================== ADMIN ACTIVITIES ====================
            if (path === '/admin/activity' && request.method === 'GET') {
                const auth = await checkAuth(request.headers, env);
                if (!auth) {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const activities = await getData(env, 'activities');
                return new Response(JSON.stringify(activities), { headers: corsHeaders });
            }
            
            // ==================== ADMIN INVOICES ====================
            if (path === '/admin/invoices' && request.method === 'GET') {
                const auth = await checkAuth(request.headers, env);
                if (!auth) {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const invoices = await getData(env, 'invoices');
                return new Response(JSON.stringify(invoices), { headers: corsHeaders });
            }
            
            // ==================== ADMIN DEVICE REQUESTS ====================
            if (path === '/admin/device-requests' && request.method === 'GET') {
                const auth = await checkAuth(request.headers, env);
                if (!auth) {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const urlParams = new URL(request.url).searchParams;
                const status = urlParams.get('status');
                
                let requests = await getData(env, 'device_requests');
                if (status) {
                    requests = requests.filter(r => r.status === status);
                }
                
                return new Response(JSON.stringify(requests), { headers: corsHeaders });
            }
            
            // ==================== GENERATE LICENSE ====================
            if (path === '/generate-license' && request.method === 'POST') {
                const auth = await checkAuth(request.headers, env);
                if (!auth || auth.role !== 'SUPER_ADMIN') {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const body = await request.json();
                const { companyName, pic, phone, email, address, package: pkg, customMaxDevices, notes } = body;
                
                if (!companyName || !pic || !phone || !email) {
                    return new Response(JSON.stringify({ ok: false, error: 'Missing required fields' }), { headers: corsHeaders });
                }
                
                // Generate unique license key
                const licenseKey = 'VMS-' + generateId().toUpperCase().substring(0, 16);
                
                // Set max devices based on package
                let maxDevices = customMaxDevices ? parseInt(customMaxDevices) : (pkg === 'PRO' ? 999 : (pkg === 'BASIC' ? 10 : 2));
                let expiredAt = Date.now();
                
                // Set expiry based on package
                if (pkg === 'DEMO') {
                    expiredAt += 7 * 86400000; // 7 days
                } else {
                    expiredAt += 30 * 86400000; // 30 days trial
                }
                
                const newCompany = {
                    id: generateId(),
                    companyName: companyName,
                    licenseKey: licenseKey,
                    pic: pic,
                    phone: phone,
                    email: email,
                    address: address || '',
                    package: pkg,
                    maxDevices: maxDevices,
                    currentDevices: 0,
                    expiredAt: expiredAt,
                    status: 'ACTIVE',
                    createdAt: Date.now(),
                    notes: notes || ''
                };
                
                const companies = await getData(env, 'companies');
                companies.push(newCompany);
                await saveData(env, 'companies', companies);
                
                return new Response(JSON.stringify({
                    ok: true,
                    licenseKey: licenseKey,
                    company: newCompany
                }), { headers: corsHeaders });
            }
            
            // ==================== RENEW LICENSE ====================
            if (path === '/renew-license' && request.method === 'POST') {
                const auth = await checkAuth(request.headers, env);
                if (!auth || auth.role !== 'SUPER_ADMIN') {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const body = await request.json();
                const { companyId, months, amount, paymentMethod } = body;
                
                const companies = await getData(env, 'companies');
                const company = companies.find(c => c.id === companyId);
                if (!company) {
                    return new Response(JSON.stringify({ ok: false, error: 'Company not found' }), { headers: corsHeaders });
                }
                
                // Extend expiry
                const currentExpiry = company.expiredAt;
                const newExpiry = Math.max(currentExpiry, Date.now()) + (months * 30 * 86400000);
                company.expiredAt = newExpiry;
                company.lastRenewedAt = Date.now();
                
                await saveData(env, 'companies', companies);
                
                // Create invoice
                const invoices = await getData(env, 'invoices');
                const invoice = {
                    id: generateId(),
                    companyId: company.id,
                    companyName: company.companyName,
                    type: 'RENEWAL',
                    amount: amount,
                    months: months,
                    status: paymentMethod === 'CASH' ? 'PAID' : 'UNPAID',
                    paymentMethod: paymentMethod,
                    createdAt: Date.now(),
                    paidAt: paymentMethod === 'CASH' ? Date.now() : null
                };
                invoices.push(invoice);
                await saveData(env, 'invoices', invoices);
                
                return new Response(JSON.stringify({ ok: true, company: company, invoice: invoice }), { headers: corsHeaders });
            }
            
            // ==================== UPDATE PACKAGE ====================
            if (path === '/update-package' && request.method === 'POST') {
                const auth = await checkAuth(request.headers, env);
                if (!auth || auth.role !== 'SUPER_ADMIN') {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const body = await request.json();
                const { companyId, newPackage, customMaxDevices, notes } = body;
                
                const companies = await getData(env, 'companies');
                const company = companies.find(c => c.id === companyId);
                if (!company) {
                    return new Response(JSON.stringify({ ok: false, error: 'Company not found' }), { headers: corsHeaders });
                }
                
                company.package = newPackage;
                if (customMaxDevices) {
                    company.maxDevices = parseInt(customMaxDevices);
                } else {
                    company.maxDevices = newPackage === 'PRO' ? 999 : 10;
                }
                company.packageUpdatedAt = Date.now();
                company.packageNotes = notes;
                
                await saveData(env, 'companies', companies);
                
                return new Response(JSON.stringify({ ok: true, company: company }), { headers: corsHeaders });
            }
            
            // ==================== APPROVE DEVICE (PENDING) ====================
            if (path === '/approve-device' && request.method === 'POST') {
                const auth = await checkAuth(request.headers, env);
                if (!auth) {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const body = await request.json();
                const { deviceId, approve } = body;
                
                const devices = await getData(env, 'devices');
                const device = devices.find(d => d.deviceId === deviceId);
                if (!device) {
                    return new Response(JSON.stringify({ ok: false, error: 'Device not found' }), { headers: corsHeaders });
                }
                
                device.status = approve ? 'ACTIVE' : 'REJECTED';
                if (!approve) {
                    device.deletedAt = Date.now();
                }
                
                await saveData(env, 'devices', devices);
                
                // Update company device count
                const companies = await getData(env, 'companies');
                const company = companies.find(c => c.id === device.companyId);
                if (company && approve) {
                    company.currentDevices = devices.filter(d => d.companyId === company.id && d.status === 'ACTIVE').length;
                    await saveData(env, 'companies', companies);
                }
                
                return new Response(JSON.stringify({ ok: true, device: device }), { headers: corsHeaders });
            }
            
            // ==================== DELETE DEVICE ====================
            if (path === '/delete-device' && request.method === 'POST') {
                const auth = await checkAuth(request.headers, env);
                if (!auth) {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const body = await request.json();
                const { deviceId, reason } = body;
                
                const devices = await getData(env, 'devices');
                const index = devices.findIndex(d => d.deviceId === deviceId);
                if (index === -1) {
                    return new Response(JSON.stringify({ ok: false, error: 'Device not found' }), { headers: corsHeaders });
                }
                
                devices[index].status = 'DELETED';
                devices[index].deletedAt = Date.now();
                devices[index].deleteReason = reason;
                await saveData(env, 'devices', devices);
                
                return new Response(JSON.stringify({ ok: true }), { headers: corsHeaders });
            }
            
            // ==================== DELETE COMPANY ====================
            if (path === '/delete-company' && request.method === 'POST') {
                const auth = await checkAuth(request.headers, env);
                if (!auth || auth.role !== 'SUPER_ADMIN') {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const body = await request.json();
                const { companyId, reason } = body;
                
                const companies = await getData(env, 'companies');
                const index = companies.findIndex(c => c.id === companyId);
                if (index === -1) {
                    return new Response(JSON.stringify({ ok: false, error: 'Company not found' }), { headers: corsHeaders });
                }
                
                companies.splice(index, 1);
                await saveData(env, 'companies', companies);
                
                // Delete related devices
                const devices = await getData(env, 'devices');
                const remainingDevices = devices.filter(d => d.companyId !== companyId);
                await saveData(env, 'devices', remainingDevices);
                
                return new Response(JSON.stringify({ ok: true }), { headers: corsHeaders });
            }
            
            // ==================== MARK INVOICE PAID ====================
            if (path === '/mark-invoice-paid' && request.method === 'POST') {
                const auth = await checkAuth(request.headers, env);
                if (!auth) {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const body = await request.json();
                const { invoiceId, paymentMethod } = body;
                
                const invoices = await getData(env, 'invoices');
                const invoice = invoices.find(i => i.id === invoiceId);
                if (!invoice) {
                    return new Response(JSON.stringify({ ok: false, error: 'Invoice not found' }), { headers: corsHeaders });
                }
                
                invoice.status = 'PAID';
                invoice.paidAt = Date.now();
                invoice.paymentMethod = paymentMethod;
                await saveData(env, 'invoices', invoices);
                
                // Jika ini adalah device addition request, activate device
                if (invoice.type === 'DEVICE_ADDITION' && invoice.requestId) {
                    const requests = await getData(env, 'device_requests');
                    const request = requests.find(r => r.id === invoice.requestId);
                    if (request && request.status === 'WAITING_PAYMENT') {
                        request.status = 'PAID';
                        request.paidAt = Date.now();
                        await saveData(env, 'device_requests', requests);
                        
                        // Tambahkan device ke company
                        const companies = await getData(env, 'companies');
                        const company = companies.find(c => c.id === request.companyId);
                        if (company) {
                            const devices = await getData(env, 'devices');
                            const newDevice = {
                                deviceId: 'dev_' + generateId(),
                                deviceName: request.deviceName,
                                licenseKey: request.licenseKey,
                                companyId: company.id,
                                companyName: company.companyName,
                                status: 'ACTIVE',
                                firstSeen: Date.now(),
                                lastSeen: Date.now(),
                                violations: [],
                                sessions: []
                            };
                            devices.push(newDevice);
                            await saveData(env, 'devices', devices);
                            
                            company.currentDevices = devices.filter(d => d.companyId === company.id && d.status === 'ACTIVE').length;
                            await saveData(env, 'companies', companies);
                        }
                    }
                }
                
                return new Response(JSON.stringify({ ok: true, invoice: invoice }), { headers: corsHeaders });
            }
            
            // ==================== ADMIN USERS ====================
            if (path === '/admin/users' && request.method === 'GET') {
                const auth = await checkAuth(request.headers, env);
                if (!auth || auth.role !== 'SUPER_ADMIN') {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const admins = await getData(env, 'admins');
                const safeAdmins = admins.map(a => ({ username: a.username, role: a.role, lastLogin: a.lastLogin }));
                return new Response(JSON.stringify(safeAdmins), { headers: corsHeaders });
            }
            
            if (path === '/admin/add-user' && request.method === 'POST') {
                const auth = await checkAuth(request.headers, env);
                if (!auth || auth.role !== 'SUPER_ADMIN') {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const body = await request.json();
                const { username, password, role } = body;
                
                if (!username || !password) {
                    return new Response(JSON.stringify({ ok: false, error: 'Username and password required' }), { headers: corsHeaders });
                }
                
                const admins = await getData(env, 'admins');
                if (admins.find(a => a.username === username)) {
                    return new Response(JSON.stringify({ ok: false, error: 'Username already exists' }), { headers: corsHeaders });
                }
                
                const hash = await sha256(password);
                admins.push({
                    username: username,
                    password: hash,
                    role: role || 'ADMIN',
                    createdAt: Date.now()
                });
                await saveData(env, 'admins', admins);
                
                return new Response(JSON.stringify({ ok: true }), { headers: corsHeaders });
            }
            
            if (path === '/admin/delete-user' && request.method === 'POST') {
                const auth = await checkAuth(request.headers, env);
                if (!auth || auth.role !== 'SUPER_ADMIN') {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const body = await request.json();
                const { username } = body;
                
                if (username === 'admin') {
                    return new Response(JSON.stringify({ ok: false, error: 'Cannot delete default admin' }), { headers: corsHeaders });
                }
                
                const admins = await getData(env, 'admins');
                const filtered = admins.filter(a => a.username !== username);
                await saveData(env, 'admins', filtered);
                
                return new Response(JSON.stringify({ ok: true }), { headers: corsHeaders });
            }
            
            // ==================== ADMIN SETTINGS ====================
            if (path === '/admin/settings' && request.method === 'POST') {
                const auth = await checkAuth(request.headers, env);
                if (!auth || auth.role !== 'SUPER_ADMIN') {
                    return new Response(JSON.stringify({ ok: false, error: 'Unauthorized' }), { headers: corsHeaders, status: 401 });
                }
                
                const body = await request.json();
                await saveData(env, 'settings', body);
                
                return new Response(JSON.stringify({ ok: true }), { headers: corsHeaders });
            }
            
            // ==================== SAVE DATA (FROM FIELD DEVICE) ====================
            if (path === '/save' && request.method === 'POST') {
                const body = await request.json();
                
                // Simpan data dari field device
                if (body.visitors && Object.keys(body.visitors).length > 0) {
                    let allVisitors = await getData(env, 'visitors');
                    for (const [key, value] of Object.entries(body.visitors)) {
                        allVisitors[key] = { ...allVisitors[key], ...value, lastSync: Date.now() };
                    }
                    await saveData(env, 'visitors', allVisitors);
                }
                
                if (body.logs && body.logs.length > 0) {
                    let allLogs = await getData(env, 'logs');
                    allLogs = [...body.logs, ...allLogs];
                    await saveData(env, 'logs', allLogs.slice(0, 10000));
                }
                
                // Simpan anti nakal report
                if (body.anti) {
                    let reports = await getData(env, 'anti_nakal_reports');
                    reports.unshift({
                        ...body.anti,
                        deviceId: body.deviceId,
                        site: body.site,
                        timestamp: Date.now()
                    });
                    await saveData(env, 'anti_nakal_reports', reports.slice(0, 5000));
                }
                
                return new Response(JSON.stringify({ ok: true }), { headers: corsHeaders });
            }
            
            // ==================== SYNC USERS ====================
            if (path === '/sync-users' && request.method === 'POST') {
                const body = await request.json();
                if (body.users && Array.isArray(body.users)) {
                    // Merge users from client
                    let serverUsers = await getData(env, 'users_from_clients');
                    for (const user of body.users) {
                        const existing = serverUsers.find(u => u.username === user.username);
                        if (!existing) {
                            serverUsers.push(user);
                        }
                    }
                    await saveData(env, 'users_from_clients', serverUsers);
                    
                    // Return merged users
                    return new Response(JSON.stringify({ ok: true, users: serverUsers }), { headers: corsHeaders });
                }
                return new Response(JSON.stringify({ ok: true }), { headers: corsHeaders });
            }
            
            // ==================== FORCE INIT ====================
            if (path === '/force-init' && request.method === 'POST') {
                // Initialize default admin if not exists
                const admins = await getData(env, 'admins');
                if (admins.length === 0) {
                    const defaultHash = await sha256('123456');
                    admins.push({
                        username: 'admin',
                        password: defaultHash,
                        role: 'SUPER_ADMIN',
                        createdAt: Date.now()
                    });
                    await saveData(env, 'admins', admins);
                }
                
                // Initialize settings if not exists
                const settings = await getData(env, 'settings');
                if (Object.keys(settings).length === 0) {
                    await saveData(env, 'settings', {
                        pricing: {
                            BASIC: { price: 500000, maxDevices: 10, extraDeviceFee: 50000 },
                            PRO: { price: 2000000, maxDevices: 999, extraDeviceFee: 0 }
                        },
                        general: { tax: 11 }
                    });
                }
                
                return new Response(JSON.stringify({ ok: true }), { headers: corsHeaders });
            }
            
            // ==================== ROOT / HEALTH CHECK ====================
            if (path === '/') {
                return new Response(JSON.stringify({ 
                    status: 'online', 
                    version: 'v3.0 Enterprise',
                    timestamp: Date.now()
                }), { headers: corsHeaders });
            }
            
            // ==================== DEFAULT 404 ====================
            return new Response(JSON.stringify({ ok: false, error: 'Endpoint not found' }), { 
                status: 404, 
                headers: corsHeaders 
            });
            
        } catch (error) {
            console.error('Worker error:', error);
            return new Response(JSON.stringify({ ok: false, error: error.message }), { 
                status: 500, 
                headers: corsHeaders 
            });
        }
    }
};

// ==================== HELPER FUNCTIONS ====================

async function getData(env, key) {
    try {
        const value = await env.VMS_STORAGE.get(key);
        return value ? JSON.parse(value) : (Array.isArray([]) ? [] : {});
    } catch (e) {
        // Fallback untuk development tanpa KV
        console.warn(`KV get ${key} failed:`, e);
        const defaultValues = {
            companies: [],
            devices: [],
            activities: [],
            invoices: [],
            device_requests: [],
            admins: [],
            visitors: {},
            logs: [],
            anti_nakal_reports: [],
            users_from_clients: [],
            settings: {}
        };
        return defaultValues[key] || (key === 'visitors' ? {} : []);
    }
}

async function saveData(env, key, data) {
    try {
        await env.VMS_STORAGE.put(key, JSON.stringify(data));
    } catch (e) {
        console.warn(`KV save ${key} failed:`, e);
    }
}

async function checkAuth(headers, env) {
    const token = headers.get('x-token');
    if (!token) return null;
    
    const admins = await getData(env, 'admins');
    const admin = admins.find(a => a.token === token);
    
    if (admin && admin.lastLogin && (Date.now() - admin.lastLogin) < 24 * 3600000) {
        return { username: admin.username, role: admin.role };
    }
    
    return null;
}

function generateId() {
    return Date.now().toString(36) + Math.random().toString(36).substring(2, 10);
}

function generateToken() {
    return 'token_' + Date.now().toString(36) + '_' + Math.random().toString(36).substring(2, 15);
}

async function sha256(message) {
    const msgBuffer = new TextEncoder().encode(message);
    const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
    return hashHex;
}