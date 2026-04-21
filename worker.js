// ============================================================
// VMS COMMAND CENTER - FULL WORKER v7.0
// 100% COMPATIBLE DENGAN SEMUA HTML
// ============================================================

export default {
    async fetch(request, env) {
        const url = new URL(request.url);
        const method = request.method;

        const KEYS = {
            USERS: 'vms_users_v7',
            COMPANIES: 'vms_companies_v7',
            DEVICES: 'vms_devices_v7',
            LICENSES: 'vms_licenses_v7',
            TOKENS: 'vms_tokens_v7',
            ACTIVITY: 'vms_activity_v7',
            INVOICES: 'vms_invoices_v7',
            SETTINGS: 'vms_settings_v7',
            DEVICE_REQUESTS: 'vms_device_requests_v7'
        };

        const CORS = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, x-token',
            'Content-Type': 'application/json'
        };

        if (method === 'OPTIONS') {
            return new Response(null, { headers: CORS });
        }

        const kv = async () => {
            if (!env.VMS_STORAGE) throw new Error("KV storage not bound");
            return env.VMS_STORAGE;
        };

        const sha256 = async (str) => {
            const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(str));
            return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
        };

        const verifyAdmin = async () => {
            const token = request.headers.get("x-token");
            if (!token) return null;
            try {
                const k = await kv();
                const raw = await k.get(`${KEYS.TOKENS}_${token}`);
                if (!raw) return null;
                const data = JSON.parse(raw);
                if (Date.now() > data.exp) {
                    await k.delete(`${KEYS.TOKENS}_${token}`);
                    return null;
                }
                return data;
            } catch { return null; }
        };

        const res = (data, status = 200) => new Response(JSON.stringify(data), { status, headers: CORS });
        const err = (msg, status = 400) => res({ error: msg, ok: false }, status);

        // ==================== ROOT ====================
        if (url.pathname === '/' && method === 'GET') {
            return res({ status: 'online', version: 'v7.0-COMPLETE', name: 'VMS Command Center' });
        }

        // ==================== FORCE INIT ====================
        if (url.pathname === '/force-init' && method === 'POST') {
            const k = await kv();
            let users = JSON.parse(await k.get(KEYS.USERS) || '[]');
            if (users.length === 0) {
                users = [{
                    id: 'admin_1',
                    username: 'admin',
                    password: await sha256('VMSAdmin2024!'),
                    role: 'SUPER_ADMIN',
                    createdAt: Date.now()
                }];
                await k.put(KEYS.USERS, JSON.stringify(users));
            }
            let settings = JSON.parse(await k.get(KEYS.SETTINGS) || '{}');
            if (Object.keys(settings).length === 0) {
                settings = {
                    pricing: {
                        DEMO: { price: 0, duration: 7, maxDevices: 2, extraDeviceFee: 0 },
                        BASIC: { price: 500000, duration: 30, maxDevices: 10, extraDeviceFee: 50000 },
                        PRO: { price: 2000000, duration: 30, maxDevices: 999, extraDeviceFee: 0 }
                    },
                    currency: 'IDR',
                    tax: 11,
                    createdAt: Date.now()
                };
                await k.put(KEYS.SETTINGS, JSON.stringify(settings));
            }
            return res({ ok: true, message: "System initialized" });
        }

        // ==================== LOGIN ====================
        if (url.pathname === '/login' && method === 'POST') {
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { username, password } = body;
            if (!username || !password) return err("Username and password required");
            const k = await kv();
            let users = JSON.parse(await k.get(KEYS.USERS) || '[]');
            if (users.length === 0) {
                users = [{
                    id: 'admin_1',
                    username: 'admin',
                    password: await sha256('VMSAdmin2024!'),
                    role: 'SUPER_ADMIN',
                    createdAt: Date.now()
                }];
                await k.put(KEYS.USERS, JSON.stringify(users));
            }
            const hashed = await sha256(password);
            const user = users.find(u => u.username === username && u.password === hashed);
            if (!user) return err("Invalid credentials", 401);
            const token = await sha256(username + Date.now() + Math.random());
            const exp = Date.now() + 24 * 60 * 60 * 1000;
            await k.put(`${KEYS.TOKENS}_${token}`, JSON.stringify({
                username: user.username,
                role: user.role,
                userId: user.id,
                exp
            }));
            return res({ ok: true, token, role: user.role, username: user.username, expiresIn: 24 * 60 * 60 * 1000 });
        }

        // ==================== LOGOUT ====================
        if (url.pathname === '/logout' && method === 'POST') {
            const token = request.headers.get("x-token");
            if (token) {
                const k = await kv();
                await k.delete(`${KEYS.TOKENS}_${token}`);
            }
            return res({ ok: true });
        }

        // ==================== SYNC USERS ====================
        if (url.pathname === '/sync-users' && method === 'POST') {
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { users } = body;
            if (users && Array.isArray(users)) {
                const k = await kv();
                await k.put(KEYS.USERS, JSON.stringify(users));
                return res({ ok: true, users });
            }
            return err("Invalid users data");
        }

        // ==================== SAVE (GPS/ANTI NAKAL) ====================
        if (url.pathname === '/save' && method === 'POST') {
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { deviceId, meta, anti } = body;
            if (!deviceId) return err("Device ID required");
            const k = await kv();
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let device = devices.find(d => d.deviceId === deviceId);
            if (device) {
                if (meta) device.meta = { ...device.meta, ...meta };
                if (anti) {
                    device.lastLocation = { lat: anti.lat, lng: anti.lng, acc: anti.acc, ts: Date.now() };
                    if (!device.violations) device.violations = [];
                    device.violations.unshift({ type: 'GPS_UPDATE', data: anti, timestamp: Date.now() });
                }
                device.lastSeen = Date.now();
                await k.put(KEYS.DEVICES, JSON.stringify(devices));
            }
            return res({ ok: true });
        }

        // ==================== VALIDATE LICENSE ====================
        if (url.pathname === '/validate-license' && method === 'POST') {
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { licenseKey, deviceId, deviceName, meta } = body;
            if (!licenseKey || !deviceId) return err("License key and device ID required");
            const k = await kv();
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            const license = licenses[licenseKey];
            if (!license) return err("Invalid license key", 403);
            const company = companies.find(c => c.id === license.companyId);
            if (!company) return err("Company not found", 403);
            if (license.expiredAt < Date.now()) return err("License expired. Please renew.", 403);
            if (company.status !== 'ACTIVE') return err(`Company status: ${company.status}`, 403);
            let device = devices.find(d => d.deviceId === deviceId);
            if (!device) {
                const companyDevices = devices.filter(d => d.companyId === company.id);
                if (companyDevices.length >= license.maxDevices) {
                    return err(`Max devices reached (${license.maxDevices})`, 403);
                }
                device = {
                    deviceId, deviceName: deviceName || deviceId,
                    companyId: company.id, companyName: company.companyName, licenseKey,
                    status: 'PENDING_APPROVAL', firstSeen: Date.now(), lastSeen: Date.now(),
                    meta: meta || {}, violations: [], checkins: []
                };
                devices.push(device);
                await k.put(KEYS.DEVICES, JSON.stringify(devices));
                let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
                activity.unshift({ id: 'act_' + Date.now(), type: 'DEVICE_REGISTRATION_REQUEST', companyId: company.id, companyName: company.companyName, deviceId, deviceName: deviceName || deviceId, timestamp: Date.now() });
                await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            } else {
                device.lastSeen = Date.now();
                if (meta) device.meta = { ...device.meta, ...meta };
                await k.put(KEYS.DEVICES, JSON.stringify(devices));
            }
            company.currentDevices = devices.filter(d => d.companyId === company.id && d.status === 'ACTIVE').length;
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            if (!license.devices.includes(deviceId)) {
                license.devices.push(deviceId);
                await k.put(KEYS.LICENSES, JSON.stringify(licenses));
            }
            return res({
                ok: true, status: device.status,
                company: {
                    id: company.id, name: company.companyName, package: company.package,
                    maxDevices: license.maxDevices, currentDevices: company.currentDevices,
                    expiredAt: license.expiredAt
                }
            });
        }

        // ==================== REQUEST APPROVAL ====================
        if (url.pathname === '/request-approval' && method === 'POST') {
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { licenseKey, deviceId, deviceName, reason } = body;
            const k = await kv();
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            const device = devices.find(d => d.deviceId === deviceId);
            if (!device) return err("Device not found");
            device.status = 'PENDING_APPROVAL';
            device.approvalRequest = { requestedAt: Date.now(), reason: reason || 'New device registration', deviceName: deviceName || device.deviceName };
            activity.unshift({ id: 'act_' + Date.now(), type: 'DEVICE_APPROVAL_REQUEST', companyId: device.companyId, companyName: device.companyName, deviceId, deviceName: device.deviceName, timestamp: Date.now() });
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            return res({ ok: true, message: "Approval request sent to admin" });
        }

        // ==================== APPROVE DEVICE ====================
        if (url.pathname === '/approve-device' && method === 'POST') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { deviceId, approve, notes } = body;
            const k = await kv();
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            const device = devices.find(d => d.deviceId === deviceId);
            if (!device) return err("Device not found");
            device.status = approve ? 'ACTIVE' : 'REJECTED';
            device.approvedAt = Date.now();
            device.approvedBy = auth.username;
            device.approvalNotes = notes || '';
            if (approve) {
                const company = companies.find(c => c.id === device.companyId);
                if (company) {
                    company.currentDevices = devices.filter(d => d.companyId === company.id && d.status === 'ACTIVE').length;
                    await k.put(KEYS.COMPANIES, JSON.stringify(companies));
                }
            }
            activity.unshift({ id: 'act_' + Date.now(), type: approve ? 'DEVICE_APPROVED' : 'DEVICE_REJECTED', companyId: device.companyId, companyName: device.companyName, deviceId, deviceName: device.deviceName, approvedBy: auth.username, timestamp: Date.now() });
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            return res({ ok: true, message: `Device ${approve ? 'approved' : 'rejected'}` });
        }

        // ==================== DELETE DEVICE ====================
        if (url.pathname === '/delete-device' && method === 'POST') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { deviceId, reason } = body;
            const k = await kv();
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            const deviceIndex = devices.findIndex(d => d.deviceId === deviceId);
            if (deviceIndex === -1) return err("Device not found");
            const device = devices[deviceIndex];
            if (licenses[device.licenseKey]) {
                licenses[device.licenseKey].devices = licenses[device.licenseKey].devices.filter(d => d !== deviceId);
                await k.put(KEYS.LICENSES, JSON.stringify(licenses));
            }
            const company = companies.find(c => c.id === device.companyId);
            if (company) {
                company.currentDevices = devices.filter(d => d.companyId === company.id && d.deviceId !== deviceId && d.status === 'ACTIVE').length;
                await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            }
            activity.unshift({ id: 'act_' + Date.now(), type: 'DEVICE_DELETED', companyId: device.companyId, companyName: device.companyName, deviceId, deviceName: device.deviceName, deletedBy: auth.username, reason: reason || 'Deleted by admin', timestamp: Date.now() });
            devices.splice(deviceIndex, 1);
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            return res({ ok: true, message: "Device deleted" });
        }

        // ==================== DELETE COMPANY ====================
        if (url.pathname === '/delete-company' && method === 'POST') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { companyId, reason } = body;
            const k = await kv();
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            const companyIndex = companies.findIndex(c => c.id === companyId);
            if (companyIndex === -1) return err("Company not found");
            const company = companies[companyIndex];
            activity.unshift({ id: 'act_' + Date.now(), type: 'COMPANY_DELETED', companyId: company.id, companyName: company.companyName, deletedBy: auth.username, reason: reason || 'No reason provided', timestamp: Date.now() });
            devices = devices.filter(d => d.companyId !== companyId);
            delete licenses[company.licenseKey];
            companies.splice(companyIndex, 1);
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.LICENSES, JSON.stringify(licenses));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            return res({ ok: true, message: `Company deleted` });
        }

        // ==================== GENERATE LICENSE ====================
        if (url.pathname === '/generate-license' && method === 'POST') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { companyName, pic, phone, email, address, package: pkg, customMaxDevices, notes } = body;
            if (!companyName || !pic || !phone || !email || !pkg) return err("Missing required fields");
            const k = await kv();
            const settings = JSON.parse(await k.get(KEYS.SETTINGS) || '{}');
            const pricing = settings.pricing || {};
            const pkgConfig = pricing[pkg];
            if (!pkgConfig) return err("Invalid package");
            const licenseKey = 'VMS-' + Date.now() + '-' + Math.random().toString(36).substring(2, 15).toUpperCase();
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            if (companies.find(c => c.companyName.toLowerCase() === companyName.toLowerCase())) return err("Company already exists");
            const maxDevices = customMaxDevices || pkgConfig.maxDevices;
            const expiredAt = Date.now() + (pkgConfig.duration || 30) * 24 * 60 * 60 * 1000;
            const newCompany = {
                id: 'comp_' + Date.now() + '_' + Math.random().toString(36).substring(2, 8),
                companyName, pic, phone, email, address: address || '', package: pkg,
                licenseKey, maxDevices, currentDevices: 0, status: 'ACTIVE',
                createdAt: Date.now(), expiredAt, lastPayment: Date.now(),
                paymentHistory: [], notes: notes || '',
                metadata: { totalViolations: 0, totalCheckins: 0, lastActive: null, devices: [] }
            };
            companies.push(newCompany);
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            licenses[licenseKey] = {
                companyId: newCompany.id, companyName, package: pkg, maxDevices,
                devices: [], createdAt: Date.now(), expiredAt, status: 'ACTIVE', lastRenewed: Date.now()
            };
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.LICENSES, JSON.stringify(licenses));
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            activity.unshift({ id: 'act_' + Date.now(), type: 'LICENSE_CREATED', companyId: newCompany.id, companyName, details: `License created for ${companyName}`, admin: auth.username, timestamp: Date.now() });
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            return res({ ok: true, licenseKey, company: newCompany });
        }

        // ==================== RENEW LICENSE ====================
        if (url.pathname === '/renew-license' && method === 'POST') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { companyId, months, amount, paymentMethod } = body;
            const k = await kv();
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            let invoices = JSON.parse(await k.get(KEYS.INVOICES) || '[]');
            const company = companies.find(c => c.id === companyId);
            if (!company) return err("Company not found");
            const newExpiry = Math.max(Date.now(), company.expiredAt) + months * 30 * 24 * 60 * 60 * 1000;
            company.expiredAt = newExpiry;
            company.status = 'ACTIVE';
            company.lastPayment = Date.now();
            company.paymentHistory = company.paymentHistory || [];
            company.paymentHistory.unshift({ date: Date.now(), months, amount: amount || 0, method: paymentMethod || 'MANUAL', invoiceId: 'inv_' + Date.now() });
            if (licenses[company.licenseKey]) {
                licenses[company.licenseKey].expiredAt = newExpiry;
                licenses[company.licenseKey].status = 'ACTIVE';
                licenses[company.licenseKey].lastRenewed = Date.now();
            }
            const invoice = {
                id: 'inv_' + Date.now(), companyId: company.id, companyName: company.companyName,
                amount: amount || 0, months, status: 'PAID', createdAt: Date.now(),
                paidAt: Date.now(), paymentMethod: paymentMethod || 'MANUAL', admin: auth.username
            };
            invoices.unshift(invoice);
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            for (const device of devices) {
                if (device.companyId === companyId && (device.status === 'SUSPENDED' || device.status === 'BANNED')) {
                    device.status = 'ACTIVE';
                }
            }
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.LICENSES, JSON.stringify(licenses));
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.INVOICES, JSON.stringify(invoices.slice(0, 500)));
            return res({ ok: true, message: `License renewed for ${months} months`, newExpiry });
        }

        // ==================== UPDATE PACKAGE ====================
        if (url.pathname === '/update-package' && method === 'POST') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { companyId, newPackage, customMaxDevices, notes } = body;
            const k = await kv();
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            const company = companies.find(c => c.id === companyId);
            if (!company) return err("Company not found");
            const settings = JSON.parse(await k.get(KEYS.SETTINGS) || '{}');
            const pricing = settings.pricing || {};
            const pkgConfig = pricing[newPackage];
            if (!pkgConfig) return err("Invalid package");
            const maxDevices = customMaxDevices || pkgConfig.maxDevices;
            company.package = newPackage;
            company.maxDevices = maxDevices;
            company.packageUpdatedAt = Date.now();
            company.packageUpdatedBy = auth.username;
            company.packageNotes = notes || '';
            if (licenses[company.licenseKey]) {
                licenses[company.licenseKey].package = newPackage;
                licenses[company.licenseKey].maxDevices = maxDevices;
            }
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.LICENSES, JSON.stringify(licenses));
            return res({ ok: true, message: `Package updated to ${newPackage}` });
        }

        // ==================== MARK INVOICE PAID ====================
        if (url.pathname === '/mark-invoice-paid' && method === 'POST') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { invoiceId, paymentMethod, notes } = body;
            const k = await kv();
            let invoices = JSON.parse(await k.get(KEYS.INVOICES) || '[]');
            const invoice = invoices.find(i => i.id === invoiceId);
            if (!invoice) return err("Invoice not found");
            invoice.status = 'PAID';
            invoice.paidAt = Date.now();
            invoice.paidBy = auth.username;
            invoice.paymentMethod = paymentMethod || 'MANUAL';
            invoice.paymentNotes = notes || '';
            if (invoice.months) {
                let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
                let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
                const company = companies.find(c => c.id === invoice.companyId);
                if (company) {
                    const newExpiry = Math.max(Date.now(), company.expiredAt) + invoice.months * 30 * 24 * 60 * 60 * 1000;
                    company.expiredAt = newExpiry;
                    company.status = 'ACTIVE';
                    company.lastPayment = Date.now();
                    if (licenses[company.licenseKey]) {
                        licenses[company.licenseKey].expiredAt = newExpiry;
                        licenses[company.licenseKey].status = 'ACTIVE';
                    }
                    await k.put(KEYS.COMPANIES, JSON.stringify(companies));
                    await k.put(KEYS.LICENSES, JSON.stringify(licenses));
                }
            }
            await k.put(KEYS.INVOICES, JSON.stringify(invoices));
            return res({ ok: true, message: "Invoice marked as paid" });
        }

        // ==================== CHECK-IN/OUT ====================
        if (url.pathname === '/checkin' && method === 'POST') {
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { licenseKey, deviceId, action, location } = body;
            if (!licenseKey || !deviceId) return err("License key and device ID required");
            if (!['IN', 'OUT'].includes(action)) return err("Action must be IN or OUT");
            const k = await kv();
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            const device = devices.find(d => d.deviceId === deviceId);
            if (!device) return err("Device not found");
            if (device.status !== 'ACTIVE') return err(`Device status: ${device.status}`);
            const checkinRecord = { id: 'chk_' + Date.now(), action, timestamp: Date.now(), location: location || null, deviceId, deviceName: device.deviceName };
            if (!device.checkins) device.checkins = [];
            device.checkins.unshift(checkinRecord);
            device.lastCheckin = Date.now();
            device.lastAction = action;
            const company = companies.find(c => c.id === device.companyId);
            if (company) company.metadata.totalCheckins = (company.metadata.totalCheckins || 0) + 1;
            activity.unshift({ id: 'act_' + Date.now(), type: `CHECK_${action}`, companyId: device.companyId, companyName: device.companyName, deviceId, deviceName: device.deviceName, location, timestamp: Date.now() });
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            return res({ ok: true, message: `Check-${action} recorded successfully` });
        }

        // ==================== REPORT VIOLATION ====================
        if (url.pathname === '/report-violation' && method === 'POST') {
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { licenseKey, deviceId, violationType, details, location } = body;
            const k = await kv();
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            const device = devices.find(d => d.deviceId === deviceId);
            if (!device) return err("Device not found");
            const violation = { id: 'viol_' + Date.now(), type: violationType, details: details || '', location: location || null, timestamp: Date.now(), deviceId, deviceName: device.deviceName };
            if (!device.violations) device.violations = [];
            device.violations.unshift(violation);
            const recentViolations = device.violations.filter(v => v.timestamp > Date.now() - 7 * 24 * 60 * 60 * 1000);
            if (recentViolations.length >= 5 && device.status === 'ACTIVE') {
                device.status = 'SUSPENDED';
                device.suspendedAt = Date.now();
                device.suspendReason = `Auto-suspended due to ${recentViolations.length} violations`;
            }
            const company = companies.find(c => c.id === device.companyId);
            if (company) company.metadata.totalViolations = (company.metadata.totalViolations || 0) + 1;
            activity.unshift({ id: 'act_' + Date.now(), type: 'VIOLATION_REPORTED', companyId: device.companyId, companyName: device.companyName, deviceId, deviceName: device.deviceName, violationType, details, timestamp: Date.now() });
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            return res({ ok: true, deviceStatus: device.status, violationCount: recentViolations.length });
        }

        // ==================== REQUEST ADDITIONAL DEVICE ====================
        if (url.pathname === '/request-device' && method === 'POST') {
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { licenseKey, deviceName, reason } = body;
            if (!licenseKey || !deviceName) return err("License key and device name required");
            const k = await kv();
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let deviceRequests = JSON.parse(await k.get(KEYS.DEVICE_REQUESTS) || '[]');
            const license = licenses[licenseKey];
            if (!license) return err("Invalid license key", 403);
            const company = companies.find(c => c.id === license.companyId);
            if (!company) return err("Company not found", 403);
            const settings = JSON.parse(await k.get(KEYS.SETTINGS) || '{}');
            const pricing = settings.pricing || {};
            const pkgConfig = pricing[company.package];
            const extraFee = pkgConfig?.extraDeviceFee || 50000;
            const requestId = 'req_' + Date.now() + '_' + Math.random().toString(36).substring(2, 8);
            const newRequest = {
                id: requestId, companyId: company.id, companyName: company.companyName,
                licenseKey, deviceName, reason: reason || 'Additional device request',
                status: 'PENDING', fee: extraFee, requestedAt: Date.now()
            };
            deviceRequests.unshift(newRequest);
            await k.put(KEYS.DEVICE_REQUESTS, JSON.stringify(deviceRequests.slice(0, 500)));
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            activity.unshift({ id: 'act_' + Date.now(), type: 'DEVICE_REQUEST_SUBMITTED', companyId: company.id, companyName: company.companyName, deviceName, fee: extraFee, requestId, timestamp: Date.now() });
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            return res({ ok: true, requestId, fee: extraFee, message: `Request submitted. Fee: Rp ${extraFee.toLocaleString()}` });
        }

        // ==================== GET DEVICE REQUESTS ====================
        if (url.pathname === '/admin/device-requests' && method === 'GET') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            const k = await kv();
            const requests = JSON.parse(await k.get(KEYS.DEVICE_REQUESTS) || '[]');
            const status = url.searchParams.get('status');
            let filtered = requests;
            if (status) filtered = requests.filter(r => r.status === status);
            return res(filtered);
        }

        // ==================== APPROVE DEVICE REQUEST ====================
        if (url.pathname === '/approve-device-request' && method === 'POST') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { requestId, approve, notes } = body;
            const k = await kv();
            let deviceRequests = JSON.parse(await k.get(KEYS.DEVICE_REQUESTS) || '[]');
            let invoices = JSON.parse(await k.get(KEYS.INVOICES) || '[]');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            const reqIndex = deviceRequests.findIndex(r => r.id === requestId);
            if (reqIndex === -1) return err("Request not found");
            const request = deviceRequests[reqIndex];
            if (approve) {
                const invoice = {
                    id: 'INV-' + Date.now() + '-' + Math.random().toString(36).substring(2, 6).toUpperCase(),
                    companyId: request.companyId, companyName: request.companyName,
                    type: 'ADDITIONAL_DEVICE', deviceName: request.deviceName,
                    amount: request.fee, status: 'UNPAID', createdAt: Date.now(),
                    dueDate: Date.now() + 7 * 24 * 60 * 60 * 1000, requestId: request.id,
                    generatedBy: auth.username
                };
                invoices.unshift(invoice);
                request.status = 'WAITING_PAYMENT';
                request.invoiceId = invoice.id;
                request.processedAt = Date.now();
                request.processedBy = auth.username;
                request.approvalNotes = notes || '';
                activity.unshift({ id: 'act_' + Date.now(), type: 'DEVICE_REQUEST_APPROVED', companyId: request.companyId, companyName: request.companyName, deviceName: request.deviceName, invoiceId: invoice.id, amount: request.fee, admin: auth.username, timestamp: Date.now() });
                await k.put(KEYS.INVOICES, JSON.stringify(invoices.slice(0, 500)));
            } else {
                request.status = 'REJECTED';
                request.processedAt = Date.now();
                request.processedBy = auth.username;
                request.rejectionNotes = notes || '';
                activity.unshift({ id: 'act_' + Date.now(), type: 'DEVICE_REQUEST_REJECTED', companyId: request.companyId, companyName: request.companyName, deviceName: request.deviceName, admin: auth.username, reason: notes || '', timestamp: Date.now() });
            }
            deviceRequests[reqIndex] = request;
            await k.put(KEYS.DEVICE_REQUESTS, JSON.stringify(deviceRequests));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            return res({ ok: true, status: approve ? 'WAITING_PAYMENT' : 'REJECTED', invoiceId: approve ? invoice.id : null, amount: approve ? request.fee : null });
        }

        // ==================== GET INVOICES ====================
        if (url.pathname === '/admin/invoices' && method === 'GET') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            const k = await kv();
            const invoices = JSON.parse(await k.get(KEYS.INVOICES) || '[]');
            const status = url.searchParams.get('status');
            let filtered = invoices;
            if (status) filtered = invoices.filter(i => i.status === status);
            return res(filtered);
        }

        // ==================== GET COMPANIES ====================
        if (url.pathname === '/admin/companies' && method === 'GET') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            const k = await kv();
            const companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            const devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            const enriched = companies.map(c => ({
                ...c,
                devices: devices.filter(d => d.companyId === c.id),
                activeDevices: devices.filter(d => d.companyId === c.id && d.status === 'ACTIVE').length,
                pendingDevices: devices.filter(d => d.companyId === c.id && d.status === 'PENDING_APPROVAL').length,
                suspendedDevices: devices.filter(d => d.companyId === c.id && d.status === 'SUSPENDED').length
            }));
            return res(enriched);
        }

        // ==================== GET DEVICES ====================
        if (url.pathname === '/admin/devices' && method === 'GET') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            const k = await kv();
            const devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            const companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            const enriched = devices.map(d => ({ ...d, companyInfo: companies.find(c => c.id === d.companyId) }));
            return res(enriched);
        }

        // ==================== GET ACTIVITY ====================
        if (url.pathname === '/admin/activity' && method === 'GET') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            const k = await kv();
            const activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            const limit = parseInt(url.searchParams.get('limit') || '200');
            const type = url.searchParams.get('type');
            let filtered = activity;
            if (type) filtered = activity.filter(a => a.type === type);
            return res(filtered.slice(0, limit));
        }

        // ==================== GET STATS ====================
        if (url.pathname === '/admin/stats' && method === 'GET') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            const k = await kv();
            const companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            const devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            const activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            const invoices = JSON.parse(await k.get(KEYS.INVOICES) || '[]');
            const now = Date.now();
            const thirtyDaysAgo = now - 30 * 24 * 60 * 60 * 1000;
            const stats = {
                companies: {
                    total: companies.length,
                    active: companies.filter(c => c.status === 'ACTIVE' && c.expiredAt > now).length,
                    expired: companies.filter(c => c.expiredAt < now).length,
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
                    total: activity.filter(a => a.type === 'VIOLATION_REPORTED').length,
                    last7Days: activity.filter(a => a.type === 'VIOLATION_REPORTED' && a.timestamp > now - 7*24*60*60*1000).length,
                    last30Days: activity.filter(a => a.type === 'VIOLATION_REPORTED' && a.timestamp > thirtyDaysAgo).length
                },
                checkins: {
                    total: activity.filter(a => a.type === 'CHECK_IN' || a.type === 'CHECK_OUT').length,
                    last7Days: activity.filter(a => (a.type === 'CHECK_IN' || a.type === 'CHECK_OUT') && a.timestamp > now - 7*24*60*60*1000).length,
                    last30Days: activity.filter(a => (a.type === 'CHECK_IN' || a.type === 'CHECK_OUT') && a.timestamp > thirtyDaysAgo).length
                },
                revenue: {
                    total: invoices.reduce((sum, inv) => sum + (inv.amount || 0), 0),
                    last30Days: invoices.filter(inv => inv.createdAt > thirtyDaysAgo).reduce((sum, inv) => sum + (inv.amount || 0), 0)
                },
                recentActivity: activity.slice(0, 20),
                timestamp: now
            };
            return res(stats);
        }

        // ==================== GET COMPANY DETAILS ====================
        if (url.pathname.startsWith('/admin/company/') && method === 'GET') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            const companyId = url.pathname.split('/').pop();
            const k = await kv();
            const companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            const devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            const activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            const invoices = JSON.parse(await k.get(KEYS.INVOICES) || '[]');
            const company = companies.find(c => c.id === companyId);
            if (!company) return err("Company not found");
            const companyDevices = devices.filter(d => d.companyId === companyId);
            return res({
                ...company,
                devices: companyDevices,
                activity: activity.filter(a => a.companyId === companyId).slice(0, 100),
                invoices: invoices.filter(i => i.companyId === companyId),
                stats: {
                    totalDevices: companyDevices.length,
                    activeDevices: companyDevices.filter(d => d.status === 'ACTIVE').length,
                    pendingDevices: companyDevices.filter(d => d.status === 'PENDING_APPROVAL').length,
                    suspendedDevices: companyDevices.filter(d => d.status === 'SUSPENDED').length,
                    totalViolations: companyDevices.reduce((sum, d) => sum + (d.violations?.length || 0), 0),
                    totalCheckins: companyDevices.reduce((sum, d) => sum + (d.checkins?.length || 0), 0)
                }
            });
        }

        // ==================== GET SETTINGS ====================
        if (url.pathname === '/admin/settings' && method === 'GET') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            const k = await kv();
            const settings = JSON.parse(await k.get(KEYS.SETTINGS) || '{}');
            return res(settings);
        }

        // ==================== UPDATE SETTINGS ====================
        if (url.pathname === '/admin/settings' && method === 'POST') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { pricing, general } = body;
            const k = await kv();
            let settings = JSON.parse(await k.get(KEYS.SETTINGS) || '{}');
            if (pricing) settings.pricing = { ...settings.pricing, ...pricing };
            if (general) settings.general = { ...settings.general, ...general };
            settings.updatedAt = Date.now();
            settings.updatedBy = auth.username;
            await k.put(KEYS.SETTINGS, JSON.stringify(settings));
            return res({ ok: true, message: "Settings updated" });
        }

        // ==================== ADD ADMIN USER ====================
        if (url.pathname === '/admin/add-user' && method === 'POST') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { username, password, role } = body;
            if (!username || !password) return err("Username and password required");
            const k = await kv();
            let users = JSON.parse(await k.get(KEYS.USERS) || '[]');
            if (users.find(u => u.username === username)) return err("User already exists");
            users.push({
                id: 'user_' + Date.now(), username, password: await sha256(password),
                role: role || 'ADMIN', createdAt: Date.now(), createdBy: auth.username
            });
            await k.put(KEYS.USERS, JSON.stringify(users));
            return res({ ok: true, message: `User ${username} added` });
        }

        // ==================== DELETE ADMIN USER ====================
        if (url.pathname === '/admin/delete-user' && method === 'POST') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            let body;
            try { body = await request.json(); } catch(e) { return err("Invalid JSON body"); }
            const { username } = body;
            if (username === 'admin') return err("Cannot delete default admin");
            if (username === auth.username) return err("Cannot delete yourself");
            const k = await kv();
            let users = JSON.parse(await k.get(KEYS.USERS) || '[]');
            users = users.filter(u => u.username !== username);
            await k.put(KEYS.USERS, JSON.stringify(users));
            return res({ ok: true, message: `User ${username} deleted` });
        }

        // ==================== GET ADMIN USERS ====================
        if (url.pathname === '/admin/users' && method === 'GET') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            const k = await kv();
            const users = JSON.parse(await k.get(KEYS.USERS) || '[]');
            return res(users.map(u => ({ id: u.id, username: u.username, role: u.role, createdAt: u.createdAt })));
        }

        // ==================== CRON CHECK EXPIRED ====================
        if (url.pathname === '/cron/check-expired' && method === 'GET') {
            const auth = await verifyAdmin();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            const k = await kv();
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            const now = Date.now();
            let updated = false;
            for (const company of companies) {
                if (company.expiredAt < now && company.status === 'ACTIVE') {
                    company.status = 'EXPIRED';
                    updated = true;
                    for (const device of devices) {
                        if (device.companyId === company.id && (device.status === 'ACTIVE' || device.status === 'SUSPENDED')) {
                            device.status = 'BANNED';
                        }
                    }
                    if (licenses[company.licenseKey]) licenses[company.licenseKey].status = 'EXPIRED';
                    activity.unshift({ id: 'act_' + Date.now(), type: 'AUTO_BAN_ALL_DEVICES', companyId: company.id, companyName: company.companyName, reason: 'License expired', timestamp: now });
                }
            }
            if (updated) {
                await k.put(KEYS.COMPANIES, JSON.stringify(companies));
                await k.put(KEYS.DEVICES, JSON.stringify(devices));
                await k.put(KEYS.LICENSES, JSON.stringify(licenses));
                await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            }
            return res({ ok: true, message: "Expired check completed", updated });
        }

        // ==================== FALLBACK ====================
        return err("Endpoint not found", 404);
    }
};