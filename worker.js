// ============================================================
// VMS COMMAND CENTER - FULL ENTERPRISE EDITION v3.0
// Cloudflare Worker - Complete Backend API
// ============================================================

export default {
    async fetch(request, env) {
        const url = new URL(request.url);
        const method = request.method;

        // Storage keys
        const KEYS = {
            COMPANIES: 'vms_companies_full_v3',
            DEVICES: 'vms_devices_full_v3',
            LICENSES: 'vms_licenses_full_v3',
            USERS: 'vms_users_full_v3',
            ACTIVITY: 'vms_activity_full_v3',
            TOKENS: 'vms_tokens_full_v3',
            INVOICES: 'vms_invoices_full_v3',
            SETTINGS: 'vms_settings_full_v3'
        };

        const CORS = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, x-token, x-company-id, x-device-id',
            'Content-Type': 'application/json'
        };

        if (method === 'OPTIONS') return new Response(null, { headers: CORS });

        const kv = async () => {
            if (!env.VMS_STORAGE) throw new Error("KV storage not bound");
            return env.VMS_STORAGE;
        };

        const sha256 = async (str) => {
            const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(str));
            return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
        };

        const verify = async () => {
            const token = request.headers.get("x-token");
            if (!token) return null;
            try {
                const k = await kv();
                const raw = await k.get(`${KEYS.TOKENS}_${token}`);
                if (!raw) return null;
                const data = JSON.parse(raw);
                if (Date.now() > data.exp) {
                    await k.delete(`${KEYS.TODANA}_${token}`);
                    return null;
                }
                return data;
            } catch { return null; }
        };

        const res = (data, status = 200) => new Response(JSON.stringify(data), { status, headers: CORS });
        const err = (msg, status = 400) => res({ error: msg, ok: false }, status);

        // ==================== ROOT ====================
        if (url.pathname === '/' && method === 'GET') {
            return res({ 
                status: 'online', 
                version: 'v3.0-FULL-ENTERPRISE',
                name: 'VMS Command Center',
                features: ['Multi-company', 'Device management', 'Violation tracking', 'Auto-billing', 'Package system']
            });
        }

        // ==================== INIT SYSTEM ====================
        if (url.pathname === '/init' && method === 'POST') {
            const k = await kv();
            
            // Create default admin
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
            
            // Create default settings
            let settings = JSON.parse(await k.get(KEYS.SETTINGS) || '{}');
            if (Object.keys(settings).length === 0) {
                settings = {
                    pricing: {
                        DEMO: { price: 0, duration: 7, maxDevices: 2 },
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

        // ==================== GENERATE LICENSE (Admin) ====================
        if (url.pathname === '/generate-license' && method === 'POST') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const { 
                companyName, pic, phone, email, address, 
                package: pkg, customMaxDevices, notes 
            } = await request.json();
            
            if (!companyName || !pic || !phone || !email || !pkg) {
                return err("Missing required fields: companyName, pic, phone, email, package");
            }
            
            const k = await kv();
            const settings = JSON.parse(await k.get(KEYS.SETTINGS) || '{}');
            const pricing = settings.pricing || {};
            const pkgConfig = pricing[pkg];
            
            if (!pkgConfig) return err("Invalid package");
            
            // Generate unique license key
            const licenseKey = 'VMS-' + Date.now() + '-' + Math.random().toString(36).substring(2, 15).toUpperCase();
            
            // Check existing company
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            if (companies.find(c => c.companyName.toLowerCase() === companyName.toLowerCase())) {
                return err("Company already exists");
            }
            
            const maxDevices = customMaxDevices || pkgConfig.maxDevices;
            const expiredAt = Date.now() + (pkgConfig.duration || 30) * 24 * 60 * 60 * 1000;
            
            const newCompany = {
                id: 'comp_' + Date.now() + '_' + Math.random().toString(36).substring(2, 8),
                companyName,
                pic,
                phone,
                email,
                address: address || '',
                package: pkg,
                licenseKey,
                maxDevices,
                currentDevices: 0,
                status: 'ACTIVE',
                createdAt: Date.now(),
                expiredAt,
                lastPayment: Date.now(),
                paymentHistory: [],
                notes: notes || '',
                metadata: {
                    totalViolations: 0,
                    totalCheckins: 0,
                    lastActive: null,
                    devices: []
                }
            };
            
            companies.push(newCompany);
            
            // Save to licenses
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            licenses[licenseKey] = {
                companyId: newCompany.id,
                companyName,
                package: pkg,
                maxDevices,
                devices: [],
                createdAt: Date.now(),
                expiredAt,
                status: 'ACTIVE',
                lastRenewed: Date.now()
            };
            
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.LICENSES, JSON.stringify(licenses));
            
            // Log activity
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            activity.unshift({
                id: 'act_' + Date.now(),
                type: 'LICENSE_CREATED',
                companyId: newCompany.id,
                companyName,
                details: `License ${licenseKey} created for ${companyName} (${pkg} package)`,
                admin: auth.username,
                timestamp: Date.now()
            });
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            
            return res({
                ok: true,
                licenseKey,
                company: newCompany,
                message: `License generated successfully for ${companyName}`
            });
        }

        // ==================== VALIDATE LICENSE (Client Device) ====================
        if (url.pathname === '/validate-license' && method === 'POST') {
            const { licenseKey, deviceId, deviceName, meta, location } = await request.json();
            
            if (!licenseKey || !deviceId) return err("License key and device ID required");
            
            const k = await kv();
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            
            const license = licenses[licenseKey];
            if (!license) return err("Invalid license key", 403);
            
            const company = companies.find(c => c.id === license.companyId);
            if (!company) return err("Company not found", 403);
            
            // Check expired
            if (license.expiredAt < Date.now()) {
                license.status = 'EXPIRED';
                company.status = 'EXPIRED';
                await k.put(KEYS.LICENSES, JSON.stringify(licenses));
                await k.put(KEYS.COMPANIES, JSON.stringify(companies));
                return err("License expired. Please renew.", 403);
            }
            
            // Check if company is active
            if (company.status !== 'ACTIVE') {
                return err(`Company status: ${company.status}. Contact admin.`, 403);
            }
            
            // Find or create device
            let device = devices.find(d => d.deviceId === deviceId);
            const isNewDevice = !device;
            
            if (isNewDevice) {
                // Check max devices limit
                const companyDevices = devices.filter(d => d.companyId === company.id);
                if (companyDevices.length >= license.maxDevices) {
                    return err(`Max devices reached (${license.maxDevices}). Please upgrade package or remove unused devices.`, 403);
                }
                
                device = {
                    deviceId,
                    deviceName: deviceName || deviceId,
                    companyId: company.id,
                    companyName: company.companyName,
                    licenseKey,
                    status: 'PENDING_APPROVAL',
                    firstSeen: Date.now(),
                    lastSeen: Date.now(),
                    lastLocation: location || null,
                    meta: meta || {},
                    violations: [],
                    checkins: [],
                    approvedAt: null,
                    approvedBy: null
                };
                devices.push(device);
                
                // Log device registration request
                let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
                activity.unshift({
                    id: 'act_' + Date.now(),
                    type: 'DEVICE_REGISTRATION_REQUEST',
                    companyId: company.id,
                    companyName: company.companyName,
                    deviceId,
                    deviceName: deviceName || deviceId,
                    timestamp: Date.now(),
                    status: 'PENDING'
                });
                await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            } else {
                device.lastSeen = Date.now();
                if (location) device.lastLocation = location;
                if (meta) device.meta = { ...device.meta, ...meta };
            }
            
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            
            // Update company metadata
            company.metadata.lastActive = Date.now();
            company.currentDevices = devices.filter(d => d.companyId === company.id && d.status === 'ACTIVE').length;
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            
            // Update license device list
            if (!license.devices.includes(deviceId)) {
                license.devices.push(deviceId);
                await k.put(KEYS.LICENSES, JSON.stringify(licenses));
            }
            
            return res({
                ok: true,
                status: device.status,
                company: {
                    id: company.id,
                    name: company.companyName,
                    package: company.package,
                    maxDevices: license.maxDevices,
                    currentDevices: company.currentDevices,
                    expiredAt: license.expiredAt
                },
                device: {
                    id: device.deviceId,
                    name: device.deviceName,
                    status: device.status
                },
                message: device.status === 'PENDING_APPROVAL' ? 'Device pending admin approval' : 'License validated successfully'
            });
        }

        // ==================== REQUEST DEVICE APPROVAL ====================
        if (url.pathname === '/request-approval' && method === 'POST') {
            const { licenseKey, deviceId, deviceName, reason } = await request.json();
            
            const k = await kv();
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            
            const device = devices.find(d => d.deviceId === deviceId);
            if (!device) return err("Device not found");
            
            device.status = 'PENDING_APPROVAL';
            device.approvalRequest = {
                requestedAt: Date.now(),
                reason: reason || 'New device registration',
                deviceName: deviceName || device.deviceName
            };
            
            activity.unshift({
                id: 'act_' + Date.now(),
                type: 'DEVICE_APPROVAL_REQUEST',
                companyId: device.companyId,
                companyName: device.companyName,
                deviceId,
                deviceName: device.deviceName,
                reason: reason || 'New device registration',
                timestamp: Date.now(),
                status: 'PENDING'
            });
            
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            
            return res({ ok: true, message: "Approval request sent to admin" });
        }

        // ==================== APPROVE/REJECT DEVICE (Admin) ====================
        if (url.pathname === '/approve-device' && method === 'POST') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const { deviceId, approve, notes } = await request.json();
            
            const k = await kv();
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            
            const device = devices.find(d => d.deviceId === deviceId);
            if (!device) return err("Device not found");
            
            const company = companies.find(c => c.id === device.companyId);
            
            device.status = approve ? 'ACTIVE' : 'REJECTED';
            device.approvedAt = Date.now();
            device.approvedBy = auth.username;
            device.approvalNotes = notes || '';
            
            if (approve && company) {
                company.metadata.devices = company.metadata.devices || [];
                if (!company.metadata.devices.includes(deviceId)) {
                    company.metadata.devices.push(deviceId);
                }
                company.currentDevices = devices.filter(d => d.companyId === company.id && d.status === 'ACTIVE').length;
            }
            
            activity.unshift({
                id: 'act_' + Date.now(),
                type: approve ? 'DEVICE_APPROVED' : 'DEVICE_REJECTED',
                companyId: device.companyId,
                companyName: device.companyName,
                deviceId,
                deviceName: device.deviceName,
                approvedBy: auth.username,
                notes: notes || '',
                timestamp: Date.now()
            });
            
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            
            return res({ ok: true, message: `Device ${approve ? 'approved' : 'rejected'}` });
        }

        // ==================== DELETE DEVICE (Admin) ====================
        if (url.pathname === '/delete-device' && method === 'POST') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const { deviceId, reason } = await request.json();
            
            const k = await kv();
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            
            const deviceIndex = devices.findIndex(d => d.deviceId === deviceId);
            if (deviceIndex === -1) return err("Device not found");
            
            const device = devices[deviceIndex];
            
            // Remove from license
            if (licenses[device.licenseKey]) {
                licenses[device.licenseKey].devices = licenses[device.licenseKey].devices.filter(d => d !== deviceId);
                await k.put(KEYS.LICENSES, JSON.stringify(licenses));
            }
            
            // Remove from company
            const company = companies.find(c => c.id === device.companyId);
            if (company && company.metadata.devices) {
                company.metadata.devices = company.metadata.devices.filter(d => d !== deviceId);
                company.currentDevices = devices.filter(d => d.companyId === company.id && d.deviceId !== deviceId && d.status === 'ACTIVE').length;
            }
            
            // Log deletion
            activity.unshift({
                id: 'act_' + Date.now(),
                type: 'DEVICE_DELETED',
                companyId: device.companyId,
                companyName: device.companyName,
                deviceId,
                deviceName: device.deviceName,
                deletedBy: auth.username,
                reason: reason || 'Contract terminated or device unused',
                timestamp: Date.now()
            });
            
            // Remove device
            devices.splice(deviceIndex, 1);
            
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            
            return res({ ok: true, message: "Device deleted successfully" });
        }

        // ==================== REPORT VIOLATION (Client) ====================
        if (url.pathname === '/report-violation' && method === 'POST') {
            const { licenseKey, deviceId, violationType, details, location, evidence } = await request.json();
            
            const k = await kv();
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            
            const device = devices.find(d => d.deviceId === deviceId);
            if (!device) return err("Device not found");
            
            const violation = {
                id: 'viol_' + Date.now(),
                type: violationType,
                details: details || '',
                location: location || null,
                evidence: evidence || null,
                timestamp: Date.now(),
                deviceId,
                deviceName: device.deviceName
            };
            
            if (!device.violations) device.violations = [];
            device.violations.unshift(violation);
            
            // Count violations in last 7 days
            const recentViolations = device.violations.filter(v => v.timestamp > Date.now() - 7 * 24 * 60 * 60 * 1000);
            
            // Auto suspend if 5+ violations in 7 days
            if (recentViolations.length >= 5 && device.status === 'ACTIVE') {
                device.status = 'SUSPENDED';
                device.suspendedAt = Date.now();
                device.suspendReason = `Auto-suspended due to ${recentViolations.length} violations in 7 days`;
                
                activity.unshift({
                    id: 'act_' + Date.now(),
                    type: 'AUTO_SUSPEND',
                    companyId: device.companyId,
                    companyName: device.companyName,
                    deviceId,
                    deviceName: device.deviceName,
                    reason: device.suspendReason,
                    violationCount: recentViolations.length,
                    timestamp: Date.now()
                });
            }
            
            // Update company violation count
            const company = companies.find(c => c.id === device.companyId);
            if (company) {
                company.metadata.totalViolations = (company.metadata.totalViolations || 0) + 1;
            }
            
            activity.unshift({
                id: 'act_' + Date.now(),
                type: 'VIOLATION_REPORTED',
                companyId: device.companyId,
                companyName: device.companyName,
                deviceId,
                deviceName: device.deviceName,
                violationType,
                details,
                timestamp: Date.now()
            });
            
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            
            return res({ 
                ok: true, 
                deviceStatus: device.status,
                violationCount: recentViolations.length,
                message: device.status === 'SUSPENDED' ? 'Device suspended due to multiple violations' : 'Violation reported successfully'
            });
        }

        // ==================== CHECK-IN/CHECK-OUT (Client) ====================
        if (url.pathname === '/checkin' && method === 'POST') {
            const { licenseKey, deviceId, action, location } = await request.json();
            
            if (!licenseKey || !deviceId) return err("License key and device ID required");
            if (!['IN', 'OUT'].includes(action)) return err("Action must be IN or OUT");
            
            const k = await kv();
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            
            const device = devices.find(d => d.deviceId === deviceId);
            if (!device) return err("Device not found");
            if (device.status !== 'ACTIVE') return err(`Device status: ${device.status}. Cannot perform check-in/out.`);
            
            const checkinRecord = {
                id: 'chk_' + Date.now(),
                action,
                timestamp: Date.now(),
                location: location || null,
                deviceId,
                deviceName: device.deviceName
            };
            
            if (!device.checkins) device.checkins = [];
            device.checkins.unshift(checkinRecord);
            device.lastCheckin = Date.now();
            device.lastAction = action;
            
            // Update company total checkins
            const company = companies.find(c => c.id === device.companyId);
            if (company) {
                company.metadata.totalCheckins = (company.metadata.totalCheckins || 0) + 1;
            }
            
            activity.unshift({
                id: 'act_' + Date.now(),
                type: `CHECK_${action}`,
                companyId: device.companyId,
                companyName: device.companyName,
                deviceId,
                deviceName: device.deviceName,
                location,
                timestamp: Date.now()
            });
            
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            
            return res({ ok: true, message: `Check-${action} recorded successfully` });
        }

        // ==================== RENEW LICENSE (Admin) ====================
        if (url.pathname === '/renew-license' && method === 'POST') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const { companyId, months, amount, paymentMethod } = await request.json();
            
            const k = await kv();
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            let invoices = JSON.parse(await k.get(KEYS.INVOICES) || '[]');
            
            const company = companies.find(c => c.id === companyId);
            if (!company) return err("Company not found");
            
            const oldExpiry = company.expiredAt;
            const newExpiry = Math.max(Date.now(), oldExpiry) + months * 30 * 24 * 60 * 60 * 1000;
            
            company.expiredAt = newExpiry;
            company.status = 'ACTIVE';
            company.lastPayment = Date.now();
            company.paymentHistory = company.paymentHistory || [];
            company.paymentHistory.unshift({
                date: Date.now(),
                months,
                amount: amount || 0,
                method: paymentMethod || 'MANUAL',
                invoiceId: 'inv_' + Date.now()
            });
            
            if (licenses[company.licenseKey]) {
                licenses[company.licenseKey].expiredAt = newExpiry;
                licenses[company.licenseKey].status = 'ACTIVE';
                licenses[company.licenseKey].lastRenewed = Date.now();
            }
            
            // Create invoice
            const invoice = {
                id: 'inv_' + Date.now(),
                companyId: company.id,
                companyName: company.companyName,
                amount: amount || 0,
                months,
                status: 'PAID',
                createdAt: Date.now(),
                paidAt: Date.now(),
                paymentMethod: paymentMethod || 'MANUAL',
                admin: auth.username
            };
            invoices.unshift(invoice);
            
            // Reactivate suspended/banned devices
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            for (const device of devices) {
                if (device.companyId === companyId && (device.status === 'SUSPENDED' || device.status === 'BANNED')) {
                    device.status = 'ACTIVE';
                    device.reactivatedAt = Date.now();
                    device.reactivatedReason = 'License renewed';
                }
            }
            
            // Log activity
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            activity.unshift({
                id: 'act_' + Date.now(),
                type: 'LICENSE_RENEWED',
                companyId: company.id,
                companyName: company.companyName,
                months,
                amount,
                oldExpiry,
                newExpiry,
                admin: auth.username,
                timestamp: Date.now()
            });
            
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.LICENSES, JSON.stringify(licenses));
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.INVOICES, JSON.stringify(invoices.slice(0, 500)));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            
            return res({ ok: true, message: `License renewed for ${months} months`, newExpiry });
        }

        // ==================== UPDATE COMPANY PACKAGE (Admin) ====================
        if (url.pathname === '/update-package' && method === 'POST') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const { companyId, newPackage, customMaxDevices, notes } = await request.json();
            
            const k = await kv();
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            
            const company = companies.find(c => c.id === companyId);
            if (!company) return err("Company not found");
            
            const settings = JSON.parse(await k.get(KEYS.SETTINGS) || '{}');
            const pricing = settings.pricing || {};
            const pkgConfig = pricing[newPackage];
            
            if (!pkgConfig) return err("Invalid package");
            
            const oldPackage = company.package;
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
            
            activity.unshift({
                id: 'act_' + Date.now(),
                type: 'PACKAGE_UPDATED',
                companyId: company.id,
                companyName: company.companyName,
                oldPackage,
                newPackage,
                maxDevices,
                admin: auth.username,
                timestamp: Date.now()
            });
            
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.LICENSES, JSON.stringify(licenses));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            
            return res({ ok: true, message: `Package updated from ${oldPackage} to ${newPackage}` });
        }

        // ==================== DELETE COMPANY (Admin) ====================
        if (url.pathname === '/delete-company' && method === 'POST') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const { companyId, reason } = await request.json();
            
            const k = await kv();
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            let licenses = JSON.parse(await k.get(KEYS.LICENSES) || '{}');
            let activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            
            const companyIndex = companies.findIndex(c => c.id === companyId);
            if (companyIndex === -1) return err("Company not found");
            
            const company = companies[companyIndex];
            
            // Log deletion
            activity.unshift({
                id: 'act_' + Date.now(),
                type: 'COMPANY_DELETED',
                companyId: company.id,
                companyName: company.companyName,
                deletedBy: auth.username,
                reason: reason || 'No reason provided',
                devicesCount: devices.filter(d => d.companyId === companyId).length,
                timestamp: Date.now()
            });
            
            // Remove all devices
            devices = devices.filter(d => d.companyId !== companyId);
            
            // Remove license
            delete licenses[company.licenseKey];
            
            // Remove company
            companies.splice(companyIndex, 1);
            
            await k.put(KEYS.COMPANIES, JSON.stringify(companies));
            await k.put(KEYS.DEVICES, JSON.stringify(devices));
            await k.put(KEYS.LICENSES, JSON.stringify(licenses));
            await k.put(KEYS.ACTIVITY, JSON.stringify(activity.slice(0, 1000)));
            
            return res({ ok: true, message: `Company ${company.companyName} deleted` });
        }

        // ==================== GET ALL COMPANIES (Admin) ====================
        if (url.pathname === '/admin/companies' && method === 'GET') {
            const auth = await verify();
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

        // ==================== GET ALL DEVICES (Admin) ====================
        if (url.pathname === '/admin/devices' && method === 'GET') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const k = await kv();
            const devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            const companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            
            const enriched = devices.map(d => ({
                ...d,
                companyInfo: companies.find(c => c.id === d.companyId)
            }));
            
            return res(enriched);
        }

        // ==================== GET ACTIVITY LOGS (Admin) ====================
        if (url.pathname === '/admin/activity' && method === 'GET') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const k = await kv();
            const activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            const limit = parseInt(url.searchParams.get('limit') || '200');
            const type = url.searchParams.get('type');
            
            let filtered = activity;
            if (type) filtered = activity.filter(a => a.type === type);
            
            return res(filtered.slice(0, limit));
        }

        // ==================== GET DASHBOARD STATS (Admin) ====================
        if (url.pathname === '/admin/stats' && method === 'GET') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const k = await kv();
            const companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            const devices = JSON.parse(await k.get(KEYS.DEVICES) || '[]');
            const activity = JSON.parse(await k.get(KEYS.ACTIVITY) || '[]');
            const invoices = JSON.parse(await k.get(KEYS.INVOICES) || '[]');
            
            const now = Date.now();
            const sevenDaysAgo = now - 7 * 24 * 60 * 60 * 1000;
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
                    banned: devices.filter(d => d.status === 'BANNED').length,
                    byCompany: devices.reduce((acc, d) => {
                        acc[d.companyName] = (acc[d.companyName] || 0) + 1;
                        return acc;
                    }, {})
                },
                violations: {
                    total: activity.filter(a => a.type === 'VIOLATION_REPORTED').length,
                    last7Days: activity.filter(a => a.type === 'VIOLATION_REPORTED' && a.timestamp > sevenDaysAgo).length,
                    last30Days: activity.filter(a => a.type === 'VIOLATION_REPORTED' && a.timestamp > thirtyDaysAgo).length,
                    byType: activity.filter(a => a.type === 'VIOLATION_REPORTED').reduce((acc, a) => {
                        acc[a.violationType] = (acc[a.violationType] || 0) + 1;
                        return acc;
                    }, {})
                },
                checkins: {
                    total: activity.filter(a => a.type === 'CHECK_IN' || a.type === 'CHECK_OUT').length,
                    last7Days: activity.filter(a => (a.type === 'CHECK_IN' || a.type === 'CHECK_OUT') && a.timestamp > sevenDaysAgo).length,
                    last30Days: activity.filter(a => (a.type === 'CHECK_IN' || a.type === 'CHECK_OUT') && a.timestamp > thirtyDaysAgo).length
                },
                revenue: {
                    total: invoices.reduce((sum, inv) => sum + (inv.amount || 0), 0),
                    last30Days: invoices.filter(inv => inv.createdAt > thirtyDaysAgo).reduce((sum, inv) => sum + (inv.amount || 0), 0),
                    byCompany: invoices.reduce((acc, inv) => {
                        acc[inv.companyName] = (acc[inv.companyName] || 0) + (inv.amount || 0);
                        return acc;
                    }, {})
                },
                recentActivity: activity.slice(0, 20),
                timestamp: now
            };
            
            return res(stats);
        }

        // ==================== GET COMPANY DETAILS (Admin) ====================
        if (url.pathname.startsWith('/admin/company/') && method === 'GET') {
            const auth = await verify();
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
            const companyActivity = activity.filter(a => a.companyId === companyId);
            const companyInvoices = invoices.filter(i => i.companyId === companyId);
            
            return res({
                ...company,
                devices: companyDevices,
                activity: companyActivity.slice(0, 100),
                invoices: companyInvoices,
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

        // ==================== UPDATE SETTINGS (Admin) ====================
        if (url.pathname === '/admin/settings' && method === 'POST') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const { pricing, general } = await request.json();
            
            const k = await kv();
            let settings = JSON.parse(await k.get(KEYS.SETTINGS) || '{}');
            
            if (pricing) settings.pricing = { ...settings.pricing, ...pricing };
            if (general) settings.general = { ...settings.general, ...general };
            
            settings.updatedAt = Date.now();
            settings.updatedBy = auth.username;
            
            await k.put(KEYS.SETTINGS, JSON.stringify(settings));
            
            return res({ ok: true, message: "Settings updated" });
        }

        // ==================== GET SETTINGS (Admin) ====================
        if (url.pathname === '/admin/settings' && method === 'GET') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const k = await kv();
            const settings = JSON.parse(await k.get(KEYS.SETTINGS) || '{}');
            
            return res(settings);
        }

        // ==================== ADD ADMIN USER ====================
        if (url.pathname === '/admin/add-user' && method === 'POST') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const { username, password, role } = await request.json();
            
            if (!username || !password) return err("Username and password required");
            
            const k = await kv();
            let users = JSON.parse(await k.get(KEYS.USERS) || '[]');
            
            if (users.find(u => u.username === username)) return err("User already exists");
            
            users.push({
                id: 'user_' + Date.now(),
                username,
                password: await sha256(password),
                role: role || 'ADMIN',
                createdAt: Date.now(),
                createdBy: auth.username
            });
            
            await k.put(KEYS.USERS, JSON.stringify(users));
            
            return res({ ok: true, message: `User ${username} added` });
        }

        // ==================== DELETE ADMIN USER ====================
        if (url.pathname === '/admin/delete-user' && method === 'POST') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const { username } = await request.json();
            
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
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const k = await kv();
            const users = JSON.parse(await k.get(KEYS.USERS) || '[]');
            
            return res(users.map(u => ({ id: u.id, username: u.username, role: u.role, createdAt: u.createdAt })));
        }

        // ==================== LOGIN ====================
        if (url.pathname === '/login' && method === 'POST') {
            const { username, password } = await request.json();
            
            const k = await kv();
            let users = JSON.parse(await k.get(KEYS.USERS) || '[]');
            
            // Auto-init if no users
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
            
            const hash = await sha256(password);
            const user = users.find(u => u.username === username && u.password === hash);
            
            if (!user) return err("Invalid credentials", 401);
            
            const token = await sha256(username + Date.now() + Math.random());
            const exp = Date.now() + 8 * 60 * 60 * 1000; // 8 hours
            
            await k.put(`${KEYS.TOKENS}_${token}`, JSON.stringify({
                username: user.username,
                role: user.role,
                userId: user.id,
                exp
            }));
            
            return res({
                ok: true,
                token,
                role: user.role,
                username: user.username,
                expiresIn: 8 * 60 * 60 * 1000
            });
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

        // ==================== CHECK EXPIRED LICENSES (Auto Run) ====================
        if (url.pathname === '/cron/check-expired' && method === 'GET') {
            // This endpoint can be called by a cron job
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
                    
                    // Ban all devices
                    for (const device of devices) {
                        if (device.companyId === company.id && (device.status === 'ACTIVE' || device.status === 'SUSPENDED')) {
                            device.status = 'BANNED';
                            device.bannedAt = now;
                            device.banReason = 'License expired - no payment received';
                        }
                    }
                    
                    if (licenses[company.licenseKey]) {
                        licenses[company.licenseKey].status = 'EXPIRED';
                    }
                    
                    activity.unshift({
                        id: 'act_' + Date.now(),
                        type: 'AUTO_BAN_ALL_DEVICES',
                        companyId: company.id,
                        companyName: company.companyName,
                        reason: 'License expired - auto ban',
                        devicesBanned: devices.filter(d => d.companyId === company.id).length,
                        timestamp: now
                    });
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

        // ==================== GENERATE INVOICE ====================
        if (url.pathname === '/generate-invoice' && method === 'POST') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const { companyId, months, amount, notes } = await request.json();
            
            const k = await kv();
            let companies = JSON.parse(await k.get(KEYS.COMPANIES) || '[]');
            let invoices = JSON.parse(await k.get(KEYS.INVOICES) || '[]');
            
            const company = companies.find(c => c.id === companyId);
            if (!company) return err("Company not found");
            
            const invoice = {
                id: 'INV-' + Date.now() + '-' + Math.random().toString(36).substring(2, 6).toUpperCase(),
                companyId: company.id,
                companyName: company.companyName,
                companyAddress: company.address,
                companyPic: company.pic,
                amount: amount || 0,
                months: months || 1,
                status: 'UNPAID',
                createdAt: Date.now(),
                dueDate: Date.now() + 14 * 24 * 60 * 60 * 1000,
                notes: notes || '',
                generatedBy: auth.username
            };
            
            invoices.unshift(invoice);
            
            await k.put(KEYS.INVOICES, JSON.stringify(invoices.slice(0, 500)));
            
            return res({ ok: true, invoice });
        }

        // ==================== MARK INVOICE PAID ====================
        if (url.pathname === '/mark-invoice-paid' && method === 'POST') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const { invoiceId, paymentMethod, notes } = await request.json();
            
            const k = await kv();
            let invoices = JSON.parse(await k.get(KEYS.INVOICES) || '[]');
            
            const invoice = invoices.find(i => i.id === invoiceId);
            if (!invoice) return err("Invoice not found");
            
            invoice.status = 'PAID';
            invoice.paidAt = Date.now();
            invoice.paidBy = auth.username;
            invoice.paymentMethod = paymentMethod || 'MANUAL';
            invoice.paymentNotes = notes || '';
            
            await k.put(KEYS.INVOICES, JSON.stringify(invoices));
            
            // Also renew license if this is a renewal invoice
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
            
            return res({ ok: true, message: "Invoice marked as paid" });
        }

        // ==================== GET INVOICES ====================
        if (url.pathname === '/admin/invoices' && method === 'GET') {
            const auth = await verify();
            if (!auth || auth.role !== 'SUPER_ADMIN') return err("Unauthorized", 401);
            
            const k = await kv();
            const invoices = JSON.parse(await k.get(KEYS.INVOICES) || '[]');
            const status = url.searchParams.get('status');
            
            let filtered = invoices;
            if (status) filtered = invoices.filter(i => i.status === status);
            
            return res(filtered);
        }

        // ==================== FALLBACK ====================
        return err("Endpoint not found", 404);
    }
};