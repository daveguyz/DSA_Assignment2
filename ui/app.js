// NamEats UI - talks to the microservices through the nginx gateway (/api/*)
"use strict";

const API = "/api";
const POLL_MS = 2500;
const LIFECYCLE = ["CREATED", "CONFIRMED", "PREPARING", "READY", "OUT_FOR_DELIVERY", "DELIVERED"];
const LABEL = {
  CREATED: "Order placed", CONFIRMED: "Paid & confirmed", PREPARING: "Being prepared",
  READY: "Ready for pickup", OUT_FOR_DELIVERY: "On the way", DELIVERED: "Delivered", CANCELLED: "Cancelled"
};

const state = {
  tab: "customer",
  customerId: null, restaurantId: null, cart: {}, menu: [], trackOrderId: null,
  kitchenRestaurantId: null, driverId: null,
  maps: {}
};

// ---------------------------------------------------------------- helpers
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const money = (n) => "N$ " + Number(n || 0).toFixed(2);
const short = (id) => "#" + String(id).slice(0, 8).toUpperCase();
const time = (iso) => iso ? new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "";

async function api(path, opts = {}) {
  const res = await fetch(API + path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined
  });
  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  if (!res.ok) {
    const err = new Error((data && data.message) || res.statusText);
    err.status = res.status;
    throw err;
  }
  return data;
}

let toastTimer;
function toast(msg, isErr = false) {
  const t = $("toast");
  t.textContent = msg;
  t.className = "toast show" + (isErr ? " err" : "");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (t.className = "toast"), 3500);
}

function fillSelect(sel, items, valueFn, labelFn, keep) {
  const prev = keep ?? sel.value;
  sel.innerHTML = items.map((i) => `<option value="${esc(valueFn(i))}">${esc(labelFn(i))}</option>`).join("");
  if (prev && items.some((i) => String(valueFn(i)) === String(prev))) sel.value = prev;
}

function notesHtml(list) {
  if (!list.length) return `<p class="muted">Nothing yet.</p>`;
  return list.map((n) => `<div class="note"><span class="ch">${esc(n.channel)}</span><b>${esc(n.title)}</b> - ${esc(n.message)}
    <span class="id">${time(n.createdAt)}</span></div>`).join("");
}

// ---------------------------------------------------------------- maps
function getMap(key, el) {
  if (state.maps[key]) return state.maps[key];
  if (typeof L === "undefined") { $(el).innerHTML = `<p class="muted">Map library not loaded (offline?).</p>`; return null; }
  const map = L.map(el).setView([-22.5609, 17.0836], 13);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 18, attribution: "&copy; OpenStreetMap contributors"
  }).addTo(map);
  const layer = L.layerGroup().addTo(map);
  state.maps[key] = { map, layer, fitted: null };
  return state.maps[key];
}

function drawDelivery(key, el, d) {
  const m = getMap(key, el);
  if (!m) return;
  m.layer.clearLayers();
  if (!d) return;
  const pts = [];
  if (d.route && d.route.path) {
    L.polyline(d.route.path, { color: "#c8642b", weight: 5, opacity: .8, dashArray: d.status === "ASSIGNED" ? "8 8" : null }).addTo(m.layer);
  }
  if (d.restaurantLat) {
    L.circleMarker([d.restaurantLat, d.restaurantLng], { radius: 9, color: "#231d18", fillColor: "#f0a35e", fillOpacity: 1 })
      .bindTooltip(d.restaurantName || "Restaurant").addTo(m.layer);
    pts.push([d.restaurantLat, d.restaurantLng]);
  }
  if (d.customerLat) {
    L.circleMarker([d.customerLat, d.customerLng], { radius: 9, color: "#231d18", fillColor: "#4f7a3a", fillOpacity: 1 })
      .bindTooltip("Customer").addTo(m.layer);
    pts.push([d.customerLat, d.customerLng]);
  }
  if (d.driverLat && d.status !== "DELIVERED" && d.status !== "CANCELLED") {
    L.marker([d.driverLat, d.driverLng], { icon: L.divIcon({ className: "driver-dot", iconSize: [16, 16] }) })
      .bindTooltip(d.driverName || "Driver", { permanent: true, direction: "top" }).addTo(m.layer);
    pts.push([d.driverLat, d.driverLng]);
  }
  // fit once per delivery so live movement does not keep re-zooming
  if (pts.length && m.fitted !== d.orderId + d.status) {
    m.map.fitBounds(pts, { padding: [40, 40], maxZoom: 15 });
    m.fitted = d.orderId + d.status;
  }
  setTimeout(() => m.map.invalidateSize(), 50);
}

// ---------------------------------------------------------------- surge
async function loadSurge() {
  try {
    const s = await api("/pricing/surge");
    const el = $("surge");
    el.textContent = `surge x${Number(s.multiplier).toFixed(2)} - ${s.reason}`;
    el.className = "surge" + (s.multiplier > 1 ? " hot" : "");
  } catch { $("surge").textContent = "pricing offline"; }
}

// ================================================================ CUSTOMER
async function initCustomer() {
  const customers = await api("/customers");
  fillSelect($("c-customer"), customers, (c) => c.id, (c) => c.fullName);
  state.customerId = Number($("c-customer").value);
  await loadAddresses();
  await loadRestaurants();
}

async function loadAddresses() {
  const p = await api(`/customers/${state.customerId}`);
  fillSelect($("c-address"), p.addresses, (a) => a.id, (a) => `${a.label} - ${a.street}, ${a.suburb}`);
}

async function loadRestaurants() {
  const list = await api("/restaurants");
  $("c-restaurants").innerHTML = list.map((r) => {
    const open = r.openNow && r.acceptingOrders;
    return `<div class="card click ${r.id === state.restaurantId ? "selected" : ""}" data-rid="${r.id}">
      <div class="row"><span class="name">${esc(r.name)}</span><span class="pill ${open ? "open" : "closed"}">${open ? "open" : "closed"}</span></div>
      <div class="meta">${esc(r.cuisine)} - ${esc(r.suburb)} - rated ${r.rating}</div></div>`;
  }).join("");
  document.querySelectorAll("#c-restaurants .card").forEach((c) =>
    c.addEventListener("click", () => selectRestaurant(Number(c.dataset.rid), list.find((r) => r.id === Number(c.dataset.rid)))));
}

async function selectRestaurant(id, r) {
  state.restaurantId = id;
  state.cart = {};
  $("c-menu-title").textContent = r ? `Menu - ${r.name}` : "Menu";
  await loadRestaurants();
  await loadMenu();
}

async function loadMenu() {
  if (!state.restaurantId) return;
  state.menu = await api(`/restaurants/${state.restaurantId}/menu`);
  $("c-menu").innerHTML = state.menu.map((m) => {
    const out = !m.available || m.stock <= 0;
    return `<div class="card">
      <div class="row"><span class="name">${esc(m.name)}</span><span class="price">${money(m.price)}</span></div>
      <div class="meta">${esc(m.description)}</div>
      <div class="row"><span class="id">${out ? "sold out" : m.stock + " in stock"}</span>
        <span><button class="btn small ghost" data-dec="${m.id}" ${out ? "disabled" : ""}>-</button>
        <b>${state.cart[m.id] || 0}</b>
        <button class="btn small" data-inc="${m.id}" ${out ? "disabled" : ""}>+</button></span></div></div>`;
  }).join("") || `<p class="muted">No menu items.</p>`;
  document.querySelectorAll("[data-inc]").forEach((b) => b.onclick = () => changeQty(Number(b.dataset.inc), 1));
  document.querySelectorAll("[data-dec]").forEach((b) => b.onclick = () => changeQty(Number(b.dataset.dec), -1));
  renderCart();
}

function changeQty(id, delta) {
  state.cart[id] = Math.max(0, (state.cart[id] || 0) + delta);
  if (!state.cart[id]) delete state.cart[id];
  loadMenu();
}

function renderCart() {
  const lines = Object.entries(state.cart);
  if (!lines.length) { $("c-cart").innerHTML = ""; return; }
  let subtotal = 0;
  const rows = lines.map(([id, q]) => {
    const m = state.menu.find((x) => x.id === Number(id));
    subtotal += m.price * q;
    return `<div class="row"><span>${q} x ${esc(m.name)}</span><span class="price">${money(m.price * q)}</span></div>`;
  }).join("");
  $("c-cart").innerHTML = `${rows}<div class="row"><b>Food subtotal</b><b class="price">${money(subtotal)}</b></div>
    <p class="muted">Delivery fee (distance x surge) is added once the restaurant confirms stock.</p>
    <button id="c-place" class="btn">Place order</button>`;
  $("c-place").onclick = placeOrder;
}

async function placeOrder() {
  try {
    const order = await api("/orders", {
      method: "POST",
      body: {
        customerId: state.customerId,
        restaurantId: state.restaurantId,
        addressId: Number($("c-address").value),
        paymentMethod: $("c-payment").value,
        items: Object.entries(state.cart).map(([id, q]) => ({ menuItemId: Number(id), quantity: q }))
      }
    });
    toast(`Order ${short(order.id)} placed - watch it move through the pipeline`);
    state.cart = {};
    state.trackOrderId = order.id;
    await loadMenu();
    await refreshCustomer();
  } catch (e) { toast(e.message, true); }
}

async function refreshCustomer() {
  if (!state.customerId) return;
  const orders = await api(`/orders?customerId=${state.customerId}&limit=15`);
  $("c-orders").innerHTML = orders.map((o) => `
    <div class="card click ${o.id === state.trackOrderId ? "selected" : ""}" data-oid="${o.id}">
      <div class="row"><span class="name">${esc(o.restaurantName || "Restaurant " + o.restaurantId)}</span><span class="pill ${o.status}">${o.status}</span></div>
      <div class="row"><span class="id">${short(o.id)} - ${time(o.createdAt)}</span><span class="price">${o.total > 0 ? money(o.total) : "pricing..."}</span></div>
    </div>`).join("") || `<p class="muted">No orders yet.</p>`;
  document.querySelectorAll("#c-orders .card").forEach((c) => c.onclick = () => { state.trackOrderId = c.dataset.oid; refreshCustomer(); });
  if (state.trackOrderId) await refreshTracking();
  if (state.restaurantId) loadMenu(); // live stock
}

async function refreshTracking() {
  const id = state.trackOrderId;
  const o = await api(`/orders/${id}`);
  $("c-track").classList.remove("hidden");
  $("t-title").textContent = `Order ${short(o.id)} - ${o.restaurantName || ""}`;
  const reached = new Set(o.history.map((h) => h.toStatus));
  const cancelled = o.status === "CANCELLED";
  const steps = LIFECYCLE.map((s) => {
    const h = o.history.find((x) => x.toStatus === s);
    const cls = o.status === s ? "current" : reached.has(s) ? "done" : "";
    return `<li class="${cls}">${LABEL[s]}${h ? `<small>${time(h.changedAt)} - ${esc(h.reason || "")}</small>` : ""}</li>`;
  });
  if (cancelled) {
    const h = o.history.find((x) => x.toStatus === "CANCELLED");
    steps.push(`<li class="cancel">Cancelled<small>${esc(o.cancelReason || (h && h.reason) || "")}</small></li>`);
  }
  $("t-timeline").innerHTML = steps.join("");
  $("t-bill").innerHTML = o.items.map((i) => `<div><span>${i.quantity} x ${esc(i.name || "item " + i.menuItemId)}</span><span>${money(i.unitPrice * i.quantity)}</span></div>`).join("") +
    `<div><span>Delivery fee (surge x${Number(o.surgeMultiplier).toFixed(2)})</span><span>${money(o.deliveryFee)}</span></div>
     <div class="total"><span>Total - ${esc(o.paymentMethod)}</span><span>${money(o.total)}</span></div>`;
  const canCancel = o.status === "CREATED" || o.status === "CONFIRMED";
  $("t-cancel").classList.toggle("hidden", !canCancel);
  $("t-cancel").onclick = async () => {
    try { await api(`/orders/${id}/cancel`, { method: "POST", body: { reason: "Changed my mind" } }); toast("Cancellation requested"); refreshCustomer(); }
    catch (e) { toast(e.message, true); }
  };
  const notes = await api(`/notifications?orderId=${id}&recipientType=CUSTOMER&limit=20`);
  $("t-notes").innerHTML = notesHtml(notes);
  try {
    const d = await api(`/deliveries/${id}`);
    drawDelivery("track", "t-map", d);
    $("t-eta").textContent = d.driverName
      ? `${d.driverName} (${d.driverPhone || ""}) - ${d.status} - route ${d.route ? d.route.via.join(" > ") : ""} - ${d.distanceKm ?? "?"} km, ETA ${d.etaMinutes ?? "?"} min`
      : `Delivery status: ${d.status}`;
  } catch {
    drawDelivery("track", "t-map", null);
    $("t-eta").textContent = cancelled ? "" : "Waiting for payment confirmation before a driver is dispatched.";
  }
}

// ================================================================ RESTAURANT
async function initRestaurant() {
  const list = await api("/restaurants");
  fillSelect($("r-restaurant"), list, (r) => r.id, (r) => r.name);
  state.kitchenRestaurantId = Number($("r-restaurant").value);
}

async function refreshRestaurant() {
  const id = state.kitchenRestaurantId;
  if (!id) return;
  const r = await api(`/restaurants/${id}`);
  $("r-toggle").textContent = r.acceptingOrders ? "Pause new orders" : "Resume orders";
  $("r-toggle").onclick = async () => {
    await api(`/restaurants/${id}/availability`, { method: "PUT", body: { acceptingOrders: !r.acceptingOrders } });
    refreshRestaurant();
  };
  const open = r.openNow && r.acceptingOrders;
  $("r-open").className = "pill " + (open ? "open" : "closed");
  $("r-open").textContent = !r.openNow ? "outside opening hours" : r.acceptingOrders ? "accepting orders" : "paused";

  const queue = await api(`/restaurants/${id}/orders`);
  $("r-queue").innerHTML = queue.map((k) => `
    <div class="card">
      <div class="row"><span class="name">Order ${short(k.orderId)}</span><span class="pill ${k.status}">${k.status}</span></div>
      <div class="meta">${k.items.map((i) => `${i.quantity} x ${esc(i.name)}`).join(", ")}</div>
      <div class="row"><span class="id">${time(k.createdAt)} - ${money(k.subtotal)}</span>
        ${k.status === "CONFIRMED" ? `<button class="btn small" data-prep="${k.orderId}">Start preparing</button>` : ""}
        ${k.status === "PREPARING" ? `<button class="btn small green" data-ready="${k.orderId}">Mark ready</button>` : ""}
        ${k.status === "READY" ? `<span class="muted">waiting for driver</span>` : ""}
      </div></div>`).join("") || `<p class="muted">No paid orders in the kitchen. (Orders appear once payment succeeds.)</p>`;
  document.querySelectorAll("[data-prep]").forEach((b) => b.onclick = () => kitchen(id, b.dataset.prep, "preparing"));
  document.querySelectorAll("[data-ready]").forEach((b) => b.onclick = () => kitchen(id, b.dataset.ready, "ready"));

  $("r-stock").innerHTML = `<tr><th>Item</th><th>Price</th><th>Stock</th><th></th></tr>` + r.menu.map((m) => `
    <tr><td>${esc(m.name)}</td><td class="num">${money(m.price)}</td>
        <td class="num">${m.stock}</td>
        <td><button class="btn small ghost" data-restock="${m.id}">+10</button>
            <button class="btn small ghost" data-avail="${m.id}" data-v="${!m.available}">${m.available ? "Disable" : "Enable"}</button></td></tr>`).join("");
  document.querySelectorAll("[data-restock]").forEach((b) => b.onclick = async () => {
    await api(`/restaurants/${id}/menu/${b.dataset.restock}/stock`, { method: "PUT", body: { add: 10 } }); refreshRestaurant();
  });
  document.querySelectorAll("[data-avail]").forEach((b) => b.onclick = async () => {
    await api(`/restaurants/${id}/menu/${b.dataset.avail}`, { method: "PUT", body: { available: b.dataset.v === "true" } }); refreshRestaurant();
  });
  $("r-notes").innerHTML = notesHtml(await api(`/notifications?recipientType=RESTAURANT&recipientId=${id}&limit=15`));
}

async function kitchen(rid, oid, step) {
  try { await api(`/restaurants/${rid}/orders/${oid}/${step}`, { method: "POST" }); toast(`Order ${short(oid)} -> ${step}`); refreshRestaurant(); }
  catch (e) { toast(e.message, true); }
}

// ================================================================ DRIVER
async function initDriver() {
  const drivers = await api("/drivers");
  fillSelect($("d-driver"), drivers, (d) => d.id, (d) => `${d.name} (${d.vehicle})`);
  state.driverId = Number($("d-driver").value);
}

async function refreshDriver() {
  const id = state.driverId;
  if (!id) return;
  const d = await api(`/drivers/${id}`);
  $("d-status").className = "pill " + d.status;
  $("d-status").textContent = d.status;
  $("d-online").disabled = d.status !== "OFFLINE";
  $("d-offline").disabled = d.status !== "AVAILABLE";
  const jobs = await api(`/drivers/${id}/deliveries?active=true`);
  const job = jobs[0];
  if (!job) {
    $("d-job").innerHTML = `<p class="muted">${d.status === "OFFLINE" ? "You are offline." : "Waiting for the dispatcher to assign the nearest order..."}</p>`;
    drawDelivery("driver", "d-map", { driverLat: d.latitude, driverLng: d.longitude, driverName: d.name, status: "IDLE", orderId: "idle" });
  } else {
    $("d-job").innerHTML = `<div class="card">
      <div class="row"><span class="name">${esc(job.restaurantName)} -> customer</span><span class="pill ${job.status}">${job.status}</span></div>
      <div class="meta">Drop-off: ${esc(job.deliveryAddress)}</div>
      <div class="meta">Route: ${job.route ? esc(job.route.via.join(" > ")) : "-"} - ${job.distanceKm} km - ETA ${job.etaMinutes} min</div>
      <div class="row"><span class="id">Order ${short(job.orderId)} - ${job.orderReady ? "food READY" : "kitchen still busy"}</span>
        ${job.status === "ASSIGNED" ? `<button class="btn small" id="d-pickup" ${job.orderReady ? "" : "disabled"}>Picked up</button>` : ""}
        ${job.status === "PICKED_UP" ? `<button class="btn small green" id="d-complete">Delivered</button>` : ""}
      </div></div>`;
    if ($("d-pickup")) $("d-pickup").onclick = () => driverAction(job.orderId, "pickup");
    if ($("d-complete")) $("d-complete").onclick = () => driverAction(job.orderId, "complete");
    drawDelivery("driver", "d-map", job);
  }
  $("d-notes").innerHTML = notesHtml(await api(`/notifications?recipientType=DRIVER&recipientId=${id}&limit=15`));
}

async function driverAction(orderId, action) {
  try { await api(`/deliveries/${orderId}/${action}`, { method: "POST", body: { driverId: state.driverId } }); toast(`Order ${short(orderId)}: ${action} done`); refreshDriver(); }
  catch (e) { toast(e.message, true); }
}

async function setDriverStatus(status) {
  try { await api(`/drivers/${state.driverId}/status`, { method: "PUT", body: { status } }); refreshDriver(); }
  catch (e) { toast(e.message, true); }
}

// ================================================================ ADMIN
async function refreshAdmin() {
  const [{ summary: s, byStatus }, rest, drv, health, fleet, feed] = await Promise.all([
    api("/admin/reports/summary"), api("/admin/reports/restaurants"), api("/admin/reports/deliveries"),
    api("/admin/system/health"), api("/drivers/stats"), api("/notifications?limit=40")
  ]);
  const kpi = (v, l) => `<div class="kpi"><strong>${v}</strong><span>${l}</span></div>`;
  $("a-kpis").innerHTML = kpi(s.totalOrders, "orders") + kpi(s.delivered, "delivered") + kpi(s.inProgress, "in progress") +
    kpi(money(s.revenue), "delivered revenue") + kpi(money(s.avgOrderValue), "avg order value") +
    kpi(s.avgFulfilmentMinutes + " min", "order -> door") + kpi(s.cancellationRate + "%", "cancellation rate");
  $("a-restaurants").innerHTML = `<tr><th>Restaurant</th><th>Orders</th><th>Delivered</th><th>Revenue</th><th>Avg prep</th><th>Cancel %</th></tr>` +
    rest.map((r) => `<tr><td>${esc(r.restaurantName)}</td><td class="num">${r.totalOrders}</td><td class="num">${r.delivered}</td>
      <td class="num">${money(r.revenue)}</td><td class="num">${r.avgPrepMinutes} min</td><td class="num">${r.cancellationRate}</td></tr>`).join("");
  $("a-drivers").innerHTML = `<tr><th>Driver</th><th>Jobs</th><th>Done</th><th>Km</th><th>Avg ride</th><th>On-time %</th></tr>` +
    drv.map((d) => `<tr><td>${esc(d.driverName)}</td><td class="num">${d.assigned}</td><td class="num">${d.completed}</td>
      <td class="num">${d.totalDistanceKm}</td><td class="num">${d.avgDeliveryMinutes} min</td><td class="num">${d.onTimeRate}</td></tr>`).join("");
  $("a-health").innerHTML = health.map((h) => `<span class="pill ${h.status}">${esc(h.name)}: ${h.status}</span>`).join("") +
    byStatus.map((b) => `<span class="pill ${b.status}">${b.status}: ${b.count}</span>`).join("");
  $("a-fleet").innerHTML = `<span class="pill AVAILABLE">available ${fleet.available}</span><span class="pill BUSY">busy ${fleet.busy}</span>
    <span class="pill">offline ${fleet.offline}</span><span class="pill">queued deliveries ${fleet.pendingDeliveries}</span>`;
  $("a-feed").innerHTML = feed.map((n) => `<div class="note"><span class="ch">${esc(n.channel)} -> ${esc(n.recipientType)}#${n.recipientId}</span>
    <b>${esc(n.title)}</b> - ${esc(n.message)} <span class="id">${time(n.createdAt)}</span></div>`).join("");
}

// ================================================================ wiring
const refreshers = { customer: refreshCustomer, restaurant: refreshRestaurant, driver: refreshDriver, admin: refreshAdmin };

function switchTab(tab) {
  state.tab = tab;
  document.querySelectorAll(".tab").forEach((b) => b.classList.toggle("active", b.dataset.tab === tab));
  document.querySelectorAll(".panel").forEach((p) => p.classList.toggle("active", p.id === "tab-" + tab));
  tick();
  Object.values(state.maps).forEach((m) => setTimeout(() => m.map.invalidateSize(), 60));
}

let busy = false;
async function tick() {
  if (busy) return;
  busy = true;
  try { await Promise.all([refreshers[state.tab](), loadSurge()]); }
  catch (e) { console.warn(e); }
  finally { busy = false; }
}

document.addEventListener("DOMContentLoaded", async () => {
  document.querySelectorAll(".tab").forEach((b) => b.onclick = () => switchTab(b.dataset.tab));
  $("c-customer").onchange = async (e) => { state.customerId = Number(e.target.value); state.trackOrderId = null; $("c-track").classList.add("hidden"); await loadAddresses(); tick(); };
  $("r-restaurant").onchange = (e) => { state.kitchenRestaurantId = Number(e.target.value); tick(); };
  $("d-driver").onchange = (e) => { state.driverId = Number(e.target.value); state.maps.driver && (state.maps.driver.fitted = null); tick(); };
  $("d-online").onclick = () => setDriverStatus("AVAILABLE");
  $("d-offline").onclick = () => setDriverStatus("OFFLINE");
  try {
    await Promise.all([initCustomer(), initRestaurant(), initDriver()]);
  } catch (e) {
    toast("Services are still starting - retrying...", true);
    setTimeout(() => location.reload(), 5000);
    return;
  }
  tick();
  setInterval(tick, POLL_MS);
});
