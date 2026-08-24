const $ = (selector) => document.querySelector(selector);
const LANGUAGE_KEY = "vs_ai_gateway_language";
const LANGUAGE_FILES = ["zh-CN", "en-US"];
let catalog = null;
let providerTypes = [];
let locale = "zh-CN";
let messages = {};

function escapeHtml(value) {
  return String(value ?? "-").replace(/[&<>'"]/g, (char) => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", "'":"&#39;", '"':"&quot;" }[char]));
}

function t(key, variables = {}) {
  const value = messages[key] ?? key;
  return value.replace(/\{(\w+)\}/g, (_, name) => String(variables[name] ?? `{${name}}`));
}

async function loadLanguage(preferred) {
  const selected = LANGUAGE_FILES.includes(preferred) ? preferred : "zh-CN";
  try {
    const response = await fetch(`/static/i18n/${selected}.json`, { cache: "no-store" });
    if (!response.ok) throw new Error(`language resource ${selected} unavailable`);
    messages = await response.json();
  } catch (error) {
    if (selected === "zh-CN") throw error;
    const fallback = await fetch("/static/i18n/zh-CN.json", { cache: "no-store" });
    if (!fallback.ok) throw error;
    messages = await fallback.json();
  }
  locale = selected;
  localStorage.setItem(LANGUAGE_KEY, selected);
  document.documentElement.lang = selected;
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const value = t(node.dataset.i18n);
    if (node.dataset.i18nAttr) node.setAttribute(node.dataset.i18nAttr, value);
    else node.textContent = value;
  });
  document.title = t("page.title");
  ["#languageSelect", "#languageSelectApp"].forEach((selector) => {
    const node = $(selector);
    if (node) node.value = selected;
  });
  if (catalog) render(catalog);
  updateGatewayStatus();
}

function preferredLanguage() {
  const saved = localStorage.getItem(LANGUAGE_KEY);
  if (LANGUAGE_FILES.includes(saved)) return saved;
  return navigator.languages?.some((item) => item.toLowerCase().startsWith("zh")) ? "zh-CN" : "en-US";
}

async function api(path, options = {}) {
  let response;
  try {
    response = await fetch(path, { headers: { "Content-Type": "application/json", ...(options.headers || {}) }, ...options });
  } catch (_) {
    throw new Error(t("errors.network"));
  }
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const code = data.error?.code;
    const localized = code ? t(`errors.${code}`) : "";
    throw new Error(localized && localized !== `errors.${code}` ? localized : t("errors.unknown"));
  }
  return data;
}

function capability(kind) { return kind.includes("asr") ? "ASR" : kind.includes("tts") ? "TTS" : "LLM"; }
function capabilityLabel(kind) { return t(`provider.capability.${capability(kind).toLowerCase()}`); }
function providerType(kind) { return providerTypes.find((item) => item.kind === kind); }
function providerKindLabel(kind) {
  const descriptor = providerType(kind);
  return descriptor ? t(descriptor.display_name) : kind;
}
function providerById(id) { return catalog?.providers.find((item) => item.provider_id === id); }
function providerReady(id, expected) {
  const item = providerById(id);
  return Boolean(item?.enabled && capability(item.kind).toLowerCase() === expected && item.runtime_state === "ready");
}
function profileRequirements(pipelineType) {
  return {
    transcription: ["asr"],
    post_call_analysis: ["asr", "llm"],
    llm_task: ["llm"],
    voice_agent: ["asr", "llm", "tts"],
  }[pipelineType] || [];
}
function profileReady(profile) {
  return profileRequirements(profile.pipeline_type).every((type) => providerReady(profile[`${type}_provider_id`], type));
}
function providerStatus(provider) {
  if (!provider.enabled) return `<span class="badge disabled">${escapeHtml(t("badge.disabled"))}</span>`;
  const key = provider.runtime_state === "ready" ? "badge.ready" : "badge.notReady";
  const title = provider.runtime_message ? ` title="${escapeHtml(t(`runtime.${provider.runtime_message}`))}"` : "";
  return `<span class="badge${provider.runtime_state === "ready" ? " ready" : ""}"${title}>${escapeHtml(t(key))}</span>`;
}
function profileStatus(profile) {
  if (!profile.enabled) return `<span class="badge disabled">${escapeHtml(t("badge.disabled"))}</span>`;
  return profileReady(profile) ? `<span class="badge ready">${escapeHtml(t("badge.ready"))}</span>` : `<span class="badge">${escapeHtml(t("badge.notReady"))}</span>`;
}

function fillProfileProviderOptions() {
  const asr = catalog.providers.filter((item) => capability(item.kind) === "ASR");
  const llm = catalog.providers.filter((item) => capability(item.kind) === "LLM");
  const tts = catalog.providers.filter((item) => capability(item.kind) === "TTS");
  const options = (items) => items.map((item) => `<option value="${escapeHtml(item.provider_id)}">${escapeHtml(item.display_name)} (${escapeHtml(item.provider_id)})</option>`).join("");
  $("#profileAsrProvider").innerHTML = options(asr);
  $("#profileLlmProvider").innerHTML = options(llm);
  $("#profileTtsProvider").innerHTML = options(tts);
}

function setProfilePipelineFields() {
  const pipelineType = $("#profilePipelineType").value;
  const required = new Set(profileRequirements(pipelineType));
  [["asr", "#profileAsrField"], ["llm", "#profileLlmField"], ["tts", "#profileTtsField"]].forEach(([type, selector]) => {
    const field = $(selector);
    const active = required.has(type);
    field.hidden = !active;
    field.querySelector("select").disabled = !active;
  });
}

function render(next) {
  catalog = next;
  const enabled = catalog.providers.filter((item) => item.enabled).length;
  const executable = catalog.profiles.filter((profile) => profile.enabled && profileReady(profile)).length;
  $("#catalogVersion").textContent = catalog.version;
  $("#providerEnabled").textContent = `${enabled}/${catalog.providers.length}`;
  $("#profileExecutable").textContent = `${executable}/${catalog.profiles.length}`;
  $("#overviewProfiles").innerHTML = catalog.profiles.map((profile) => `<tr><td>${escapeHtml(profile.profile_id)}</td><td>${profile.profile_version}</td><td>${escapeHtml(t(`pipeline.${pipelineKey(profile.pipeline_type)}`))}</td><td>${escapeHtml(profile.asr_provider_id || "-")}</td><td>${profileStatus(profile)}</td></tr>`).join("") || `<tr><td colspan="5">${escapeHtml(t("empty.profiles"))}</td></tr>`;
  renderProviderRows("asr", "#asrProviderRows");
  renderProviderRows("llm", "#llmProviderRows");
  renderProviderRows("tts", "#ttsProviderRows");
  $("#profileRows").innerHTML = catalog.profiles.map((profile) => `<tr><td>${escapeHtml(profile.profile_id)}</td><td>${profile.profile_version}</td><td>${escapeHtml(t(`pipeline.${pipelineKey(profile.pipeline_type)}`))}</td><td>${escapeHtml(profile.asr_provider_id || "-")}</td><td>${escapeHtml(profile.llm_provider_id || "-")}</td><td>${profileStatus(profile)}</td><td><div class="row-actions"><button class="table-action" type="button" data-edit-profile="${escapeHtml(profile.profile_id)}">${escapeHtml(t("common.edit"))}</button><button class="table-action table-action--danger" type="button" data-delete-profile="${escapeHtml(profile.profile_id)}">${escapeHtml(t("profile.delete"))}</button></div></td></tr>`).join("") || `<tr><td colspan="7">${escapeHtml(t("empty.profiles"))}</td></tr>`;
  fillProfileProviderOptions();
  setProfilePipelineFields();
}

function renderProviderRows(capabilityName, selector) {
  const rows = catalog.providers.filter((provider) => capability(provider.kind).toLowerCase() === capabilityName);
  $(selector).innerHTML = rows.map((provider) => `<tr><td><strong>${escapeHtml(provider.display_name)}</strong><small>${escapeHtml(provider.provider_id)} · ${escapeHtml(t("provider.revision", { revision: provider.revision }))}</small></td><td>${escapeHtml(providerKindLabel(provider.kind))}</td><td>${provider.secret?.configured ? escapeHtml(provider.secret.masked || t("provider.secretConfigured")) : escapeHtml(t("provider.secretMissing"))}</td><td>${providerStatus(provider)}</td><td><div class="row-actions"><button class="table-action" type="button" data-edit-provider="${escapeHtml(provider.provider_id)}">${escapeHtml(t("common.edit"))}</button><button class="table-action table-action--danger" type="button" data-delete-provider="${escapeHtml(provider.provider_id)}">${escapeHtml(t("provider.delete"))}</button></div></td></tr>`).join("") || `<tr><td colspan="5">${escapeHtml(t("empty.providers"))}</td></tr>`;
}

function pipelineKey(type) {
  return ({ transcription: "transcription", post_call_analysis: "postCallAnalysis", llm_task: "llmTask", voice_agent: "voiceAgent" })[type] || type;
}

function updateGatewayStatus() {
  if (catalog) $("#gatewayStatus").textContent = t("status.connected", { version: catalog.version });
}

async function loadCatalog() {
  const [result, types] = await Promise.all([api("/api/catalog"), api("/api/provider-types")]);
  providerTypes = types.provider_types || [];
  render(result.catalog);
  $("#statusDot").className = "status-dot online";
  updateGatewayStatus();
}

function message(id, text, success = false) {
  const node = $(id);
  node.textContent = text;
  node.className = `message${success ? " success" : ""}`;
}

function openDialog(id) {
  const dialog = $(id);
  if (!dialog.open) dialog.showModal();
}

function closeDialog(id) {
  const dialog = $(id);
  if (dialog.open) dialog.close();
}

function setProviderKindFields() {
  renderProviderFields($("#providerKind").value);
  const provider = providerById($("#providerForm [name=provider_id]").value);
  document.querySelectorAll(".provider-secret-hint").forEach((hint) => {
    hint.textContent = provider?.secret?.configured ? t("provider.secretRetain", { mask: provider.secret.masked || "" }) : t("provider.secretRequired");
  });
}

function setProviderCapability(capabilityName) {
  const select = $("#providerKind");
  select.dataset.capability = capabilityName || "";
  const types = providerTypes.filter((item) => !capabilityName || item.capability === capabilityName);
  select.innerHTML = types.map((item) => `<option value="${escapeHtml(item.kind)}">${escapeHtml(t(item.display_name))}</option>`).join("");
}

function renderProviderFields(kind, values = {}, { maskedSecret = false } = {}) {
  const container = $("#providerFields");
  const descriptor = providerType(kind);
  if (!descriptor) {
    container.innerHTML = `<p class="form-hint">${escapeHtml(t("provider.typeUnavailable"))}</p>`;
    return;
  }
  container.innerHTML = descriptor.fields.map((field) => {
    const value = values[field.name] ?? field.default_value ?? "";
    const attrs = [`name="${escapeHtml(field.name)}"`, "data-provider-field=true"];
    if (field.required) attrs.push("required");
    if (field.input === "url") attrs.push('type="url"');
    else if (field.input === "number") attrs.push('type="number"');
    else if (field.input === "password") {
      const displayMaskedSecret = field.secret && maskedSecret && value;
      attrs.push(`type="${displayMaskedSecret ? "text" : "password"}"`, `autocomplete="${displayMaskedSecret ? "off" : "new-password"}"`);
      if (displayMaskedSecret) attrs.push('data-masked-secret="true"');
      if (field.secret) attrs.push('data-provider-secret="true"');
    }
    if (field.min != null) attrs.push(`min="${escapeHtml(field.min)}"`);
    if (field.max != null) attrs.push(`max="${escapeHtml(field.max)}"`);
    if (field.step != null) attrs.push(`step="${escapeHtml(field.step)}"`);
    if (field.input === "select") {
      const options = field.options.map((option) => `<option value="${escapeHtml(option.value)}"${String(value) === option.value ? " selected" : ""}>${escapeHtml(t(option.label))}</option>`).join("");
      return `<label><span>${escapeHtml(t(field.label))}</span><select ${attrs.join(" ")}>${options}</select></label>`;
    }
    const hint = field.secret ? '<small class="provider-secret-hint"></small>' : "";
    return `<label${field.secret ? ' class="secret-field"' : ""}><span>${escapeHtml(t(field.label))}</span><input ${attrs.join(" ")} value="${escapeHtml(value)}">${hint}</label>`;
  }).join("");
  container.querySelectorAll('input[data-provider-secret="true"]').forEach((input) => {
    input.addEventListener("focus", () => {
      if (input.dataset.maskedSecret === "true") input.select();
    });
    input.addEventListener("input", () => {
      if (input.dataset.maskedSecret !== "true") return;
      input.dataset.maskedSecret = "false";
      input.type = "password";
      input.autocomplete = "new-password";
    });
  });
}

function resetProviderForm() {
  const form = $("#providerForm");
  form.reset();
  form.elements.expected_revision.value = "";
  form.elements.provider_id.readOnly = false;
  form.elements.kind.disabled = false;
  setProviderCapability("");
  $("#providerKind").value = providerTypes[0]?.kind || "";
  setProviderKindFields();
  message("#providerMessage", "");
}

function openNewProvider(capabilityName = "asr") {
  resetProviderForm();
  const kindByCapability = { asr: "local_http_asr", llm: "open_ai_compatible_llm", tts: "byte_dance_tts" };
  setProviderCapability(capabilityName);
  const preferred = kindByCapability[capabilityName];
  $("#providerKind").value = providerTypes.some((item) => item.kind === preferred && item.capability === capabilityName)
    ? preferred
    : providerTypes.find((item) => item.capability === capabilityName)?.kind || "";
  setProviderKindFields();
  $("#providerDialogTitle").textContent = t("provider.add");
  openDialog("#providerDialog");
}

function editProvider(providerId) {
  const provider = providerById(providerId);
  if (!provider) return;
  const form = $("#providerForm");
  resetProviderForm();
  form.elements.provider_id.value = provider.provider_id;
  form.elements.provider_id.readOnly = true;
  form.elements.display_name.value = provider.display_name;
  setProviderCapability(capability(provider.kind).toLowerCase());
  form.elements.kind.value = provider.kind;
  form.elements.kind.disabled = true;
  form.elements.enabled.checked = provider.enabled;
  form.elements.expected_revision.value = provider.revision;
  const parameters = { ...(provider.parameters || {}) };
  const secretField = providerType(provider.kind)?.fields.find((field) => field.secret);
  if (secretField && provider.secret?.configured) parameters[secretField.name] = provider.secret.masked || "";
  renderProviderFields(provider.kind, parameters, { maskedSecret: Boolean(provider.secret?.configured) });
  $("#providerDialogTitle").textContent = t("provider.edit");
  message("#providerMessage", "");
  openDialog("#providerDialog");
}

async function deleteProvider(providerId) {
  const provider = providerById(providerId);
  if (!provider || !window.confirm(t("provider.deleteConfirm", { name: provider.display_name }))) return;
  try {
    const result = await api(`/api/providers/${encodeURIComponent(providerId)}`, {
      method: "DELETE",
      body: JSON.stringify({ expected_revision: provider.revision }),
    });
    render(result.catalog);
  } catch (error) {
    window.alert(error.message || t("errors.unknown"));
  }
}

function providerPayload(form) {
  const kind = form.elements.kind.value;
  const common = {
    provider_id: form.elements.provider_id.value.trim(),
    display_name: form.elements.display_name.value.trim(),
    kind,
    enabled: form.elements.enabled.checked,
    expected_revision: form.elements.expected_revision.value ? Number(form.elements.expected_revision.value) : null,
  };
  const descriptor = providerType(kind);
  const parameters = { type: kind };
  let secret = null;
  descriptor?.fields.forEach((field) => {
    const input = form.querySelector(`[name="${field.name}"]`);
    if (!input) return;
    const value = input.value.trim();
    if (field.secret) secret = input.dataset.maskedSecret === "true" ? null : (value || null);
    else parameters[field.name] = field.input === "number" ? Number(value) : value;
  });
  return { ...common, parameters, secret };
}

function editProfile(profileId) {
  const profile = catalog.profiles.find((item) => item.profile_id === profileId);
  if (!profile) return;
  const form = $("#profileForm");
  form.reset();
  fillProfileProviderOptions();
  form.elements.profile_id.value = profile.profile_id;
  form.elements.profile_id.readOnly = true;
  form.elements.profile_version.value = profile.profile_version;
  form.elements.pipeline_type.value = profile.pipeline_type;
  setProfilePipelineFields();
  if (profile.asr_provider_id) form.elements.asr_provider_id.value = profile.asr_provider_id;
  if (profile.llm_provider_id) form.elements.llm_provider_id.value = profile.llm_provider_id;
  if (profile.tts_provider_id) form.elements.tts_provider_id.value = profile.tts_provider_id;
  form.elements.enabled.checked = profile.enabled;
  message("#profileMessage", "");
  $("#profileDialogTitle").textContent = t("profile.edit");
  openDialog("#profileDialog");
}

function openNewProfile() {
  const form = $("#profileForm");
  form.reset();
  form.elements.profile_id.readOnly = false;
  form.elements.profile_version.value = "1";
  form.elements.enabled.checked = true;
  fillProfileProviderOptions();
  setProfilePipelineFields();
  message("#profileMessage", "");
  $("#profileDialogTitle").textContent = t("profile.add");
  openDialog("#profileDialog");
}

async function deleteProfile(profileId) {
  const profile = catalog?.profiles.find((item) => item.profile_id === profileId);
  if (!profile || !window.confirm(t("profile.deleteConfirm", { name: profile.profile_id }))) return;
  try {
    const result = await api(`/api/profiles/${encodeURIComponent(profileId)}`, {
      method: "DELETE",
      body: JSON.stringify({ expected_revision: profile.profile_version }),
    });
    render(result.catalog);
  } catch (error) {
    window.alert(error.message || t("errors.unknown"));
  }
}

async function login(event) {
  event.preventDefault();
  const button = $("#loginButton");
  button.disabled = true;
  button.textContent = t("login.submitting");
  message("#loginMessage", t("login.verifying"));
  try {
    const result = await api("/api/auth/login", { method:"POST", body:JSON.stringify({ username: $("#username").value.trim(), password: $("#password").value }) });
    $("#currentUser").textContent = result.username;
    $("#loginScreen").hidden = true;
    $("#app").hidden = false;
    await showCatalogOrReportError();
  } catch (error) { message("#loginMessage", error.message || t("errors.unknown"));
  } finally { button.disabled = false; button.textContent = t("login.submit"); }
}

async function showCatalogOrReportError() {
  try { await loadCatalog();
  } catch (error) { $("#statusDot").className = "status-dot error"; $("#gatewayStatus").textContent = error.message || t("errors.unknown"); }
}

async function saveProvider(event) {
  event.preventDefault();
  try {
    const result = await api("/api/providers", { method:"PUT", body:JSON.stringify(providerPayload(event.currentTarget)) });
    render(result.catalog);
    closeDialog("#providerDialog");
  } catch (error) { message("#providerMessage", error.message); }
}

async function saveProfile(event) {
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  const pipelineType = form.get("pipeline_type");
  const requires = new Set(profileRequirements(pipelineType));
  try {
    const profileId = form.get("profile_id");
    const payload = { profile_id: profileId, profile_version: Number(form.get("profile_version")), enabled: form.get("enabled") === "on", pipeline_type: pipelineType, asr_provider_id: requires.has("asr") ? form.get("asr_provider_id") : null, llm_provider_id: requires.has("llm") ? form.get("llm_provider_id") : null, tts_provider_id: requires.has("tts") ? form.get("tts_provider_id") : null, capture: { complete_ratio_ppm: 995000, process_min_ratio_ppm: 950000, complete_max_gap_ms: 200, process_max_gap_ms: 5000 } };
    const editing = form.elements.profile_id.readOnly;
    const result = await api(editing ? "/api/profiles/" + encodeURIComponent(profileId) : "/api/profiles", { method:"PUT", body:JSON.stringify(payload) });
    render(result.catalog);
    closeDialog("#profileDialog");
  } catch (error) { message("#profileMessage", error.message); }
}

function changeLanguage(event) { return loadLanguage(event.currentTarget.value); }

function initialize() {
  $("#loginForm").addEventListener("submit", login);
  $("#providerForm").addEventListener("submit", saveProvider);
  $("#profileForm").addEventListener("submit", saveProfile);
  $("#profilePipelineType").addEventListener("change", setProfilePipelineFields);
  $("#providerKind").addEventListener("change", setProviderKindFields);
  ["#addAsrProviderButton", "#addLlmProviderButton", "#addTtsProviderButton"].forEach((selector) => $(selector).addEventListener("click", (event) => openNewProvider(event.currentTarget.dataset.providerCapability)));
  $("#addProfileButton").addEventListener("click", openNewProfile);
  document.querySelectorAll("[data-close-dialog]").forEach((button) => button.addEventListener("click", () => closeDialog(`#${button.dataset.closeDialog}`)));
  ["#asrProviderRows", "#llmProviderRows", "#ttsProviderRows"].forEach((selector) => $(selector).addEventListener("click", (event) => { const id = event.target.dataset.editProvider; if (id) editProvider(id); const deleteId = event.target.dataset.deleteProvider; if (deleteId) deleteProvider(deleteId); }));
  $("#profileRows").addEventListener("click", (event) => { const id = event.target.dataset.editProfile; if (id) editProfile(id); const deleteId = event.target.dataset.deleteProfile; if (deleteId) deleteProfile(deleteId); });
  $("#languageSelect").addEventListener("change", changeLanguage);
  $("#languageSelectApp").addEventListener("change", changeLanguage);
  $("#refresh").addEventListener("click", showCatalogOrReportError);
  $("#logout").addEventListener("click", async () => { await api("/api/auth/logout", { method:"POST" }); window.location.reload(); });
  document.querySelectorAll(".nav-link").forEach((link) => link.addEventListener("click", (event) => { event.preventDefault(); document.querySelectorAll(".nav-link").forEach((item) => item.classList.remove("active")); link.classList.add("active"); document.querySelectorAll(".view").forEach((view) => { const active = `#${view.id}` === link.getAttribute("href"); view.hidden = !active; view.classList.toggle("active", active); }); $("#pageTitle").textContent = t(link.dataset.i18n); }));
  loadLanguage(preferredLanguage()).then(() => {
    setProviderKindFields();
    setProfilePipelineFields();
    return api("/api/auth/me");
  }).then((result) => {
    $("#currentUser").textContent = result.username;
    $("#loginScreen").hidden = true;
    $("#app").hidden = false;
    return showCatalogOrReportError();
  }).catch((error) => { if (error.message && !error.message.includes("AUTH_REQUIRED")) console.warn(error); });
}

initialize();
