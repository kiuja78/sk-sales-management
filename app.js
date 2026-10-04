const STORAGE_KEY = "sk-sales-manager-v1";
const LOCAL_BACKUP_KEY = "sk-sales-manager-backup-v1";
const LOCAL_BACKUP_INDEX_KEY = "sk-sales-manager-backup-index-v1";
const LOCAL_BACKUP_LIMIT = 12;
const DB_LAST_AUTO_BACKUP_KEY = "sk-sales-db-last-auto-v1";
const DB_DRIVE_LAST_BACKUP_KEY = "sk-sales-db-drive-last-v1";

const DURABLE_DB_NAME = "sk-sales-manager-durable-v1";
const DURABLE_STORE_NAME = "snapshots";
const DURABLE_HISTORY_STORE_NAME = "history";
const DURABLE_HISTORY_META_STORE_NAME = "history-meta";
const DURABLE_SNAPSHOT_KEY = "latest";
let durableBackupWriteTimer = 0;
let durableBackupWritePromise = Promise.resolve();

function openDurableBackupDb() {
  return new Promise((resolve, reject) => {
    if (!window.indexedDB) return reject(new Error("IndexedDB unavailable"));
    let settled = false;
    const finishReject = (error) => {
      if (settled) return;
      settled = true;
      window.clearTimeout(timeoutId);
      reject(error);
    };
    const timeoutId = window.setTimeout(() => finishReject(new Error("IndexedDB open timeout")), 2500);
    const request = indexedDB.open(DURABLE_DB_NAME, 3);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(DURABLE_STORE_NAME)) db.createObjectStore(DURABLE_STORE_NAME);
      if (!db.objectStoreNames.contains(DURABLE_HISTORY_STORE_NAME)) db.createObjectStore(DURABLE_HISTORY_STORE_NAME);
      if (!db.objectStoreNames.contains(DURABLE_HISTORY_META_STORE_NAME)) db.createObjectStore(DURABLE_HISTORY_META_STORE_NAME);
    };
    request.onblocked = () => console.warn("[INDEXEDDB] upgrade is blocked by another open tab");
    request.onsuccess = () => {
      const db = request.result;
      db.onversionchange = () => { try { db.close(); } catch (_) {} };
      if (settled) { try { db.close(); } catch (_) {} return; }
      settled = true;
      window.clearTimeout(timeoutId);
      resolve(db);
    };
    request.onerror = () => finishReject(request.error || new Error("IndexedDB open failed"));
  });
}

async function writeDurableBackupSnapshot(sourceState = state, reason = "auto") {
  try {
    const snapshot = {
      backupType: "SK_Sales_Manager_DurableBackup",
      schemaVersion: STATE_SCHEMA_VERSION,
      exportedAt: new Date().toISOString(),
      reason,
      dataCount: stateDataCount(sourceState),
      data: sourceState
    };
    const db = await openDurableBackupDb();
    await new Promise((resolve, reject) => {
      const tx = db.transaction(DURABLE_STORE_NAME, "readwrite");
      const store = tx.objectStore(DURABLE_STORE_NAME);
      const existing = store.get(DURABLE_SNAPSHOT_KEY);
      existing.onsuccess = () => {
        const latest = existing.result?.data;
        if (!isCorePersistedState(latest) || comparePersistenceVersion(sourceState, latest) > 0
          || persistenceStamp(sourceState) === persistenceStamp(latest)) store.put(snapshot, DURABLE_SNAPSHOT_KEY);
      };
      tx.oncomplete = resolve;
      tx.onerror = () => reject(tx.error || new Error("IndexedDB write failed"));
      tx.onabort = () => reject(tx.error || new Error("IndexedDB write aborted"));
    });
    db.close();
    return true;
  } catch (error) {
    console.warn("[DURABLE BACKUP] write failed", error);
    return false;
  }
}

async function readDurableBackupSnapshot() {
  try {
    const db = await openDurableBackupDb();
    const snapshot = await new Promise((resolve, reject) => {
      const tx = db.transaction(DURABLE_STORE_NAME, "readonly");
      const request = tx.objectStore(DURABLE_STORE_NAME).get(DURABLE_SNAPSHOT_KEY);
      request.onsuccess = () => resolve(request.result || null);
      request.onerror = () => reject(request.error || new Error("IndexedDB read failed"));
    });
    db.close();
    if (!isCorePersistedState(snapshot?.data)) return null;
    return snapshot;
  } catch (error) {
    console.warn("[DURABLE BACKUP] read failed", error);
    return null;
  }
}


async function clearDurableBackupSnapshots() {
  try {
    window.clearTimeout(durableBackupWriteTimer);
    durableBackupWriteTimer = 0;
    // 이미 시작된 백업 쓰기가 있다면 먼저 끝낸 뒤 비웁니다. 완전 초기화 직후
    // 늦게 끝난 이전 백업이 다시 살아나는 경쟁 조건을 막습니다.
    try { await durableBackupWritePromise; } catch (_) {}
    if (!window.indexedDB) return true;
    const db = await openDurableBackupDb();
    await new Promise((resolve, reject) => {
      const tx = db.transaction(DURABLE_STORE_NAME, "readwrite");
      tx.objectStore(DURABLE_STORE_NAME).clear();
      tx.oncomplete = resolve;
      tx.onerror = () => reject(tx.error || new Error("IndexedDB clear failed"));
      tx.onabort = () => reject(tx.error || new Error("IndexedDB clear aborted"));
    });
    db.close();
    return true;
  } catch (error) {
    console.warn("[DURABLE BACKUP] clear failed", error);
    return false;
  }
}

function queueDurableBackup(reason = "auto") {
  window.clearTimeout(durableBackupWriteTimer);
  durableBackupWriteTimer = window.setTimeout(() => {
    durableBackupWriteTimer = 0;
    let snapshotState = state;
    try { snapshotState = structuredClone(state); } catch (_) {}
    durableBackupWritePromise = durableBackupWritePromise
      .catch(() => false)
      .then(() => writeDurableBackupSnapshot(snapshotState, reason));
  }, 180);
}


function dbReasonLabel(reason = "auto") {
  const map = {
    auto: "자동백업",
    manual: "수동백업",
    "before-restore": "복구 전 안전백업",
    "before-import": "전체백업 복원 전",
    "before-reset": "완전초기화 전",
    "before-update": "프로그램 업데이트 전"
  };
  return map[reason] || String(reason || "백업");
}

function dbBackupCounts(sourceState = state) {
  return {
    records: Array.isArray(sourceState?.records) ? sourceState.records.length : 0,
    managers: Array.isArray(sourceState?.managers) ? sourceState.managers.length : 0,
    total: stateDataCount(sourceState)
  };
}

function dbHistoryMetadata(snapshot = {}) {
  return {
    id: String(snapshot.id || ""),
    backupType: String(snapshot.backupType || "SK_Sales_Manager_HistoryBackup"),
    schemaVersion: Number(snapshot.schemaVersion || STATE_SCHEMA_VERSION),
    exportedAt: String(snapshot.exportedAt || ""),
    reason: String(snapshot.reason || "manual"),
    reasonLabel: String(snapshot.reasonLabel || dbReasonLabel(snapshot.reason)),
    dataCount: Number(snapshot.dataCount || 0),
    recordCount: Number(snapshot.recordCount || 0),
    managerCount: Number(snapshot.managerCount || 0),
    externalStatus: String(snapshot.externalStatus || "local")
  };
}

async function putDbHistorySnapshot(snapshot) {
  const db = await openDurableBackupDb();
  await new Promise((resolve, reject) => {
    const tx = db.transaction([DURABLE_HISTORY_STORE_NAME, DURABLE_HISTORY_META_STORE_NAME], "readwrite");
    tx.objectStore(DURABLE_HISTORY_STORE_NAME).put(snapshot, snapshot.id);
    tx.objectStore(DURABLE_HISTORY_META_STORE_NAME).put(dbHistoryMetadata(snapshot), snapshot.id);
    tx.oncomplete = resolve;
    tx.onerror = () => reject(tx.error || new Error("DB backup history write failed"));
    tx.onabort = () => reject(tx.error || new Error("DB backup history write aborted"));
  });
  db.close();
}

async function listDbHistorySnapshots() {
  if (IS_PC_APP) return [];
  try {
    const db = await openDurableBackupDb();
    const rows = await new Promise((resolve, reject) => {
      const tx = db.transaction(DURABLE_HISTORY_META_STORE_NAME, "readonly");
      const request = tx.objectStore(DURABLE_HISTORY_META_STORE_NAME).getAll();
      request.onsuccess = () => resolve(Array.isArray(request.result) ? request.result : []);
      request.onerror = () => reject(request.error || new Error("DB backup history read failed"));
    });
    db.close();
    return rows.sort((a, b) => String(b.exportedAt || "").localeCompare(String(a.exportedAt || "")));
  } catch (error) {
    console.warn("[DB HISTORY] list failed", error);
    return [];
  }
}

async function getDbHistorySnapshot(id) {
  try {
    const db = await openDurableBackupDb();
    const snapshot = await new Promise((resolve, reject) => {
      const tx = db.transaction(DURABLE_HISTORY_STORE_NAME, "readonly");
      const request = tx.objectStore(DURABLE_HISTORY_STORE_NAME).get(String(id || ""));
      request.onsuccess = () => resolve(request.result || null);
      request.onerror = () => reject(request.error || new Error("DB backup history item read failed"));
    });
    db.close();
    return snapshot;
  } catch (error) {
    console.warn("[DB HISTORY] item read failed", error);
    return null;
  }
}

async function cleanupDbHistorySnapshots() {
  if (IS_PC_APP) return;
  try {
    const settings = normalizeDbBackupSettings(state.dbBackupSettings);
    const rows = await listDbHistorySnapshots();
    const cutoff = Date.now() - settings.retentionDays * 86400000;
    const removeIds = rows.filter((row, index) => {
      const t = Date.parse(String(row.exportedAt || ""));
      return index >= settings.maxBackups || (Number.isFinite(t) && t < cutoff);
    }).map((row) => row.id).filter(Boolean);
    if (!removeIds.length) return;
    const db = await openDurableBackupDb();
    await new Promise((resolve, reject) => {
      const tx = db.transaction([DURABLE_HISTORY_STORE_NAME, DURABLE_HISTORY_META_STORE_NAME], "readwrite");
      const store = tx.objectStore(DURABLE_HISTORY_STORE_NAME);
      const metaStore = tx.objectStore(DURABLE_HISTORY_META_STORE_NAME);
      removeIds.forEach((id) => { store.delete(id); metaStore.delete(id); });
      tx.oncomplete = resolve;
      tx.onerror = () => reject(tx.error || new Error("DB backup history cleanup failed"));
    });
    db.close();
  } catch (error) {
    console.warn("[DB HISTORY] cleanup failed", error);
  }
}

async function writeDbHistorySnapshot(reason = "manual", sourceState = state, options = {}) {
  if (IS_PC_APP) return null;
  const count = stateDataCount(sourceState);
  if (!count && options.allowEmpty !== true) return null;

  let data = sourceState;
  try { data = structuredClone(sourceState); } catch (_) {}
  const counts = dbBackupCounts(data);
  const snapshot = {
    id: `db-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    backupType: "SK_Sales_Manager_HistoryBackup",
    schemaVersion: STATE_SCHEMA_VERSION,
    exportedAt: new Date().toISOString(),
    reason,
    reasonLabel: dbReasonLabel(reason),
    dataCount: counts.total,
    recordCount: counts.records,
    managerCount: counts.managers,
    externalStatus: "local",
    data
  };

  // 현재 브라우저에도 안전 사본을 남겨 두되, 기준 복구이력은 Supabase입니다.
  await putDbHistorySnapshot(snapshot);
  await cleanupDbHistorySnapshots();

  if (window.SKCloud?.createBackup) {
    try {
      const cloud = await window.SKCloud.createBackup(data, {
        reason,
        reasonLabel: snapshot.reasonLabel,
        recordCount: snapshot.recordCount,
        managerCount: snapshot.managerCount,
        dataCount: snapshot.dataCount
      });
      snapshot.cloudBackupId = cloud?.id || "";
      snapshot.externalStatus = "supabase-ok";
      await putDbHistorySnapshot(snapshot);
      const settings = normalizeDbBackupSettings(state.dbBackupSettings);
      await window.SKCloud.cleanupBackups?.(settings.retentionDays, settings.maxBackups);
      if (reason === "auto") localStorage.setItem(DB_LAST_AUTO_BACKUP_KEY, snapshot.exportedAt);
    } catch (error) {
      console.error("[SUPABASE BACKUP] create failed", error);
      snapshot.externalStatus = "supabase-failed";
      // 자동백업 실패 시 다음 저장에서 다시 시도할 수 있도록 마지막 성공시각은 갱신하지 않습니다.
    }
  } else if (reason === "auto") {
    localStorage.setItem(DB_LAST_AUTO_BACKUP_KEY, snapshot.exportedAt);
  }

  return snapshot;
}

function queueDbAutoBackup() {
  if (IS_PC_APP) return;
  const settings = normalizeDbBackupSettings(state.dbBackupSettings);
  if (!settings.autoEnabled || stateDataCount(state) <= 0) return;
  const lastAt = Date.parse(localStorage.getItem(DB_LAST_AUTO_BACKUP_KEY) || "");
  if (Number.isFinite(lastAt) && Date.now() - lastAt < settings.intervalMinutes * 60000) return;
  window.clearTimeout(dbHistoryWriteTimer);
  dbHistoryWriteTimer = window.setTimeout(() => {
    dbHistoryWriteTimer = 0;
    let snapshotState = state;
    try { snapshotState = structuredClone(state); } catch (_) {}
    dbHistoryWritePromise = dbHistoryWritePromise.catch(() => null).then(async () => {
      const recheckAt = Date.parse(localStorage.getItem(DB_LAST_AUTO_BACKUP_KEY) || "");
      if (Number.isFinite(recheckAt) && Date.now() - recheckAt < settings.intervalMinutes * 60000) return null;
      return writeDbHistorySnapshot("auto", snapshotState);
    }).then((result) => {
      if (result && currentView === "db") refreshDbManagement();
      return result;
    });
  }, 900);
}

function formatDbDateTime(value, fallback = "-") {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return fallback;
  return `${String(date.getFullYear())}.${String(date.getMonth()+1).padStart(2,"0")}.${String(date.getDate()).padStart(2,"0")} ${String(date.getHours()).padStart(2,"0")}:${String(date.getMinutes()).padStart(2,"0")}`;
}

function dbSafeFilePart(value = "backup") {
  return String(value || "backup").replace(/[\\/:*?"<>|]+/g, "_").replace(/\s+/g, "_").slice(0, 48);
}

async function createDbBackup(reason = "manual", options = {}) {
  if (IS_PC_APP) {
    try {
      const saved = await persistState({ immediateServer: true, allowEmptyServer: options.allowEmpty === true, skipDbAutoBackup: true });
      if (saved?.ok === false) throw new Error("현재 데이터 저장 실패");
      const response = await fetch("/api/db/backup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ reason })
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok || result?.ok === false) throw new Error(result?.error || "백업 생성 실패");
      if (!options.silent) showToast(`${dbReasonLabel(reason)}을 저장했습니다.`);
      if (currentView === "db") refreshDbManagement();
      return result;
    } catch (error) {
      console.error("[DB BACKUP] PC backup failed", error);
      if (!options.silent) showToast("백업 저장에 실패했습니다.");
      return null;
    }
  }

  try {
    const snapshot = await writeDbHistorySnapshot(reason, state, { allowEmpty: options.allowEmpty });
    if (!options.silent) {
      if (!snapshot) showToast("백업할 데이터가 없습니다.");
      else if (snapshot.externalStatus === "supabase-ok") showToast(`${dbReasonLabel(reason)}을 Supabase에 저장했습니다.`);
      else showToast(`${dbReasonLabel(reason)}의 로컬 사본은 저장했지만 Supabase 백업을 확인해 주세요.`);
    }
    if (currentView === "db") refreshDbManagement();
    return snapshot;
  } catch (error) {
    console.error("[DB BACKUP] Supabase backup failed", error);
    if (!options.silent) showToast("Supabase 백업 저장에 실패했습니다.");
    return null;
  }
}

function ensureGoogleIdentityScript() {
  if (window.google?.accounts?.oauth2) return Promise.resolve(true);
  if (dbDriveScriptPromise) return dbDriveScriptPromise;
  dbDriveScriptPromise = new Promise((resolve, reject) => {
    const existing = document.querySelector('script[data-sk-google-identity="1"]');
    if (existing) {
      existing.addEventListener("load", () => resolve(true), { once: true });
      existing.addEventListener("error", () => reject(new Error("Google Identity Services 로드 실패")), { once: true });
      return;
    }
    const script = document.createElement("script");
    script.src = "https://accounts.google.com/gsi/client";
    script.async = true;
    script.defer = true;
    script.dataset.skGoogleIdentity = "1";
    script.onload = () => resolve(true);
    script.onerror = () => { dbDriveScriptPromise = null; reject(new Error("Google Identity Services 로드 실패")); };
    document.head.appendChild(script);
  });
  return dbDriveScriptPromise;
}

function validDriveToken() {
  return Boolean(dbDriveAccessToken && Date.now() < dbDriveTokenExpiresAt - 30000);
}

async function connectGoogleDrive() {
  if (IS_PC_APP) return false;
  const clientId = String($("#dbDriveClientId")?.value || state.dbBackupSettings?.driveClientId || "").trim();
  if (!clientId) { showToast("Google OAuth Client ID를 먼저 입력해 주세요."); return false; }
  state.dbBackupSettings = normalizeDbBackupSettings({
    ...state.dbBackupSettings,
    driveClientId: clientId,
    driveFolderName: String($("#dbDriveFolderName")?.value || "").trim() || "SK 영업관리 시스템 백업",
    driveEnabled: Boolean($("#dbDriveEnabled")?.checked),
    driveIntervalHours: Number($("#dbDriveIntervalHours")?.value || 24)
  });
  await persistState({ skipDbAutoBackup: true });
  try {
    await ensureGoogleIdentityScript();
    const token = await new Promise((resolve, reject) => {
      dbDriveTokenClient = window.google.accounts.oauth2.initTokenClient({
        client_id: clientId,
        scope: "https://www.googleapis.com/auth/drive.file",
        callback: (response) => response?.access_token ? resolve(response) : reject(new Error(response?.error || "Google 인증 실패")),
        error_callback: (error) => reject(new Error(error?.type || "Google 인증창 오류"))
      });
      dbDriveTokenClient.requestAccessToken({ prompt: "consent" });
    });
    dbDriveAccessToken = String(token.access_token || "");
    dbDriveTokenExpiresAt = Date.now() + Math.max(60, Number(token.expires_in || 3600)) * 1000;
    try {
      const about = await fetch("https://www.googleapis.com/drive/v3/about?fields=user(displayName,emailAddress)", { headers: { Authorization: `Bearer ${dbDriveAccessToken}` } });
      if (about.ok) {
        const info = await about.json();
        dbDriveAccountLabel = [info?.user?.displayName, info?.user?.emailAddress].filter(Boolean).join(" · ");
      }
    } catch (_) {}
    showToast("Google Drive 연결이 완료되었습니다.");
    renderDbDriveStatus();
    if (currentView === "db") await refreshDbManagement();
    return true;
  } catch (error) {
    console.error("[GOOGLE DRIVE] connect failed", error);
    showToast("Google Drive 연결에 실패했습니다. Client ID와 승인된 웹 원본을 확인해 주세요.");
    renderDbDriveStatus();
    return false;
  }
}

function driveQueryEscape(value = "") {
  return String(value || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

async function ensureDriveBackupFolder() {
  if (!validDriveToken()) throw new Error("Google Drive 연결이 필요합니다.");
  const settings = normalizeDbBackupSettings(state.dbBackupSettings);
  if (settings.driveFolderId) {
    const check = await fetch(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(settings.driveFolderId)}?fields=id,name,mimeType,trashed`, { headers: { Authorization: `Bearer ${dbDriveAccessToken}` } });
    if (check.ok) {
      const item = await check.json();
      if (!item.trashed && item.mimeType === "application/vnd.google-apps.folder" && String(item.name || "") === String(settings.driveFolderName || "")) return item.id;
    }
  }

  // 브라우저 저장소를 삭제하거나 다른 PC에서 접속해 folderId가 없어져도
  // 같은 OAuth Client가 예전에 만든 동일 폴더를 다시 찾아 기존 백업을 복구할 수 있게 합니다.
  const folderName = settings.driveFolderName || "SK 영업관리 시스템 백업";
  const q = `mimeType='application/vnd.google-apps.folder' and name='${driveQueryEscape(folderName)}' and trashed=false`;
  try {
    const found = await fetch(`https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(q)}&fields=files(id,name,createdTime)&orderBy=createdTime%20desc&pageSize=10`, {
      headers: { Authorization: `Bearer ${dbDriveAccessToken}` }
    });
    if (found.ok) {
      const result = await found.json();
      const folder = Array.isArray(result?.files) ? result.files[0] : null;
      if (folder?.id) {
        state.dbBackupSettings = normalizeDbBackupSettings({ ...settings, driveFolderId: String(folder.id) });
        await persistState({ skipDbAutoBackup: true });
        return String(folder.id);
      }
    }
  } catch (_) {}

  const created = await fetch("https://www.googleapis.com/drive/v3/files?fields=id,name", {
    method: "POST",
    headers: { Authorization: `Bearer ${dbDriveAccessToken}`, "Content-Type": "application/json" },
    body: JSON.stringify({ name: folderName, mimeType: "application/vnd.google-apps.folder" })
  });
  if (!created.ok) throw new Error(`Drive 폴더 생성 실패 (${created.status})`);
  const folder = await created.json();
  state.dbBackupSettings = normalizeDbBackupSettings({ ...settings, driveFolderId: String(folder.id || "") });
  await persistState({ skipDbAutoBackup: true });
  return state.dbBackupSettings.driveFolderId;
}

async function uploadBlobToDrive(fileName, blob, folderId, appProperties = {}) {
  const metadata = { name: fileName, parents: [folderId], appProperties };
  if (blob.size <= 4.5 * 1024 * 1024) {
    const boundary = `sk_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    const body = new Blob([
      `--${boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n`, JSON.stringify(metadata),
      `\r\n--${boundary}\r\nContent-Type: application/json\r\n\r\n`, blob,
      `\r\n--${boundary}--`
    ]);
    const response = await fetch("https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,createdTime", {
      method: "POST",
      headers: { Authorization: `Bearer ${dbDriveAccessToken}`, "Content-Type": `multipart/related; boundary=${boundary}` },
      body
    });
    if (!response.ok) throw new Error(`Drive 업로드 실패 (${response.status})`);
    return response.json();
  }
  const init = await fetch("https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&fields=id,name,createdTime", {
    method: "POST",
    headers: { Authorization: `Bearer ${dbDriveAccessToken}`, "Content-Type": "application/json; charset=UTF-8", "X-Upload-Content-Type": "application/json" },
    body: JSON.stringify(metadata)
  });
  if (!init.ok) throw new Error(`Drive 대용량 업로드 준비 실패 (${init.status})`);
  const location = init.headers.get("Location");
  if (!location) throw new Error("Drive 업로드 주소를 받지 못했습니다.");
  const upload = await fetch(location, { method: "PUT", headers: { "Content-Type": "application/json" }, body: blob });
  if (!upload.ok) throw new Error(`Drive 업로드 실패 (${upload.status})`);
  return upload.json();
}

async function maybeBackupSnapshotToGoogleDrive(snapshot, force = false) {
  if (IS_PC_APP || !snapshot?.data) return false;
  const settings = normalizeDbBackupSettings(state.dbBackupSettings);
  if ((!settings.driveEnabled && !force) || !settings.driveClientId) return false;
  if (!validDriveToken()) { renderDbDriveStatus(); return false; }
  const last = Date.parse(localStorage.getItem(DB_DRIVE_LAST_BACKUP_KEY) || "");
  if (!force && Number.isFinite(last) && Date.now() - last < settings.driveIntervalHours * 3600000) return false;
  try {
    const folderId = await ensureDriveBackupFolder();
    const stamp = String(snapshot.exportedAt || new Date().toISOString()).replace(/[-:]/g, "").replace(/\.\d+Z$/, "").replace("T", "_");
    const payload = { backupType: "SK_Sales_Manager_FullBackup", schemaVersion: snapshot.schemaVersion || STATE_SCHEMA_VERSION, exportedAt: snapshot.exportedAt, reason: snapshot.reasonLabel || dbReasonLabel(snapshot.reason), version: versionLabelForDisplay(APP_VERSION), data: snapshot.data };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    await uploadBlobToDrive(
      `SK_Sales_Backup_${stamp}_${dbSafeFilePart(snapshot.reasonLabel || snapshot.reason)}.json`,
      blob,
      folderId,
      {
        skBackup: "1",
        backupId: String(snapshot.id || ""),
        exportedAt: String(snapshot.exportedAt || ""),
        reason: String(snapshot.reason || "manual"),
        reasonLabel: String(snapshot.reasonLabel || dbReasonLabel(snapshot.reason)),
        recordCount: String(Number(snapshot.recordCount || 0)),
        managerCount: String(Number(snapshot.managerCount || 0)),
        schemaVersion: String(Number(snapshot.schemaVersion || STATE_SCHEMA_VERSION))
      }
    );
    localStorage.setItem(DB_DRIVE_LAST_BACKUP_KEY, new Date().toISOString());
    snapshot.externalStatus = "drive-ok";
    try { await putDbHistorySnapshot(snapshot); } catch (_) {}
    renderDbDriveStatus();
    if (currentView === "db") refreshDbManagement();
    return true;
  } catch (error) {
    console.error("[GOOGLE DRIVE] backup failed", error);
    snapshot.externalStatus = "drive-failed";
    try { await putDbHistorySnapshot(snapshot); } catch (_) {}
    if (/401|403/.test(String(error?.message || ""))) { dbDriveAccessToken = ""; dbDriveTokenExpiresAt = 0; }
    renderDbDriveStatus();
    return false;
  }
}


async function listGoogleDriveBackups() {
  if (IS_PC_APP || !validDriveToken()) return [];
  try {
    const folderId = await ensureDriveBackupFolder();
    const q = `'${driveQueryEscape(folderId)}' in parents and trashed=false`;
    const fields = "files(id,name,createdTime,modifiedTime,size,appProperties)";
    const response = await fetch(`https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(q)}&fields=${encodeURIComponent(fields)}&orderBy=createdTime%20desc&pageSize=1000`, {
      headers: { Authorization: `Bearer ${dbDriveAccessToken}` }
    });
    if (!response.ok) throw new Error(`Drive 백업 목록 확인 실패 (${response.status})`);
    const result = await response.json();
    return (Array.isArray(result?.files) ? result.files : [])
      .filter((file) => /^SK_Sales_Backup_.*\.json$/i.test(String(file?.name || "")))
      .map((file) => {
        const props = file?.appProperties && typeof file.appProperties === "object" ? file.appProperties : {};
        return {
          id: `drive:${String(file.id || "")}`,
          driveFileId: String(file.id || ""),
          localBackupId: String(props.backupId || ""),
          exportedAt: String(props.exportedAt || file.createdTime || file.modifiedTime || ""),
          reason: String(props.reason || "manual"),
          reasonLabel: String(props.reasonLabel || "Google Drive 백업"),
          recordCount: Number(props.recordCount || 0),
          managerCount: Number(props.managerCount || 0),
          schemaVersion: Number(props.schemaVersion || STATE_SCHEMA_VERSION),
          source: "drive",
          externalStatus: "drive-only",
          fileName: String(file.name || "")
        };
      });
  } catch (error) {
    console.warn("[GOOGLE DRIVE] history list failed", error);
    return [];
  }
}

async function downloadGoogleDriveBackup(fileId) {
  if (!validDriveToken()) throw new Error("Google Drive 연결이 필요합니다.");
  const response = await fetch(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}?alt=media`, {
    headers: { Authorization: `Bearer ${dbDriveAccessToken}` }
  });
  if (!response.ok) throw new Error(`Drive 백업 다운로드 실패 (${response.status})`);
  const payload = await response.json();
  const data = extractBackupData(payload);
  if (!looksLikeBackupData(data)) throw new Error("Drive 백업 데이터 형식이 올바르지 않습니다.");
  return { payload, data };
}

function mergeDbHistoryWithDrive(localRows = [], driveRows = []) {
  const merged = localRows.map((row) => ({ ...row }));
  const byId = new Map(merged.map((row, index) => [String(row.id || ""), index]).filter(([id]) => id));
  const bySignature = new Map(merged.map((row, index) => [`${String(row.exportedAt || "")}|${String(row.reason || "")}`, index]));
  driveRows.forEach((row) => {
    let index = row.localBackupId ? byId.get(String(row.localBackupId)) : undefined;
    if (index === undefined) index = bySignature.get(`${String(row.exportedAt || "")}|${String(row.reason || "")}`);
    if (index !== undefined) {
      merged[index] = { ...merged[index], externalStatus: "drive-ok", driveFileId: row.driveFileId || merged[index].driveFileId || "" };
      return;
    }
    merged.push({ ...row });
  });
  return merged.sort((a, b) => String(b.exportedAt || "").localeCompare(String(a.exportedAt || "")));
}

function disconnectGoogleDrive() {
  if (dbDriveAccessToken && window.google?.accounts?.oauth2?.revoke) {
    try { window.google.accounts.oauth2.revoke(dbDriveAccessToken, () => {}); } catch (_) {}
  }
  dbDriveAccessToken = "";
  dbDriveTokenExpiresAt = 0;
  dbDriveAccountLabel = "";
  dbDriveTokenClient = null;
  renderDbDriveStatus();
  if (currentView === "db") void refreshDbManagement();
  showToast("Google Drive 연결을 해제했습니다.");
}

function renderDbDriveStatus() {
  const settings = normalizeDbBackupSettings(state.dbBackupSettings);
  const pill = $("#dbDriveStatePill");
  const label = $("#dbDriveConnectionLabel");
  const lastLabel = $("#dbDriveLastBackupLabel");
  if (pill) {
    pill.className = `db-state-pill ${validDriveToken() ? "is-ok" : (settings.driveEnabled ? "is-warn" : "")}`;
    pill.textContent = validDriveToken() ? "연결됨" : (settings.driveEnabled ? "연결 필요" : "사용 안 함");
  }
  if (label) label.textContent = validDriveToken() ? `Google 연결됨${dbDriveAccountLabel ? ` · ${dbDriveAccountLabel}` : ""}` : "Google 계정이 연결되지 않았습니다.";
  const last = localStorage.getItem(DB_DRIVE_LAST_BACKUP_KEY) || "";
  if (lastLabel) lastLabel.textContent = last ? `최근 Drive 백업 ${formatDbDateTime(last)}` : "최근 Drive 백업 없음";
}

async function backupGoogleDriveNow() {
  if (IS_PC_APP) return;
  if (!validDriveToken()) {
    const ok = await connectGoogleDrive();
    if (!ok) return;
  }
  const snapshot = await writeDbHistorySnapshot("manual", state, { tryDrive: false });
  if (!snapshot) { showToast("백업할 데이터가 없습니다."); return; }
  showToast("Google Drive 백업을 진행하고 있습니다...");
  const ok = await maybeBackupSnapshotToGoogleDrive(snapshot, true);
  showToast(ok ? "Google Drive 안전백업을 완료했습니다." : "Google Drive 백업에 실패했습니다.");
}

async function fetchPcDbStatus() {
  const response = await fetch("/api/db/status", { cache: "no-store" });
  const result = await response.json().catch(() => ({}));
  if (!response.ok || result?.ok === false) throw new Error(result?.error || "DB 상태 확인 실패");
  return result;
}

function dbHistoryStatusLabel(row) {
  if (IS_PC_APP) {
    if (row.secondaryAttempted && row.secondaryOk) return { text: "기본 + 2차", cls: "is-ok" };
    if (row.secondaryAttempted && !row.secondaryOk) return { text: "기본 저장 · 2차 실패", cls: "is-warn" };
    return { text: "기본 저장", cls: "is-ok" };
  }
  if (row.source === "supabase" || String(row.id || "").startsWith("supabase:")) return { text: "Supabase", cls: "is-ok" };
  if (row.externalStatus === "supabase-ok") return { text: "Supabase + 로컬", cls: "is-ok" };
  if (row.externalStatus === "supabase-failed") return { text: "로컬만 저장", cls: "is-warn" };
  return { text: "브라우저 안전사본", cls: "is-ok" };
}

function renderDbHistoryRows(rows = []) {
  const body = $("#dbHistoryTableBody");
  const mobile = $("#dbHistoryMobileList");
  if (!body || !mobile) return;
  if (!rows.length) {
    body.innerHTML = '<tr><td colspan="6" class="empty-state">아직 생성된 백업 이력이 없습니다.</td></tr>';
    mobile.innerHTML = '<div class="empty-state">아직 생성된 백업 이력이 없습니다.</div>';
    return;
  }
  body.innerHTML = rows.map((row) => {
    const status = dbHistoryStatusLabel(row);
    const id = escapeHtml(String(row.id || row.fileName || ""));
    return `<tr><td><b>${escapeHtml(formatDbDateTime(row.exportedAt))}</b></td><td><span class="db-history-type">${escapeHtml(row.reasonLabel || dbReasonLabel(row.reason))}</span></td><td>${Number(row.recordCount || 0).toLocaleString()}건</td><td>${Number(row.managerCount || 0).toLocaleString()}명</td><td><span class="db-history-status ${status.cls}">${escapeHtml(status.text)}</span></td><td><div class="db-history-actions"><button class="db-restore" type="button" data-db-restore="${id}">복구</button><button type="button" data-db-export-history="${id}">내보내기</button></div></td></tr>`;
  }).join("");
  mobile.innerHTML = rows.map((row) => {
    const status = dbHistoryStatusLabel(row);
    const id = escapeHtml(String(row.id || row.fileName || ""));
    return `<article class="db-history-mobile-card"><div class="db-history-mobile-card-head"><strong>${escapeHtml(formatDbDateTime(row.exportedAt))}</strong><span class="db-history-type">${escapeHtml(row.reasonLabel || dbReasonLabel(row.reason))}</span></div><div class="db-history-mobile-card-meta"><span>접수 ${Number(row.recordCount || 0).toLocaleString()}건</span><span>매니저 ${Number(row.managerCount || 0).toLocaleString()}명</span><span class="db-history-status ${status.cls}">${escapeHtml(status.text)}</span></div><div class="db-history-mobile-card-actions"><button class="db-restore" type="button" data-db-restore="${id}">복구</button><button type="button" data-db-export-history="${id}">내보내기</button></div></article>`;
  }).join("");
}

function populateDbSettingsControls(pcStatus = null) {
  const settings = normalizeDbBackupSettings(state.dbBackupSettings);
  if ($("#dbAutoEnabled")) $("#dbAutoEnabled").checked = pcStatus ? pcStatus.settings.autoEnabled !== false : settings.autoEnabled;
  if ($("#dbAutoInterval")) $("#dbAutoInterval").value = String(pcStatus ? pcStatus.settings.intervalMinutes : settings.intervalMinutes);
  if ($("#dbRetentionDays")) $("#dbRetentionDays").value = String(pcStatus ? pcStatus.settings.retentionDays : settings.retentionDays);
  if ($("#dbMaxBackups")) $("#dbMaxBackups").value = String(pcStatus ? pcStatus.settings.maxBackups : settings.maxBackups);
  if (!IS_PC_APP) {
    if ($("#dbDriveClientId")) $("#dbDriveClientId").value = settings.driveClientId;
    if ($("#dbDriveFolderName")) $("#dbDriveFolderName").value = settings.driveFolderName;
    if ($("#dbDriveEnabled")) $("#dbDriveEnabled").checked = settings.driveEnabled;
    if ($("#dbDriveIntervalHours")) $("#dbDriveIntervalHours").value = String(settings.driveIntervalHours);
    renderDbDriveStatus();
  }
}

async function refreshDbManagement() {
  if (!$("#dbView")) return;
  const seq = ++dbRefreshSequence;
  const counts = dbBackupCounts(state);

  if ($("#dbCurrentDataCount")) $("#dbCurrentDataCount").textContent = `${counts.records.toLocaleString()}건`;
  if ($("#dbCurrentDataDetail")) $("#dbCurrentDataDetail").textContent = `매니저 ${counts.managers.toLocaleString()}명 · 저장항목 ${counts.total.toLocaleString()}개`;

  try {
    if (IS_PC_APP) {
      const status = await fetchPcDbStatus();
      if (seq !== dbRefreshSequence) return;
      const rows = Array.isArray(status.history) ? status.history : [];
      if ($("#dbCloudStatus")) $("#dbCloudStatus").textContent = "PC 저장";
      if ($("#dbCloudAccount")) $("#dbCloudAccount").textContent = "데스크톱 로컬 저장 방식";
      if ($("#dbCloudLastSaved")) $("#dbCloudLastSaved").textContent = rows[0] ? formatDbDateTime(rows[0].exportedAt) : "-";
      if ($("#dbCloudRevision")) $("#dbCloudRevision").textContent = "PC 백업 이력 기준";
      if ($("#dbHistoryCount")) $("#dbHistoryCount").textContent = `${rows.length.toLocaleString()}개`;
      if ($("#dbHistoryCountDetail")) $("#dbHistoryCountDetail").textContent = `기본 위치 · ${status.settings?.retentionDays || 30}일 보관`;
      populateDbSettingsControls(status);
      renderDbHistoryRows(rows);
      return;
    }

    const settings = normalizeDbBackupSettings(state.dbBackupSettings);
    const [cloudStatus, cloudRows, localRows] = await Promise.all([
      window.SKCloud?.getStatus?.(),
      window.SKCloud?.listBackups?.(Math.max(200, settings.maxBackups || 100)),
      listDbHistorySnapshots()
    ]);
    if (seq !== dbRefreshSequence) return;

    if ($("#dbCloudStatus")) $("#dbCloudStatus").textContent = cloudStatus?.connected ? "연결됨" : "확인 필요";
    if ($("#dbCloudAccount")) $("#dbCloudAccount").textContent = cloudStatus?.userEmail ? `로그인 · ${cloudStatus.userEmail}` : "Supabase 로그인 계정";
    if ($("#dbCloudLastSaved")) $("#dbCloudLastSaved").textContent = cloudStatus?.updatedAt ? formatDbDateTime(cloudStatus.updatedAt) : "저장 이력 없음";
    if ($("#dbCloudRevision")) $("#dbCloudRevision").textContent = `Revision ${Number(cloudStatus?.revision || 0).toLocaleString()} · 실시간 중앙저장`;

    const pill = $("#dbCloudStatePill");
    if (pill) {
      pill.className = `db-state-pill ${cloudStatus?.connected ? "is-ok" : "is-warn"}`;
      pill.textContent = cloudStatus?.connected ? "정상 연결" : "확인 필요";
    }

    const mappedRows = (Array.isArray(cloudRows) ? cloudRows : []).map((row) => ({
      id: `supabase:${row.id}`,
      cloudBackupId: row.id,
      exportedAt: row.createdAt,
      reason: row.reason,
      reasonLabel: row.reasonLabel,
      recordCount: row.recordCount,
      managerCount: row.managerCount,
      dataCount: row.dataCount,
      revision: row.revision,
      source: "supabase"
    }));

    if ($("#dbHistoryCount")) $("#dbHistoryCount").textContent = `${mappedRows.length.toLocaleString()}개`;
    if ($("#dbHistoryCountDetail")) $("#dbHistoryCountDetail").textContent = `Supabase · ${settings.retentionDays}일 / 최대 ${settings.maxBackups}개`;
    if ($("#dbBrowserCacheStatus")) $("#dbBrowserCacheStatus").textContent = `정상 · 로컬 안전사본 ${localRows.length.toLocaleString()}개`;

    populateDbSettingsControls();
    renderDbHistoryRows(mappedRows);
  } catch (error) {
    console.error("[DB MANAGEMENT] Supabase refresh failed", error);
    if ($("#dbCloudStatus")) $("#dbCloudStatus").textContent = "확인 실패";
    if ($("#dbCloudAccount")) $("#dbCloudAccount").textContent = "Supabase 연결상태를 확인해 주세요.";
    const pill = $("#dbCloudStatePill");
    if (pill) { pill.className = "db-state-pill is-warn"; pill.textContent = "확인 실패"; }
    renderDbHistoryRows([]);
  }
}

function renderDbManagement() {
  if (!IS_PC_APP) populateDbSettingsControls();
  void refreshDbManagement();
}

async function saveDbBackupSettings() {
  const common = {
    autoEnabled: Boolean($("#dbAutoEnabled")?.checked),
    intervalMinutes: Number($("#dbAutoInterval")?.value || 60),
    retentionDays: Number($("#dbRetentionDays")?.value || 30),
    maxBackups: Number($("#dbMaxBackups")?.value || 100)
  };

  if (IS_PC_APP) {
    try {
      const response = await fetch("/api/db/settings", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(common)
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok || result?.ok === false) throw new Error(result?.error || "설정 저장 실패");
      showToast("복구백업 설정을 저장했습니다.");
      refreshDbManagement();
    } catch (error) {
      console.error("[DB SETTINGS] PC save failed", error);
      showToast("백업 설정 저장에 실패했습니다.");
    }
    return;
  }

  state.dbBackupSettings = normalizeDbBackupSettings({
    ...state.dbBackupSettings,
    ...common,
    driveEnabled: false,
    driveClientId: "",
    driveFolderId: ""
  });
  await persistState({ skipDbAutoBackup: true });
  await cleanupDbHistorySnapshots();
  try {
    await window.SKCloud?.cleanupBackups?.(common.retentionDays, common.maxBackups);
  } catch (error) {
    console.warn("[SUPABASE BACKUP] cleanup failed", error);
  }
  showToast("Supabase 복구백업 설정을 저장했습니다.");
  refreshDbManagement();
}

async function selectPcSecondaryBackupFolder() {
  try {
    const response = await fetch("/api/db/select-folder", { method: "POST" });
    const result = await response.json().catch(() => ({}));
    if (!response.ok || result?.ok === false) throw new Error(result?.error || "폴더 선택 실패");
    if (result.cancelled) return;
    if ($("#dbPcSecondaryFolder")) $("#dbPcSecondaryFolder").value = result.folder || "";
    if ($("#dbPcSecondaryEnabled")) $("#dbPcSecondaryEnabled").checked = Boolean(result.folder);
    await saveDbBackupSettings();
  } catch (error) { console.error("[DB FOLDER] select failed", error); showToast("2차 백업 폴더를 선택하지 못했습니다."); }
}

async function clearPcSecondaryBackupFolder() {
  if (!window.confirm("2차 안전백업 폴더 설정을 해제할까요? 기본 자동백업은 계속 유지됩니다.")) return;
  if ($("#dbPcSecondaryFolder")) $("#dbPcSecondaryFolder").value = "";
  if ($("#dbPcSecondaryEnabled")) $("#dbPcSecondaryEnabled").checked = false;
  await saveDbBackupSettings();
}

async function openPcBackupFolder() {
  try {
    const response = await fetch("/api/db/open-folder", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ preferSecondary: Boolean($("#dbPcSecondaryEnabled")?.checked) }) });
    const result = await response.json().catch(() => ({}));
    if (!response.ok || result?.ok === false) throw new Error(result?.error || "폴더 열기 실패");
  } catch (error) { console.error("[DB FOLDER] open failed", error); showToast("백업 폴더를 열지 못했습니다."); }
}

async function restoreDbHistory(id) {
  if (!id) return;
  try {
    let target = null;

    if (IS_PC_APP) {
      const status = await fetchPcDbStatus();
      target = (status.history || []).find((row) => String(row.id || row.fileName) === String(id));
    } else if (String(id).startsWith("supabase:")) {
      const backupId = String(id).slice("supabase:".length);
      const cloud = await window.SKCloud?.getBackup?.(backupId);
      if (cloud?.data) {
        target = {
          id,
          data: cloud.data,
          recordCount: cloud.recordCount,
          managerCount: cloud.managerCount,
          exportedAt: cloud.createdAt,
          reason: cloud.reason,
          reasonLabel: cloud.reasonLabel,
          source: "supabase"
        };
      }
    } else {
      target = await getDbHistorySnapshot(id);
    }

    if (!target) { showToast("선택한 백업을 찾지 못했습니다."); return; }
    const ok = await confirmBackupRestoreInApp(Number(target.recordCount || 0), Number(target.managerCount || 0));
    if (!ok) return;

    showToast("현재 데이터를 Supabase에 안전백업한 뒤 복구를 진행합니다...");

    if (IS_PC_APP) {
      const response = await fetch("/api/db/restore", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id })
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok || result?.ok === false || !result.data) throw new Error(result?.error || "백업 복구 실패");
      state = normalizeState(result.data);
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
      await persistState({ immediateServer: true, allowEmptyServer: true, skipDbAutoBackup: true });
    } else {
      await writeDbHistorySnapshot("before-restore", state);
      const currentRevision = Number(state?.appMeta?.persistRevision || 0);
      state = normalizeState(target.data);
      state.appMeta = state.appMeta || {};
      state.appMeta.persistRevision = Math.max(currentRevision, Number(state.appMeta.persistRevision || 0));
      const saved = await persistState({ allowEmptyServer: true, skipDbAutoBackup: true });
      if (!saved.ok) throw new Error("복구 데이터 저장 실패");
    }

    invalidateManagerCaches();
    selectedRecordId = "";
    renderNow();
    showToast("선택한 Supabase 백업으로 복구를 완료했습니다.");
    await refreshDbManagement();
  } catch (error) {
    console.error("[DB RESTORE] failed", error);
    showToast("백업 복구 중 오류가 발생했습니다.");
  }
}

async function exportDbHistory(id) {
  if (!id) return;

  if (IS_PC_APP) {
    const link = document.createElement("a");
    link.href = `/api/db/export?id=${encodeURIComponent(id)}`;
    link.style.display = "none";
    document.body.appendChild(link);
    link.click();
    link.remove();
    return;
  }

  let snapshot = null;
  if (String(id).startsWith("supabase:")) {
    const backupId = String(id).slice("supabase:".length);
    const cloud = await window.SKCloud?.getBackup?.(backupId);
    if (cloud?.data) {
      snapshot = {
        schemaVersion: STATE_SCHEMA_VERSION,
        exportedAt: cloud.createdAt,
        reason: cloud.reason,
        reasonLabel: cloud.reasonLabel,
        data: cloud.data
      };
    }
  } else {
    snapshot = await getDbHistorySnapshot(id);
  }

  if (!snapshot?.data) { showToast("선택한 백업을 찾지 못했습니다."); return; }

  const payload = {
    backupType: "SK_Sales_Manager_FullBackup",
    schemaVersion: snapshot.schemaVersion || STATE_SCHEMA_VERSION,
    exportedAt: snapshot.exportedAt,
    reason: snapshot.reasonLabel || dbReasonLabel(snapshot.reason),
    version: versionLabelForDisplay(APP_VERSION),
    data: snapshot.data
  };
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json;charset=utf-8" });
  downloadBlob(
    `SK_DB_Backup_${String(snapshot.exportedAt || "").slice(0,19).replace(/[:T-]/g, "")}.json`,
    "application/json;charset=utf-8",
    blob
  );
}

function attachDbManagementEvents() {
  $("#dbBackupNowBtn")?.addEventListener("click", () => createDbBackup("manual"));
  $("#dbRefreshBtn")?.addEventListener("click", refreshDbManagement);
  $("#dbHistoryRefreshBtn")?.addEventListener("click", refreshDbManagement);
  $("#dbSaveSettingsBtn")?.addEventListener("click", saveDbBackupSettings);
  $("#dbDriveConnectBtn")?.addEventListener("click", connectGoogleDrive);
  $("#dbDriveBackupNowBtn")?.addEventListener("click", backupGoogleDriveNow);
  $("#dbDriveDisconnectBtn")?.addEventListener("click", disconnectGoogleDrive);
  $("#dbPcSelectFolderBtn")?.addEventListener("click", selectPcSecondaryBackupFolder);
  $("#dbPcClearFolderBtn")?.addEventListener("click", clearPcSecondaryBackupFolder);
  $("#dbPcOpenFolderBtn")?.addEventListener("click", openPcBackupFolder);
  $("#dbPcBackupNowBtn")?.addEventListener("click", () => createDbBackup("manual", { forceExternal: true }));
  $("#dbView")?.addEventListener("click", (event) => {
    const restore = event.target.closest?.("[data-db-restore]");
    if (restore) { restoreDbHistory(restore.dataset.dbRestore); return; }
    const exp = event.target.closest?.("[data-db-export-history]");
    if (exp) exportDbHistory(exp.dataset.dbExportHistory);
  });
}

const categories = ["신규", "패키지", "재렌탈", "일시불", "맴버쉽"];
const mainCategories = ["신규", "패키지", "재렌탈", "일시불"];
const activityTypes = ["", "컨스", "지원"];
const sellerRoles = ["", "지국장", "팀장"];
const membershipContactRoles = ["", "매니저", "지국장", "팀장", "고객센터"];
const statuses = ["접수", "요청", "확인", "완료", "보류", "취소"];
const settingsEditMode = { user: false, manager: false, team: false, goal: false };
const OPTIONAL_MENU_DEFAULTS = { checklist: true, analytics: true, evaluation: true, renewalguide: true, payroll: true, contactnote: false, contactrequest: false };
let managerSettingsDeletedIds = new Set();

function normalizeMenuVisibility(value = {}) {
  const source = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  return Object.fromEntries(Object.entries(OPTIONAL_MENU_DEFAULTS).map(([key, defaultValue]) => [
    key,
    typeof source[key] === "boolean" ? source[key] : defaultValue
  ]));
}


function defaultDbBackupSettings() {
  return {
    autoEnabled: true,
    intervalMinutes: 60,
    retentionDays: 30,
    maxBackups: 100,
    driveEnabled: false,
    driveClientId: "",
    driveFolderName: "SK 영업관리 시스템 백업",
    driveFolderId: "",
    driveIntervalHours: 24
  };
}

function normalizeDbBackupSettings(value = {}) {
  const source = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const interval = [30, 60, 180, 360].includes(Number(source.intervalMinutes)) ? Number(source.intervalMinutes) : 60;
  const retention = [7, 30, 60, 90].includes(Number(source.retentionDays)) ? Number(source.retentionDays) : 30;
  const maxBackups = [30, 60, 100, 200].includes(Number(source.maxBackups)) ? Number(source.maxBackups) : 100;
  const driveIntervalHours = [6, 12, 24].includes(Number(source.driveIntervalHours)) ? Number(source.driveIntervalHours) : 24;
  return {
    ...defaultDbBackupSettings(),
    ...source,
    autoEnabled: source.autoEnabled !== false,
    intervalMinutes: interval,
    retentionDays: retention,
    maxBackups,
    driveEnabled: Boolean(source.driveEnabled),
    driveClientId: String(source.driveClientId || "").trim(),
    driveFolderName: String(source.driveFolderName || "SK 영업관리 시스템 백업").trim() || "SK 영업관리 시스템 백업",
    driveFolderId: String(source.driveFolderId || "").trim(),
    driveIntervalHours
  };
}

const IS_PC_APP = typeof STATE_API_URL !== "undefined";
let dbHistoryWriteTimer = 0;
let dbHistoryWritePromise = Promise.resolve();
let dbRefreshSequence = 0;
let dbDriveAccessToken = "";
let dbDriveTokenExpiresAt = 0;
let dbDriveAccountLabel = "";
let dbDriveTokenClient = null;
let dbDriveScriptPromise = null;

function optionalMenuVisibility() {
  state.menuVisibility = normalizeMenuVisibility(state.menuVisibility);
  return state.menuVisibility;
}

function applyOptionalMenuVisibility() {
  const visibility = optionalMenuVisibility();
  Object.entries(visibility).forEach(([view, visible]) => {
    const nav = document.querySelector(`.nav-item[data-view="${view}"]`);
    if (nav) nav.hidden = !visible;
  });
  syncMobileMenuVisibility();
}

function defaultSalesAnalyticsSettings() {
  return {
    branchStartMode: "auto",
    branchStartMonth: "",
    monthStatusMode: "auto",
    monthDataStatus: {},
    sellerAliases: [],
    hiddenSellers: [],
    sellerStartMonths: {},
    productRules: []
  };
}

function normalizeSalesAnalyticsSettings(value = {}) {
  const source = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const branchStartMode = ["auto", "manual"].includes(String(source.branchStartMode || ""))
    ? String(source.branchStartMode)
    : (/^\d{4}-\d{2}$/.test(String(source.branchStartMonth || "")) ? "manual" : "auto");
  const monthStatusMode = ["auto", "manual"].includes(String(source.monthStatusMode || ""))
    ? String(source.monthStatusMode)
    : (source.monthDataStatus && Object.keys(source.monthDataStatus).length ? "manual" : "auto");
  const branchStartMonth = /^\d{4}-\d{2}$/.test(String(source.branchStartMonth || ""))
    ? String(source.branchStartMonth)
    : "";

  const monthDataStatus = {};
  Object.entries(source.monthDataStatus && typeof source.monthDataStatus === "object" ? source.monthDataStatus : {})
    .forEach(([month, status]) => {
      if (/^\d{4}-\d{2}$/.test(month) && ["미입력", "입력완료"].includes(String(status))) monthDataStatus[month] = String(status);
    });

  const sellerAliases = (Array.isArray(source.sellerAliases) ? source.sellerAliases : [])
    .map((item) => ({
      source: String(item?.source || "").trim(),
      target: String(item?.target || "").trim(),
      startMonth: /^\d{4}-\d{2}$/.test(String(item?.startMonth || "")) ? String(item.startMonth) : ""
    }))
    .filter((item) => item.source && item.target && item.startMonth);

  const hiddenSellers = [...new Set((Array.isArray(source.hiddenSellers) ? source.hiddenSellers : [])
    .map((item) => String(item || "").trim()).filter(Boolean))];

  const sellerStartMonths = {};
  Object.entries(source.sellerStartMonths && typeof source.sellerStartMonths === "object" ? source.sellerStartMonths : {})
    .forEach(([name, month]) => {
      const sellerName = String(name || "").trim();
      const startMonth = String(month || "").trim();
      if (sellerName && /^\d{4}-\d{2}$/.test(startMonth)) sellerStartMonths[sellerName] = startMonth;
    });

  const productRules = (Array.isArray(source.productRules) ? source.productRules : [])
    .map((rule) => {
      let family = String(rule?.family || "").trim();
      if (["인덕션/전기레인지", "전기레인지"].includes(family)) family = "인덕션";
      const terms = (Array.isArray(rule?.terms) ? rule.terms : String(rule?.terms || "").split(","))
        .map((term) => String(term || "").trim()).filter(Boolean);
      return { family, terms: [...new Set(terms)] };
    })
    .filter((rule) => rule.family && rule.terms.length);

  return { branchStartMode, branchStartMonth, monthStatusMode, monthDataStatus, sellerAliases, hiddenSellers, sellerStartMonths, productRules };
}

function evaluationNullableNumber(value) {
  if (value === "" || value === null || value === undefined) return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function defaultManagementEvaluationInput() {
  return {
    primaryRelativeScore: null,
    retentionRate: null,
    cancellationRate: null,
    managerChange: null,
    inspectionTotalAccount: null,
    inspectionCancel: null,
    inspectionExceptionHold: null,
    inspectionHold: null,
    inspectionCompleted: null,
    happyTalkRate: null,
    aTeamMassageUnits: null,
    aTeamMattressCareUnits: null,
    policyManual: {}
  };
}

function normalizeManagementEvaluationInput(value = {}) {
  const defaults = defaultManagementEvaluationInput();
  const source = value && typeof value === "object" ? value : {};
  const result = {};
  Object.keys(defaults).forEach((key) => {
    if (key !== "policyManual") result[key] = evaluationNullableNumber(source[key]);
  });
  result.policyManual = Object.fromEntries(
    Object.entries(source.policyManual && typeof source.policyManual === "object" ? source.policyManual : {})
      .map(([key, item]) => [String(key || "").trim(), evaluationNullableNumber(item)])
      .filter(([key]) => key)
  );
  return result;
}

function normalizeManagementEvaluationInputs(value = {}) {
  const result = {};
  const source = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  Object.entries(source).forEach(([month, item]) => {
    if (/^\d{4}-\d{2}$/.test(month)) result[month] = normalizeManagementEvaluationInput(item);
  });
  return result;
}

function evaluationKeywordList(value) {
  const source = Array.isArray(value) ? value : String(value || "").split(/[,\n]/);
  return [...new Set(source.map((item) => String(item || "").trim().normalize("NFKC").toUpperCase()).filter(Boolean))];
}

function defaultManagementEvaluationProductRules(type) {
  const primary = [
    ["정수기", "CP-ABSC, CP-AHSC, CP-AMS, CP-SS, CP-ABS, CP-ACS, CP-AAS, CP-AHS, CP-AQS, CP-WRO, CP-QRO, CP-AJS"],
    ["비데", "CBT-KFF, CBT-Q, CBT-P"],
    ["안마의자", "CMS-"],
    ["음식물처리기", "CFD-151"]
  ];
  const highValue = [
    ["정수기", "CP-ABSC, CP-AHSC, CP-AMS, CP-SS, CP-ABS, CP-ACS, CP-AAS, CP-AHS, CP-AQS, CP-WRO, CP-QRO, CP-AJS"],
    ["공기청정기", "AC-28AH, AC-25W, AC-28W, AC-35U, AC-40AB"],
    ["비데", "CBT-QSB, CBT-KFF, CBT-QSF"],
    ["안마의자", "CMS-"],
    ["음식물처리기", "CFD-151"]
  ];
  return (type === "high" ? highValue : primary).map(([title, keywords]) => ({
    id: uid(`evaluation-${type}`),
    title,
    keywords: evaluationKeywordList(keywords),
    excludeKeywords: []
  }));
}

function defaultManagementEvaluationPolicyItem(kind = "count") {
  const normalizedKind = ["count", "rate", "percentile"].includes(kind) ? kind : "count";
  const isRate = normalizedKind === "rate";
  const isPercentile = normalizedKind === "percentile";
  return {
    id: uid("evaluation-policy"),
    title: "",
    description: "",
    kind: normalizedKind,
    keywords: isRate ? ["CP-"] : [],
    requiredKeywords: [],
    excludeKeywords: [],
    categoryFilter: "business",
    countBasis: "record",
    teamAggregation: "team",
    manualLabel: isPercentile ? "상위 백분위(%)" : isRate ? "" : "추가 수량",
    manualRequired: !isRate,
    goalBase: "new-rental",
    targetRate: isRate ? 55 : 0,
    scoreMode: isPercentile ? "at-most" : "at-least",
    scoreRules: isRate
      ? [[80, 2], [85, 4], [90, 6], [95, 8], [100, 10]]
      : isPercentile
        ? [[10, 10], [30, 7], [60, 5], [80, 3], [100, 1]]
        : [[1, 1], [2, 2], [3, 3], [4, 5]]
  };
}

function defaultManagementEvaluationPolicy(month = "") {
  // SK V1.00: 실제 SK 평가/정책 기준이 확정되기 전에는 기본 평가 규칙을 자동 적용하지 않는다.
  return {
    primaryProducts: [],
    highValueProducts: [],
    policyItems: []
  };
}

function normalizeManagementEvaluationScoreRules(value, kind = "count") {
  const defaultRules = defaultManagementEvaluationPolicyItem(kind).scoreRules;
  const source = Array.isArray(value) ? value : [];
  const rows = source.map((item) => {
    const minimum = evaluationNullableNumber(Array.isArray(item) ? item[0] : item?.minimum);
    const score = evaluationNullableNumber(Array.isArray(item) ? item[1] : item?.score);
    return minimum === null || score === null ? null : [Math.max(0, minimum), Math.max(0, score)];
  }).filter(Boolean);
  return (rows.length ? rows : defaultRules).sort((a, b) => a[0] - b[0]);
}

function normalizeManagementEvaluationProductRule(item, type = "primary") {
  const source = item && typeof item === "object" ? item : {};
  return {
    id: String(source.id || uid(`evaluation-${type}`)),
    title: String(source.title || "").trim() || "제품군",
    keywords: evaluationKeywordList(source.keywords),
    excludeKeywords: evaluationKeywordList(source.excludeKeywords)
  };
}

function managementEvaluationLegacyCategoryFilter(source = {}, kind = "count") {
  if (source.categoryFilter) return source.categoryFilter;
  if (kind === "rate") return "business";
  return ({
    "new": "new",
    "new-rental": "new-rental",
    "lump-sum": "cash",
    "general": "business"
  })[source.goalBase] || "business";
}

function normalizeManagementEvaluationPolicyItem(item = {}) {
  const source = item && typeof item === "object" ? item : {};
  const kind = ["count", "rate", "percentile"].includes(source.kind) ? source.kind : "count";
  const fallback = defaultManagementEvaluationPolicyItem(kind);
  const validCategoryFilters = ["all", "business", "rental", "new-rental", "new", "package", "rerental", "cash", "membership"];
  const categoryFilter = managementEvaluationLegacyCategoryFilter(source, kind);
  return {
    id: String(source.id || fallback.id),
    title: String(source.title || fallback.title).trim() || fallback.title,
    description: String(source.description || "").trim(),
    kind,
    keywords: evaluationKeywordList(source.keywords === undefined ? fallback.keywords : source.keywords),
    requiredKeywords: evaluationKeywordList(source.requiredKeywords),
    excludeKeywords: evaluationKeywordList(source.excludeKeywords),
    categoryFilter: validCategoryFilters.includes(categoryFilter) ? categoryFilter : fallback.categoryFilter,
    countBasis: source.countBasis === "product" ? "product" : "record",
    teamAggregation: ["team", "both"].includes(source.teamAggregation)
      ? source.teamAggregation
      : (["policy-mattress", "policy-massage"].includes(String(source.id || "")) ? "both" : fallback.teamAggregation),
    manualLabel: kind === "rate"
      ? ""
      : String(source.manualLabel === undefined ? fallback.manualLabel : source.manualLabel).trim(),
    manualRequired: kind === "percentile" ? true : kind === "count" ? source.manualRequired !== false : false,
    goalBase: ["new", "new-rental", "lump-sum", "general"].includes(source.goalBase)
      ? source.goalBase
      : fallback.goalBase,
    targetRate: kind === "rate" ? Math.max(0, evaluationNullableNumber(source.targetRate) ?? fallback.targetRate) : 0,
    scoreMode: source.scoreMode === "at-most" || kind === "percentile" ? "at-most" : "at-least",
    scoreRules: normalizeManagementEvaluationScoreRules(source.scoreRules, kind)
  };
}

function normalizeManagementEvaluationPolicy(value = {}, month = "") {
  const defaults = defaultManagementEvaluationPolicy(month);
  const source = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const primaryProducts = (Array.isArray(source.primaryProducts) ? source.primaryProducts : defaults.primaryProducts)
    .map((item) => normalizeManagementEvaluationProductRule(item, "primary"));
  const highValueProducts = (Array.isArray(source.highValueProducts) ? source.highValueProducts : defaults.highValueProducts)
    .map((item) => normalizeManagementEvaluationProductRule(item, "high"));
  const policySource = Array.isArray(source.policyItems) ? source.policyItems : defaults.policyItems;
  const policyItems = policySource.map(normalizeManagementEvaluationPolicyItem);
  return { primaryProducts, highValueProducts, policyItems };
}

function normalizeManagementEvaluationPolicies(value = {}) {
  const source = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  return Object.fromEntries(Object.entries(source)
    .filter(([month]) => /^\d{4}-\d{2}$/.test(month))
    .map(([month, policy]) => [month, normalizeManagementEvaluationPolicy(policy, month)]));
}

function defaultOperatingGoalEntry() {
  return {
    rate: 100,
    reason: "",
    updatedAt: "",
    history: []
  };
}

function normalizeOperatingGoalEntry(value = {}) {
  const source = value && typeof value === "object" ? value : {};
  const rawRate = Number(source.rate);
  const rate = Number.isFinite(rawRate) ? Math.max(50, Math.min(300, Math.round(rawRate))) : 100;
  const history = (Array.isArray(source.history) ? source.history : [])
    .map((item) => {
      const previousRate = Number(item?.previousRate);
      const nextRate = Number(item?.rate);
      return {
        id: String(item?.id || uid("operating-goal-history")),
        changedAt: String(item?.changedAt || ""),
        previousRate: Number.isFinite(previousRate) ? previousRate : 100,
        rate: Number.isFinite(nextRate) ? nextRate : rate,
        baseGoal: Math.max(0, toNumber(item?.baseGoal)),
        operatingGoal: Math.max(0, toNumber(item?.operatingGoal)),
        reason: String(item?.reason || "").trim()
      };
    })
    .filter((item) => item.changedAt || item.reason || item.rate !== item.previousRate)
    .slice(0, 30);
  return {
    rate,
    reason: String(source.reason || "").trim(),
    updatedAt: String(source.updatedAt || ""),
    history
  };
}

function normalizeOperatingGoals(value = {}) {
  const result = {};
  const source = value && typeof value === "object" ? value : {};
  Object.entries(source).forEach(([month, entry]) => {
    if (/^\d{4}-\d{2}$/.test(month)) result[month] = normalizeOperatingGoalEntry(entry);
  });
  return result;
}


const sampleState = {
  appMeta: {
    branchName: "SK지국",
    masterName: "사용자",
    masterRole: "담당자",
    teamOperationMode: "1"
  },
  menuVisibility: normalizeMenuVisibility(),
  dbBackupSettings: defaultDbBackupSettings(),
  teamNames: ["원팀"],
  managers: [],
  monthSettings: {},
  managerManualStats: {},
  managerManualOrder: {},
  managerMonthlyGoals: {},
  operatingGoals: {},
  managementEvaluationInputs: {},
  managementEvaluationPolicies: {},
  salesAnalyticsSettings: defaultSalesAnalyticsSettings(),
  todos: [],
  todosByDate: {},
  checklistItems: [],
  contactNotes: [],
  contactRequests: [],
  payrollRecords: [],
  payrollUnmatchedRecords: [],
  payrollManager: "",
  payrollMonth: "",
  payrollArchives: [],
  records: [],
  promotions: []
};

// IMPORTANT: do not read/normalize persisted data while the `state` binding is still
// being initialized. normalizeManager() can consult configuredTeamNames(), which reads
// the global state. With legacy data this used to throw a TDZ ReferenceError before
// init() even ran, leaving the startup overlay on screen. Persisted data is loaded in init().
let state = structuredClone(sampleState);
let currentView = "dashboard";
let managerPerformanceMode = "assigned";
let selectedRecordId = "";
const selectedRecordIds = new Set();
let recordSequenceSort = "asc";
let membershipRecordDateSort = "asc";
let promoListFilter = "all";
let calendarDragStart = "";
let calendarDragEnd = "";
let checklistMonth = monthIso();
let selectedChecklistId = "";
let checklistModalStatus = "전체";
let checklistAlarmQueue = [];
let activeChecklistAlarmId = "";
let checklistAlarmTimer = null;
let selectedContactNoteId = "";
let selectedContactRequestId = "";
let analyticsActiveTab = "overview";
let analyticsRangePreset = 6;
let analyticsSelectedManager = "";
let analyticsRecommendationCache = [];


let managerIndexCache = null;
let allManagerNamesCache = { revision: -1, names: [] };
let stateRevision = 0;
let renderFrameId = 0;

function invalidateManagerCaches() {
  managerIndexCache = null;
  allManagerNamesCache = { revision: -1, names: [] };
}

function touchStateRevision() {
  stateRevision += 1;
  allManagerNamesCache.revision = -1;
}

function managerIndex(managers = state.managers || []) {
  if (managers === state.managers && managerIndexCache?.source === managers) {
    return managerIndexCache;
  }

  const normalized = managers.map((manager) => {
    if (
      manager
      && typeof manager === "object"
      && manager.id
      && Array.isArray(manager.teamHistory)
      && Number.isFinite(Number(manager.displayOrder))
    ) {
      return manager;
    }
    return normalizeManager(manager);
  });

  const index = {
    source: managers,
    normalized,
    byId: new Map(),
    byName: new Map()
  };

  normalized.forEach((manager) => {
    if (manager.id) index.byId.set(manager.id, manager);
    if (manager.name) index.byName.set(manager.name, manager);
  });

  if (managers === state.managers) managerIndexCache = index;
  return index;
}

function debounce(callback, delay = 120) {
  let timer = 0;
  return (...args) => {
    window.clearTimeout(timer);
    timer = window.setTimeout(() => callback(...args), delay);
  };
}


const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

function stateDataCount(value) {
  if (!value || typeof value !== "object") return 0;
  const counts = [
    Array.isArray(value.records) ? value.records.length : 0,
    Array.isArray(value.managers) ? value.managers.length : 0,
    Array.isArray(value.promotions) ? value.promotions.length : 0,
    Array.isArray(value.checklistItems) ? value.checklistItems.length : 0,
    Array.isArray(value.contactNotes) ? value.contactNotes.length : 0,
    Array.isArray(value.contactRequests) ? value.contactRequests.length : 0,
    Array.isArray(value.payrollRecords) ? value.payrollRecords.length : 0,
    Array.isArray(value.payrollUnmatchedRecords) ? value.payrollUnmatchedRecords.length : 0,
    Array.isArray(value.payrollArchives) ? value.payrollArchives.length : 0,
    value.todosByDate && typeof value.todosByDate === "object" ? Object.keys(value.todosByDate).length : 0,
    value.monthSettings && typeof value.monthSettings === "object" ? Object.keys(value.monthSettings).length : 0,
    value.managementEvaluationPolicies && typeof value.managementEvaluationPolicies === "object" ? Object.keys(value.managementEvaluationPolicies).length : 0
  ];
  return counts.reduce((a, b) => a + b, 0);
}

function isCorePersistedState(value) {
  return Boolean(
    value
    && typeof value === "object"
    && !Array.isArray(value)
    && Array.isArray(value.records)
    && Array.isArray(value.managers)
  );
}

function safeLocalBackupSnapshot(sourceState = state, reason = "auto") {
  try {
    const snapshot = {
      backupType: "SK_Sales_Manager_AutoLocalBackup",
      schemaVersion: STATE_SCHEMA_VERSION,
      exportedAt: new Date().toISOString(),
      reason,
      dataCount: stateDataCount(sourceState),
      data: sourceState
    };
    const raw = JSON.stringify(snapshot);
    // Keep the latest snapshot in a dedicated key and retain a small rolling history.
    localStorage.setItem(LOCAL_BACKUP_KEY, raw);
    let index = [];
    try { index = JSON.parse(localStorage.getItem(LOCAL_BACKUP_INDEX_KEY) || "[]"); } catch { index = []; }
    index = Array.isArray(index) ? index : [];
    index.unshift({ exportedAt: snapshot.exportedAt, reason, dataCount: snapshot.dataCount });
    index = index.slice(0, LOCAL_BACKUP_LIMIT);
    localStorage.setItem(LOCAL_BACKUP_INDEX_KEY, JSON.stringify(index));
  } catch (error) {
    console.warn("[AUTO BACKUP] local backup failed", error);
  }
}

function readAutoLocalBackup() {
  try {
    const raw = localStorage.getItem(LOCAL_BACKUP_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const data = parsed?.data && typeof parsed.data === "object" ? parsed.data : null;
    return data ? { data, exportedAt: parsed.exportedAt || "", dataCount: Number(parsed.dataCount || stateDataCount(data)) } : null;
  } catch (error) {
    console.warn("[AUTO BACKUP] local backup read failed", error);
    return null;
  }
}

function loadState() {
  const raw = localStorage.getItem(STORAGE_KEY);
  if (!raw) {
    const backup = readAutoLocalBackup();
    if (isCorePersistedState(backup?.data)) return normalizeState(backup.data);
    return structuredClone(sampleState);
  }
  try {
    const loaded = JSON.parse(raw);
    if (isCorePersistedState(loaded)) return normalizeState(loaded);
    console.warn("[STARTUP] stored state is incomplete; recovery source will be used instead");
  } catch (error) {
    console.warn("[STARTUP] stored state parse failed", error);
  }
  const backup = readAutoLocalBackup();
  if (isCorePersistedState(backup?.data)) return normalizeState(backup.data);
  return structuredClone(sampleState);
}


async function loadPersistedState() {
  const local = readBrowserState();
  const backup = local ? null : readAutoLocalBackup()?.data;
  const durable = await readDurableBackupSnapshot();
  let candidate = local || (isCorePersistedState(backup) ? backup : null);
  if (isCorePersistedState(durable?.data) && (!candidate || comparePersistenceVersion(durable.data, candidate) > 0)) candidate = durable.data;

  let cloud = null;
  if (!IS_PC_APP && window.SKCloud?.enabled?.()) {
    try {
      cloud = await window.SKCloud.loadState();
      if (isCorePersistedState(cloud?.data)) {
        // 웹판은 Supabase를 중앙 원본으로 사용합니다. 로컬/IndexedDB는 안전 캐시입니다.
        candidate = cloud.data;
        cacheCommittedState(candidate);
        try { await writeDurableBackupSnapshot(candidate, "cloud-load"); } catch (_) {}
      }
    } catch (error) {
      console.error("[SK CLOUD] startup load failed", error);
      throw new Error("Supabase에서 SK 데이터를 불러오지 못했습니다. 인터넷 연결 후 다시 시도해 주세요.");
    }
  }

  persistenceBaseState = candidate ? structuredClone(candidate) : null;
  if (candidate) {
    const schema = Number(candidate?.appMeta?.stateSchemaVersion || candidate?.schemaVersion || 0);
    state = schema === STATE_SCHEMA_VERSION ? candidate : normalizeState(candidate);
  } else state = structuredClone(sampleState);

  state.appMeta = state.appMeta && typeof state.appMeta === "object" ? state.appMeta : {};
  state.menuVisibility = normalizeMenuVisibility(state.menuVisibility);
  state.teamNames = Array.isArray(state.teamNames) && state.teamNames.length ? state.teamNames : ["A팀"];
  invalidateManagerCaches();
  touchStateRevision();

  // 최초 로그인 계정에 클라우드 행이 아직 없으면 현재 정상 상태를 1회 초기값으로 등록합니다.
  if (!IS_PC_APP && window.SKCloud?.enabled?.() && cloud && !cloud.exists) {
    try {
      const seeded = await window.SKCloud.seedIfMissing(state);
      if (seeded?.conflict) {
        const latest = await window.SKCloud.loadState();
        if (isCorePersistedState(latest?.data)) {
          state = normalizeState(latest.data);
          persistenceBaseState = structuredClone(state);
          cacheCommittedState(state);
        }
      } else {
        persistenceBaseState = structuredClone(state);
      }
    } catch (error) {
      console.error("[SK CLOUD] initial seed failed", error);
      throw new Error("Supabase 초기 데이터 저장에 실패했습니다.");
    }
  }
}

function restoreFromDurableBackupInBackground() {
  const timeout = new Promise((resolve) => window.setTimeout(() => resolve(null), 1500));
  Promise.race([readDurableBackupSnapshot(), timeout])
    .then((durable) => {
      if (!durable?.data || !stateDataCount(durable.data)) return;
      // 복원 직전에 사용자가 이미 정상 데이터를 저장했다면 절대 덮어쓰지 않습니다.
      const currentRaw = localStorage.getItem(STORAGE_KEY);
      if (currentRaw) {
        try {
          const current = JSON.parse(currentRaw);
          if (isCorePersistedState(current)) return;
        } catch (_) {}
      }
      state = normalizeState(durable.data);
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
      safeLocalBackupSnapshot(state, "durable-restore");
      invalidateManagerCaches();
      touchStateRevision();
      renderNow();
      showToast("브라우저 저장 데이터가 없어 최근 안전백업을 자동 복원했습니다.");
    })
    .catch((error) => console.warn("[DURABLE RESTORE] background restore failed", error));
}

// V11.23: compare the state that this window actually loaded/committed.
let persistenceBaseState = null;
let persistenceQueue = Promise.resolve({ ok: true });
let persistenceBlocked = false;
let persistenceErrorMessage = "";
let desktopBaseStamp = "none";

function persistenceStamp(value) {
  if (!isCorePersistedState(value)) return "none";
  const meta = value.appMeta || {};
  if (meta.persistCommitId || meta.persistRevision || meta.lastStateUpdatedAt) {
    return JSON.stringify([Number(meta.persistRevision || 0), String(meta.lastStateUpdatedAt || ""), String(meta.persistCommitId || "")]);
  }
  // Legacy data has no revision. A deterministic fingerprint avoids putting
  // customer data in request headers while still detecting a changed baseline.
  const raw = JSON.stringify(value);
  let hash = 2166136261;
  for (let i = 0; i < raw.length; i += 1) hash = Math.imul(hash ^ raw.charCodeAt(i), 16777619);
  return `legacy-${raw.length}-${hash >>> 0}`;
}

function comparePersistenceVersion(a, b) {
  const revision = Number(a?.appMeta?.persistRevision || 0) - Number(b?.appMeta?.persistRevision || 0);
  if (revision) return revision;
  return (Date.parse(a?.appMeta?.lastStateUpdatedAt || "") || 0) - (Date.parse(b?.appMeta?.lastStateUpdatedAt || "") || 0);
}

function persistenceConflicts(latest, baseline) {
  return isCorePersistedState(latest) && persistenceStamp(latest) !== persistenceStamp(baseline)
    && (!baseline || comparePersistenceVersion(latest, baseline) >= 0);
}

function readBrowserState() {
  try {
    const parsed = JSON.parse(localStorage.getItem(STORAGE_KEY) || "null");
    return isCorePersistedState(parsed) ? parsed : null;
  } catch (_) { return null; }
}

function cacheCommittedState(snapshot) {
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(snapshot)); return true; }
  catch (error) { console.warn("[LOCAL CACHE] write failed; independent storage is used", error); return false; }
}

function persistenceFailure(conflict = false, message = "") {
  if (conflict) persistenceBlocked = true;
  persistenceErrorMessage = message || (conflict
    ? "다른 창의 최신 저장과 충돌하여 저장하지 않았습니다. 필요한 변경은 전체백업으로 내보낸 뒤 새로고침해 주세요."
    : "변경 내용을 저장하지 못했습니다. 창을 닫지 말고 전체백업을 내보낸 뒤 다시 저장해 주세요.");
  showToast(persistenceErrorMessage);
  return { ok: false, conflict, error: persistenceErrorMessage };
}

async function commitDurableState(snapshot, baseline) {
  const db = await openDurableBackupDb();
  try {
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(DURABLE_STORE_NAME, "readwrite");
      const store = tx.objectStore(DURABLE_STORE_NAME);
      let conflict = false;
      const read = store.get(DURABLE_SNAPSHOT_KEY);
      read.onsuccess = () => {
        const latest = read.result?.data;
        conflict = persistenceConflicts(latest, baseline);
        if (!conflict) store.put({ backupType: "SK_Sales_Manager_DurableBackup", schemaVersion: STATE_SCHEMA_VERSION,
          exportedAt: new Date().toISOString(), reason: "persist-current", dataCount: stateDataCount(snapshot), data: snapshot }, DURABLE_SNAPSHOT_KEY);
      };
      tx.oncomplete = () => resolve({ ok: !conflict, conflict });
      tx.onerror = () => reject(tx.error || new Error("IndexedDB write failed"));
      tx.onabort = () => reject(tx.error || new Error("IndexedDB write aborted"));
    });
  } finally { db.close(); }
}

async function commitWebState(snapshot) {
  const commit = async () => {
    if (window.SKCloud?.enabled?.()) {
      let cloudResult;
      try {
        cloudResult = await window.SKCloud.saveState(snapshot);
      } catch (error) {
        console.error("[SK CLOUD] save failed", error);
        return persistenceFailure(false, "Supabase 저장에 실패했습니다. 인터넷 연결을 확인한 뒤 다시 저장해 주세요.");
      }
      if (cloudResult?.conflict) {
        return persistenceFailure(true, "다른 PC 또는 창에서 더 최신 데이터가 저장되었습니다. 새로고침 후 다시 확인해 주세요.");
      }
      if (!cloudResult?.ok) return persistenceFailure(false, "Supabase 저장에 실패했습니다.");

      // 중앙 저장 성공 후 현재 PC에도 안전 캐시를 남깁니다.
      const localOk = cacheCommittedState(snapshot);
      try { await writeDurableBackupSnapshot(snapshot, "cloud-commit"); }
      catch (error) { console.warn("[SK CLOUD] local durable mirror failed", error); }
      persistenceBaseState = snapshot;
      persistenceErrorMessage = "";
      if (localOk) safeLocalBackupSnapshot(snapshot, "persist-current");
      return { ok: true, cloud: true, local: localOk, durable: true };
    }

    const local = readBrowserState();
    if (persistenceConflicts(local, persistenceBaseState)) return persistenceFailure(true);
    let durable = null;
    try { durable = await commitDurableState(snapshot, persistenceBaseState); }
    catch (error) { console.warn("[STATE] durable save unavailable", error); }
    if (durable?.conflict) return persistenceFailure(true);
    const localOk = cacheCommittedState(snapshot);
    if (!durable?.ok && !localOk) return persistenceFailure();
    persistenceBaseState = snapshot;
    persistenceErrorMessage = "";
    if (localOk) safeLocalBackupSnapshot(snapshot, "persist-current");
    return { ok: true, local: localOk, durable: Boolean(durable?.ok) };
  };
  if (navigator.locks?.request) return navigator.locks.request(`${STORAGE_KEY}-persist`, commit);
  return commit();
}

function persistState(options = {}) {
  if (persistenceBlocked) return Promise.resolve(persistenceFailure(true));
  if (options.ensureManagers !== false) ensureManagerDataIntegrity(state);
  const existing = persistenceBaseState;
  if (stateDataCount(state) === 0 && stateDataCount(existing) > 0 && options.allowEmptyServer !== true) {
    return Promise.resolve(persistenceFailure(false, "빈 데이터 저장을 차단했습니다. 기존 저장 데이터는 유지됩니다."));
  }
  state.appMeta = state.appMeta || {};
  state.appMeta.lastStateUpdatedAt = new Date().toISOString();
  state.appMeta.stateSchemaVersion = STATE_SCHEMA_VERSION;
  const currentRevision = Number(state.appMeta.persistRevision || 0);
  const baseRevision = Number(existing?.appMeta?.persistRevision || 0);
  state.appMeta.persistRevision = Math.max(Number.isFinite(currentRevision) ? currentRevision : 0, Number.isFinite(baseRevision) ? baseRevision : 0, 0) + 1;
  state.appMeta.persistCommitId = uid("save");
  const snapshot = structuredClone(state);
  touchStateRevision();
  persistenceQueue = persistenceQueue.catch(() => ({ ok: false })).then(async () => {
    if (persistenceBlocked) return persistenceFailure(true);
    try {
      const result = IS_PC_APP ? await commitDesktopState(snapshot) : await commitWebState(snapshot);
      if (result.ok && options.skipDbAutoBackup !== true) queueDbAutoBackup();
      return result;
    } catch (error) {
      console.error("[STATE SAVE] failed", error);
      return persistenceFailure();
    }
  });
  return persistenceQueue;
}

function ensureAllRecordManualOrder(records) {
  records.forEach((record, index) => {
    const value = Number(record.manualOrder);
    if (!Number.isFinite(value) || value <= 0) record.manualOrder = index + 1;
    else record.manualOrder = value;
  });
  return records;
}

function normalizeState(loaded) {
  const loadedManagers = Array.isArray(loaded.managers) ? loaded.managers : sampleState.managers;
  const configuredTeams = normalizeTeamNames(loaded.teamNames, loadedManagers);
  const next = {
    ...structuredClone(sampleState),
    ...loaded,
    teamNames: configuredTeams,
    appMeta: { ...sampleState.appMeta, ...(loaded.appMeta || {}) },
    menuVisibility: normalizeMenuVisibility(loaded.menuVisibility),
    dbBackupSettings: normalizeDbBackupSettings(loaded.dbBackupSettings),
    managers: loadedManagers.map((manager) => normalizeManager(manager, configuredTeams)),
    records: Array.isArray(loaded.records) ? loaded.records : sampleState.records,
    promotions: Array.isArray(loaded.promotions) ? loaded.promotions.map(normalizePromotion) : sampleState.promotions.map(normalizePromotion),
    monthSettings: { ...sampleState.monthSettings, ...(loaded.monthSettings || {}) },
    managerManualStats: { ...(sampleState.managerManualStats || {}), ...(loaded.managerManualStats || {}) },
    managerManualOrder: { ...(loaded.managerManualOrder || {}) },
    managerMonthlyGoals: loaded.managerMonthlyGoals && typeof loaded.managerMonthlyGoals === "object" ? loaded.managerMonthlyGoals : {},
    operatingGoals: normalizeOperatingGoals(loaded.operatingGoals),
    managementEvaluationInputs: normalizeManagementEvaluationInputs(loaded.managementEvaluationInputs),
    managementEvaluationPolicies: normalizeManagementEvaluationPolicies(loaded.managementEvaluationPolicies),
    salesAnalyticsSettings: normalizeSalesAnalyticsSettings(loaded.salesAnalyticsSettings),
    dashboardCustomCards: Array.isArray(loaded.dashboardCustomCards) ? loaded.dashboardCustomCards : defaultDashboardCustomCards(),
    todos: Array.isArray(loaded.todos) ? loaded.todos : [],
    todosByDate: loaded.todosByDate && typeof loaded.todosByDate === "object" ? loaded.todosByDate : {},
    checklistItems: Array.isArray(loaded.checklistItems) ? loaded.checklistItems.map(normalizeChecklistItem) : [],
    contactNotes: Array.isArray(loaded.contactNotes) ? loaded.contactNotes.map(normalizeContactNote) : [],
    contactRequests: Array.isArray(loaded.contactRequests) ? loaded.contactRequests.map(normalizeContactNote) : [],
    payrollRecords: Array.isArray(loaded.payrollRecords) ? loaded.payrollRecords.map(normalizePayrollRecord) : [],
    payrollUnmatchedRecords: Array.isArray(loaded.payrollUnmatchedRecords) ? loaded.payrollUnmatchedRecords.map(normalizePayrollRecord) : [],
    payrollManager: String(loaded.payrollManager || "").trim(),
    payrollMonth: String(loaded.payrollMonth || "").trim(),
    payrollArchives: Array.isArray(loaded.payrollArchives) ? loaded.payrollArchives.map(normalizePayrollArchive) : []
  };
  next.records = next.records.map((record) => {
    const normalizedRecord = { cashAmount: 0, activityType: "", ...record };
    normalizedRecord.activityType = normalizeActivityType(normalizedRecord.activityType);
    normalizedRecord.phone = typeof formatPhoneNumber === "function" ? formatPhoneNumber(normalizedRecord.phone) : normalizedRecord.phone;
    return normalizedRecord;
  });
  ensureManagerDataIntegrity(next);
  // 조직 이력은 기존 데이터에서 가능한 가장 이른 데이터 월을 기준으로 1회 마이그레이션하고, 이후 변경은 월 단위로 누적합니다.
  const knownMonths = [
    ...Object.keys(next.monthSettings || {}),
    ...(next.records || []).map((record) => recordGoalMonth(record, "")),
    ...(next.managers || []).map((manager) => manager.joinedMonth).filter(Boolean)
  ].filter((month) => /^\d{4}-\d{2}$/.test(month)).sort();
  const legacyStartMonth = knownMonths[0] || monthIso();
  if (!next.appMeta.teamOperationHistory || !Array.isArray(next.appMeta.teamOperationHistory)) {
    const legacyMode = String(next.appMeta.teamOperationMode || "").trim();
    next.appMeta.teamOperationHistory = [{ value: legacyMode === "2" ? "2" : (legacyMode === "1" ? "1" : (next.teamNames.length >= 2 ? "2" : "1")), startMonth: legacyStartMonth, endMonth: "" }];
  }
  if (!next.appMeta.masterTeamHistory || !Array.isArray(next.appMeta.masterTeamHistory)) {
    const legacyTeam = normalizeTeamName(next.appMeta.userTeam);
    const master = String(next.appMeta.masterName || "").trim() ? next.managers.find((item) => item.name === String(next.appMeta.masterName || "").trim()) : null;
    const inferred = legacyTeam || (master ? managerTeamForMonth(master, legacyStartMonth) : "");
    next.appMeta.masterTeamHistory = inferred ? [{ team: inferred, startMonth: legacyStartMonth, endMonth: "" }] : [];
  }
  ensureAllRecordManualOrder(next.records);
  return next;
}

async function saveState(message, options = {}) {
  const result = await persistState(options);
  render();
  if (result.ok && message) showToast(message);
  return result;
}

let currentManagerShareBlob = null;
let currentManagerShareFileName = "";
let currentManagerShareUrl = "";

function uid(prefix) {
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function todayIso() {
  return formatLocalDate(new Date());
}

function monthIso(date = new Date()) {
  return formatLocalDate(date).slice(0, 7);
}

function shiftMonth(month, offset = 0) {
  const [year, monthNum] = String(month || monthIso()).split("-").map(Number);
  const date = new Date(year, (monthNum - 1) + offset, 1);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function formatMonthLabel(month) {
  const [year, monthNum] = String(month || monthIso()).split("-").map(Number);
  return `${year}년 ${monthNum}월`;
}

function formatLocalDate(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatNumber(value) {
  return new Intl.NumberFormat("ko-KR", { maximumFractionDigits: 1 }).format(toNumber(value));
}

function blankZeroNumber(value, options = {}) {
  const num = toNumber(value);
  if (!num) return "";
  const text = formatNumber(num);
  return options.minus ? `-${text}` : text;
}

function formatWon(value) {
  return `${new Intl.NumberFormat("ko-KR").format(Math.round(toNumber(value)))}원`;
}

function formatKoreanLongDate(date = new Date()) {
  return new Intl.DateTimeFormat("ko-KR", {
    year: "numeric",
    month: "long",
    day: "numeric",
    weekday: "long"
  }).format(date);
}

function dashboardDefaultEnd(period) {
  const today = todayIso();
  if (inDateRange(today, period.start, period.end)) return today;
  return period.end;
}

function setDashboardRange(start, end) {
  $("#startDateFilter").value = start;
  $("#endDateFilter").value = end;
}


function currentDashboardMonth() {
  return $("#monthFilter")?.value || monthIso();
}

function goalSettingsMonth() {
  const explicitMonth = $("#goalMonthInput")?.value || "";
  if (explicitMonth) return explicitMonth;
  const endDate = $("#periodEndInput")?.value || "";
  if (endDate && /^\d{4}-\d{2}-\d{2}$/.test(endDate)) return endDate.slice(0, 7);
  return currentDashboardMonth();
}

function syncGoalMonthFromPeriodEnd() {
  const goalMonthInput = $("#goalMonthInput");
  const endDate = $("#periodEndInput")?.value || "";
  if (goalMonthInput && endDate && /^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
    goalMonthInput.value = endDate.slice(0, 7);
  }
}

function renderGoalSettingsForMonth(month = goalSettingsMonth()) {
  const goalMonthInput = $("#goalMonthInput");
  if (goalMonthInput) goalMonthInput.value = month;
  const setting = monthSetting(month);
  $("#accountCountInput").value = setting.accountCount;
  const packageRateInput = $("#packageRateInput");
  if (packageRateInput) packageRateInput.value = setting.packageRate;
  $("#newWeightInput").value = setting.newWeight;
  $("#rentalWeightInput").value = setting.rentalWeight;
  $("#renewalWeightInput").value = setting.renewalWeight;
  $("#newIndexInput").value = setting.newIndex;
  $("#rentalIndexInput").value = setting.rentalIndex;
  $("#renewalIndexInput").value = setting.renewalIndex;
  $("#periodStartInput").value = setting.periodStart;
  $("#periodEndInput").value = setting.periodEnd;
}

function monthSetting(month = $("#monthFilter")?.value || monthIso()) {
  if (!state.monthSettings) state.monthSettings = {};
  if (!state.monthSettings[month]) {
    const prevMonth = shiftMonth(month, -1);
    const prevSetting = state.monthSettings[prevMonth];
    const inheritedStart = prevSetting?.periodEnd ? addDaysIso(prevSetting.periodEnd, 1) : `${month}-01`;
    state.monthSettings[month] = {
      accountCount: 0,
      packageRate: 45,
      newWeight: 60,
      rentalWeight: 30,
      renewalWeight: 10,
      newIndex: 9.5,
      rentalIndex: 5,
      renewalIndex: 18,
      periodStart: inheritedStart,
      periodEnd: lastDayOfMonth(month)
    };
  }
  const setting = state.monthSettings[month];
  if (setting.packageRate === undefined || setting.packageRate === null || setting.packageRate === "") setting.packageRate = 45;
  if (!setting.newWeight) setting.newWeight = 60;
  if (!setting.rentalWeight) setting.rentalWeight = 30;
  if (!setting.renewalWeight) setting.renewalWeight = 10;
  if (!setting.newIndex) setting.newIndex = setting.newDivisor || 9.5;
  if (!setting.rentalIndex) setting.rentalIndex = setting.rentalTarget && setting.accountCount ? roundSetting(setting.accountCount * 0.3 / setting.rentalTarget) : 5;
  if (!setting.renewalIndex) setting.renewalIndex = setting.renewalDivisor || 18;
  if (!setting.periodStart) setting.periodStart = `${month}-01`;
  if (!setting.periodEnd) setting.periodEnd = lastDayOfMonth(month);
  return state.monthSettings[month];
}

function monthPeriod(month = $("#monthFilter")?.value || monthIso()) {
  const setting = monthSetting(month);
  return { start: setting.periodStart, end: setting.periodEnd };
}

// 모든 월 실적의 기준은 달력의 1일~말일이 아니라 사용자설정의 목표시작일~목표종료일입니다.
// 예: 2026-08 목표기간이 2026-07-29~2026-08-27이면 7/29~7/31 접수도 8월 실적으로 분류합니다.
function goalMonthForDate(dateText, fallbackMonth = "") {
  const date = String(dateText || "").slice(0, 10);
  const fallback = normalizeManagerMonth(fallbackMonth);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return fallback || monthIso();

  const matches = Object.entries(state.monthSettings || {})
    .filter(([month, setting]) => /^\d{4}-\d{2}$/.test(month) && setting && typeof setting === "object")
    .filter(([, setting]) => {
      const start = String(setting.periodStart || "");
      const end = String(setting.periodEnd || "");
      return /^\d{4}-\d{2}-\d{2}$/.test(start) && /^\d{4}-\d{2}-\d{2}$/.test(end) && inDateRange(date, start, end);
    })
    .map(([month]) => month)
    .sort();

  if (matches.length) return matches[matches.length - 1];
  return fallback || date.slice(0, 7);
}

function recordGoalMonth(record = {}, fallbackMonth = "") {
  return goalMonthForDate(record.receivedDate || record.installDate || "", fallbackMonth);
}

// 조직 변경은 목표월과 무관한 달력월의 1일에 적용됩니다.
// 10월 퇴사자는 9/30까지 재직이며, 9/28~10/28 목표기간에서
// 9월 말 접수는 10월 실적으로 보이되 9월 조직 이력을 사용합니다.
function organizationMonthForDate(dateText, fallbackMonth = "") {
  const date = String(dateText || "").trim().slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(date)
    ? date.slice(0, 7)
    : (normalizeManagerMonth(fallbackMonth) || monthIso());
}

function managerTeamForDate(managerOrName, dateText, fallbackMonth = "") {
  const goalMonth = organizationMonthForDate(dateText, fallbackMonth);
  return managerTeamForMonth(managerOrName, goalMonth);
}

function managerStatusForDate(managerOrName, dateText, fallbackMonth = "") {
  const goalMonth = organizationMonthForDate(dateText, fallbackMonth);
  return managerStatusForMonth(managerOrName, goalMonth);
}

function calculatedGoals(month = $("#monthFilter")?.value || monthIso()) {
  const setting = monthSetting(month);
  const account = toNumber(setting.accountCount);
  const newGoal = setting.newIndex > 0 ? Math.round((account * (toNumber(setting.newWeight) / 100)) / toNumber(setting.newIndex)) : 0;
  const rentalGoal = setting.rentalIndex > 0 ? Math.round((account * (toNumber(setting.rentalWeight) / 100)) / toNumber(setting.rentalIndex)) : 0;
  const generalGoal = newGoal + rentalGoal;
  const packageGoal = Math.round(generalGoal * (toNumber(setting.packageRate) / 100));
  const renewalGoal = setting.renewalIndex > 0 ? Math.round((account * (toNumber(setting.renewalWeight) / 100)) / toNumber(setting.renewalIndex)) : 0;
  const overallGoal = generalGoal + renewalGoal;
  return { newGoal, rentalGoal, generalGoal, packageGoal, renewalGoal, overallGoal };
}


function operatingGoalEntry(month = currentDashboardMonth()) {
  if (!state.operatingGoals || typeof state.operatingGoals !== "object" || Array.isArray(state.operatingGoals)) {
    state.operatingGoals = {};
  }

  const existing = state.operatingGoals[month];
  if (!existing || typeof existing !== "object" || Array.isArray(existing)) {
    state.operatingGoals[month] = normalizeOperatingGoalEntry(existing || defaultOperatingGoalEntry());
    return state.operatingGoals[month];
  }

  // 같은 월의 객체 참조를 유지해야 저장 중 계산 함수가 호출돼도
  // 수정 대상이 끊기지 않고 실제 state에 그대로 남습니다.
  const normalized = normalizeOperatingGoalEntry(existing);
  existing.rate = normalized.rate;
  existing.reason = normalized.reason;
  existing.updatedAt = normalized.updatedAt;
  existing.history = normalized.history;
  return existing;
}

function operatingGoalMetrics(month, actual = 0, rateOverride = null) {
  const baseGoal = Math.max(0, toNumber(calculatedGoals(month).overallGoal));
  const entry = operatingGoalEntry(month);
  const requestedRate = rateOverride === null ? entry.rate : Number(rateOverride);
  const rate = Number.isFinite(requestedRate)
    ? Math.max(50, Math.min(300, Math.round(requestedRate)))
    : entry.rate;
  const operatingGoal = baseGoal > 0 ? Math.ceil(baseGoal * rate / 100) : 0;
  const actualValue = Math.max(0, toNumber(actual));
  const operatingAchievement = operatingGoal > 0 ? actualValue / operatingGoal * 100 : 0;
  const baseAchievement = baseGoal > 0 ? actualValue / baseGoal * 100 : 0;
  return {
    month,
    rate,
    baseGoal,
    operatingGoal,
    actual: actualValue,
    targetGap: Math.max(baseGoal - operatingGoal, 0),
    operatingShortage: Math.max(operatingGoal - actualValue, 0),
    baseShortage: Math.max(baseGoal - actualValue, 0),
    operatingAchievement,
    baseAchievement
  };
}

function operatingGoalCurrentActual() {
  const records = filteredRecords();
  return applyManualStatsToTotals(actuals(records)).overallActual;
}

function formatOperatingGoalDate(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("ko-KR", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  }).format(date);
}

function operatingGoalReasonText() {
  const selected = String($("#operatingGoalReasonSelect")?.value || "").trim();
  const memo = String($("#operatingGoalReasonInput")?.value || "").trim();
  if (selected && memo) return `${selected} · ${memo}`;
  return selected || memo;
}

function updateOperatingGoalPreview() {
  const input = $("#operatingGoalRateInput");
  const preview = $("#operatingGoalPreview");
  if (!input || !preview) return;
  const month = currentDashboardMonth();
  const rate = Number(input.value);
  if (!Number.isFinite(rate) || rate < 50 || rate > 300) {
    preview.textContent = "운영목표율은 50%~300% 사이로 입력해 주세요. 1% 단위로 설정할 수 있습니다.";
    preview.classList.add("is-warning");
    return;
  }
  const metrics = operatingGoalMetrics(month, operatingGoalCurrentActual(), rate);
  preview.classList.remove("is-warning");
  preview.innerHTML = `저장 시 <strong>${metrics.rate}%</strong> · 운영목표 <strong>${formatNumber(metrics.operatingGoal)}건</strong> · 현재 달성률 <strong>${formatNumber(metrics.operatingAchievement)}%</strong>`;
  $$("[data-operating-goal-rate]").forEach((button) => {
    button.classList.toggle("active", Number(button.dataset.operatingGoalRate) === metrics.rate);
  });
}

function renderOperatingGoalPanel(totals = null, month = currentDashboardMonth()) {
  const panel = $("#operatingGoalPanel");
  if (!panel) return;
  const actual = totals ? totals.overallActual : operatingGoalCurrentActual();
  const entry = operatingGoalEntry(month);
  const metrics = operatingGoalMetrics(month, actual);

  $("#operatingGoalMonthLabel").textContent = `${formatMonthLabel(month)} 지국 전체`;
  $("#operatingGoalActual").textContent = `${formatNumber(metrics.actual)}건`;
  $("#operatingGoalTarget").textContent = formatNumber(metrics.operatingGoal);
  $("#operatingGoalRateLabel").textContent = `${metrics.rate}% 운영`;
  $("#operatingGoalBaseTarget").textContent = formatNumber(metrics.baseGoal);
  $("#operatingGoalTargetGap").textContent = formatNumber(metrics.targetGap);
  $("#operatingGoalAchievement").textContent = `${formatNumber(metrics.operatingAchievement)}%`;
  $("#operatingGoalBaseAchievement").textContent = `${formatNumber(metrics.baseAchievement)}%`;
  $("#operatingGoalShortage").textContent = `${formatNumber(metrics.operatingShortage)}건`;
  $("#operatingGoalBaseShortage").textContent = `${formatNumber(metrics.baseShortage)}건`;
  $("#operatingGoalBar").style.width = `${Math.max(0, Math.min(metrics.operatingAchievement, 100))}%`;
  $("#operatingGoalBaseBar").style.width = `${Math.max(0, Math.min(metrics.baseAchievement, 100))}%`;

  const rateInput = $("#operatingGoalRateInput");
  if (rateInput && document.activeElement !== rateInput) rateInput.value = metrics.rate;
  const saveStatus = $("#operatingGoalSaveStatus");
  if (saveStatus) {
    saveStatus.textContent = entry.updatedAt
      ? `현재 월 저장값: ${metrics.rate}% · 마지막 저장 ${formatOperatingGoalDate(entry.updatedAt)}`
      : `현재 월 저장값: ${metrics.rate}% · 아직 조정 이력 없음`;
  }
  $$("[data-operating-goal-rate]").forEach((button) => {
    button.classList.toggle("active", Number(button.dataset.operatingGoalRate) === metrics.rate);
  });

  const historyBox = $("#operatingGoalHistoryList");
  if (historyBox) {
    const history = entry.history.slice(0, 5);
    historyBox.innerHTML = history.length
      ? history.map((item) => `
        <div class="operating-goal-history-item">
          <div>
            <strong>${formatNumber(item.previousRate)}% → ${formatNumber(item.rate)}%</strong>
            <span>${escapeHtml(item.reason || "사유 미입력")}</span>
          </div>
          <div>
            <b>${formatNumber(item.operatingGoal)}건</b>
            <time>${escapeHtml(formatOperatingGoalDate(item.changedAt))}</time>
          </div>
        </div>`).join("")
      : `<div class="operating-goal-history-empty">아직 운영목표 조정 이력이 없습니다.</div>`;
  }

  updateOperatingGoalPreview();
}

function saveOperatingGoal(rateOverride = null, forcedReason = "") {
  const month = currentDashboardMonth();
  const rawRate = rateOverride === null ? Number($("#operatingGoalRateInput")?.value) : Number(rateOverride);
  if (!Number.isFinite(rawRate) || rawRate < 50 || rawRate > 300) {
    showToast("운영목표율은 50%~300% 사이로 입력해 주세요. 1% 단위로 설정할 수 있습니다.");
    return;
  }

  const rate = Math.round(rawRate);
  const entry = operatingGoalEntry(month);
  const previousRate = entry.rate;
  const reason = forcedReason || operatingGoalReasonText();
  const metrics = operatingGoalMetrics(month, operatingGoalCurrentActual(), rate);

  if (rate === previousRate && !reason) {
    showToast(`현재 운영목표율이 이미 ${rate}%입니다.`);
    renderOperatingGoalPanel(null, month);
    return;
  }

  const changedAt = new Date().toISOString();
  entry.rate = rate;
  entry.reason = reason;
  entry.updatedAt = changedAt;
  entry.history.unshift({
    id: uid("operating-goal-history"),
    changedAt,
    previousRate,
    rate,
    baseGoal: metrics.baseGoal,
    operatingGoal: metrics.operatingGoal,
    reason: reason || (rate === 100 ? "100% 기준목표로 복원" : "운영목표 조정")
  });
  entry.history = entry.history.slice(0, 30);

  // 저장 직전에도 실제 state 안의 값이 바뀌었는지 검증합니다.
  state.operatingGoals[month] = entry;
  persistState();

  if ($("#operatingGoalReasonSelect")) $("#operatingGoalReasonSelect").value = "";
  if ($("#operatingGoalReasonInput")) $("#operatingGoalReasonInput").value = "";

  renderDashboard();

  const savedRate = operatingGoalEntry(month).rate;
  if (savedRate !== rate) {
    showToast("운영목표 저장값 확인 중 오류가 발생했습니다.");
    return;
  }

  showToast(`${formatMonthLabel(month)} 운영목표를 ${rate}% · ${formatNumber(metrics.operatingGoal)}건으로 저장했습니다. 매니저별 목표는 변경되지 않았습니다.`);
}

function inDateRange(date, start, end) {
  if (!date) return false;
  if (start && date < start) return false;
  if (end && date > end) return false;
  return true;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setOptions(select, values, selected = "") {
  if (!select) return;
  const markup = values.map((value) => {
    const optionValue = typeof value === "string" ? value : value.value;
    const label = typeof value === "string" ? value : value.label;
    return `<option value="${escapeHtml(optionValue)}"${optionValue === selected ? " selected" : ""}>${escapeHtml(label)}</option>`;
  }).join("");

  const signature = `${selected}::${markup}`;
  if (select.dataset.optionsSignature === signature) return;

  select.innerHTML = markup;
  select.dataset.optionsSignature = signature;
}

function uniqueSorted(values) {
  return [...new Set(values.map((value) => String(value || "").trim()).filter(Boolean))]
    .sort((a, b) => a.localeCompare(b, "ko"));
}

function optionListWithAll(values, allLabel = "전체") {
  return [{ value: "", label: allLabel }, ...uniqueSorted(values).map((value) => ({ value, label: value }))];
}

function compactValue(value, fallback = "-") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function formatPhoneNumber(value) {
  let text = String(value ?? "").trim();
  if (!text) return "";
  text = text.replace(/\.0$/, "");
  const digits = text.replace(/[^0-9]/g, "");
  if (digits.length === 10 && digits.startsWith("10")) return `0${digits}`;
  if (digits.length === 10 && digits.startsWith("1")) return `0${digits}`;
  if (digits.length === 11 && digits.startsWith("10")) return `0${digits}`;
  if (digits.length >= 10 && digits.length <= 11) return digits.startsWith("0") ? digits : `0${digits}`;
  return text;
}

function getFilters() {
  const month = $("#monthFilter").value || monthIso();
  const period = monthPeriod(month);
  const searchInput = $("#globalSearch");
  const managerInput = $("#managerFilter");
  return {
    month,
    start: $("#startDateFilter").value || period.start,
    end: $("#endDateFilter").value || period.end,
    manager: managerInput ? managerInput.value : "",
    search: searchInput ? searchInput.value.trim().toLowerCase() : ""
  };
}


function addDaysIso(dateText, days = 1) {
  if (!dateText) return "";
  const date = new Date(`${dateText}T00:00:00`);
  if (Number.isNaN(date.getTime())) return "";
  date.setDate(date.getDate() + days);
  return formatLocalDate(date);
}

function lastDayOfMonth(month) {
  const [year, monthNum] = month.split("-").map(Number);
  return formatLocalDate(new Date(year, monthNum, 0));
}



function membershipDatePeriod() {
  const range = currentRecordPeriodRange();
  return { start: range.start, end: range.end };
}

function membershipFilters() {
  const period = membershipDatePeriod();
  return {
    status: $("#membershipStatusFilter")?.value || "",
    manager: $("#membershipManagerFilter")?.value || "",
    contact: $("#membershipContactFilter")?.value || "",
    start: period.start,
    end: period.end
  };
}


function isMembershipRecord(record) {
  return normalizeCategory(record?.category) === "맴버쉽";
}

function membershipRecordContact(record = {}) {
  return compactValue(record.seller || record.contact || record.contractor, "");
}


function filteredMembershipRecordsByMonth() {
  const filters = membershipFilters();
  return state.records.filter((record) => {
    if (!isMembershipRecord(record)) return false;
    const dateValue = record.receivedDate || "";
    if (filters.start && dateValue < filters.start) return false;
    if (filters.end && dateValue > filters.end) return false;
    if (!recordBelongsToCurrentUserTeam(record, recordGoalMonth(record, currentDashboardMonth()))) return false;
    if (filters.status && compactValue(record.status, "접수") !== filters.status) return false;
    if (filters.manager && compactValue(record.manager, "") !== filters.manager) return false;
    if (filters.contact && membershipRecordContact(record) !== filters.contact) return false;
    return true;
  });
}


function renderMembershipFilterOptions(records = []) {
  const statusFilter = $("#membershipStatusFilter");
  const managerFilter = $("#membershipManagerFilter");
  const contactFilter = $("#membershipContactFilter");
  if (!statusFilter && !managerFilter && !contactFilter) return;
  const previous = {
    status: statusFilter?.value || "",
    manager: managerFilter?.value || "",
    contact: contactFilter?.value || ""
  };
  const period = membershipDatePeriod();
  const selectedGoalMonth = normalizeManagerMonth($("#recordMonthFilter")?.value) || currentDashboardMonth();
  const baseRecords = state.records.filter((record) => {
    if (!isMembershipRecord(record)) return false;
    if (!inDateRange(record.receivedDate || "", period.start, period.end)) return false;
    return recordBelongsToCurrentUserTeam(record, selectedGoalMonth);
  });
  if (statusFilter) setOptions(statusFilter, optionListWithAll(baseRecords.map((record) => record.status), "전체 상태"), previous.status);
  if (managerFilter) setOptions(managerFilter, optionListWithAll(baseRecords.map((record) => record.manager), "전체 매니저"), previous.manager);
  if (contactFilter) setOptions(contactFilter, optionListWithAll(baseRecords.map((record) => membershipRecordContact(record)), "전체 컨텍자"), previous.contact);
}



function filteredMembershipRecords() {
  return filteredMembershipRecordsByMonth();
}

function recordPeriodFilters() {
  const month = $("#recordMonthFilter")?.value || "";
  const basis = $("#recordDateBasisFilter")?.value || "receivedDate";
  return {
    month,
    basis,
    start: $("#recordStartDateFilter")?.value || "",
    end: $("#recordEndDateFilter")?.value || "",
    isAll: !month && !($("#recordStartDateFilter")?.value) && !($("#recordEndDateFilter")?.value)
  };
}

function applyRecordMonthPeriod(month) {
  if (!month) return;
  const setting = monthSetting(month);
  const startInput = $("#recordStartDateFilter");
  const endInput = $("#recordEndDateFilter");
  if (startInput) startInput.value = setting.periodStart || `${month}-01`;
  if (endInput) endInput.value = setting.periodEnd || lastDayOfMonth(month);
}


function syncRecordFiltersToDashboardMonth(month = $("#monthFilter")?.value || monthIso()) {
  if (!month) return;
  const recordMonthFilter = $("#recordMonthFilter");
  if (recordMonthFilter) recordMonthFilter.value = month;
  applyRecordMonthPeriod(month);
}

function currentRecordPeriodRange() {
  return {
    start: $("#recordStartDateFilter")?.value || "",
    end: $("#recordEndDateFilter")?.value || ""
  };
}

function recordsByRecordPeriod() {
  const filters = recordPeriodFilters();
  return state.records.filter((record) => {
    const dateValue = record[filters.basis] || "";
    if (filters.start && dateValue < filters.start) return false;
    if (filters.end && dateValue > filters.end) return false;
    return true;
  });
}

function filteredRecords() {
  const filters = getFilters();
  return state.records.filter((record) => {
    const searchable = [
      record.customerName, record.customerNo, record.previousCustomer, record.phone,
      record.product, record.manager, record.category, record.memo, record.seller
    ].join(" ").toLowerCase();
    const selectedGoalMonth = normalizeManagerMonth(filters.month) || recordGoalMonth(record, currentDashboardMonth());
    return inDateRange(record.receivedDate, filters.start, filters.end)
      && recordBelongsToCurrentUserTeam(record, selectedGoalMonth)
      && (!filters.manager || record.manager === filters.manager)
      && (!filters.search || searchable.includes(filters.search));
  });
}

function sumBy(records, category) {
  return records
    .filter((record) => categoryMatches(record.category, category) && record.status !== "취소")
    .reduce((sum, record) => sum + toNumber(record.count), 0);
}

function categoryMatches(actual, expected) {
  const normalized = normalizeCategory(actual);
  if (expected === "재렌탈") return normalized === "재렌탈";
  return normalized === expected;
}

function activityTypeCount(records, type) {
  return (records || [])
    .filter((record) => record && record.status !== "취소" && recordActivityType(record) === type)
    .reduce((sum, record) => {
      const count = toNumber(record.count);
      return sum + (count > 0 ? count : 1);
    }, 0);
}

function actuals(records) {
  const active = records.filter((record) => record.status !== "취소");
  const newCount = sumBy(active, "신규");
  const packageCount = sumBy(active, "패키지");
  const cashActual = sumBy(active, "일시불");
  const newActual = newCount + packageCount + cashActual;
  const rentalActual = sumBy(active, "재렌탈");
  const orderConsActual = activityTypeCount(active, "컨스");
  const supportActual = activityTypeCount(active, "지원");
  const renewalActual = 0;
  const refundActual = 0;
  // 컨스는 기존 판매접수에 붙는 관리 구분이므로 별도 건수로만 집계하고 영업실적 계산에는 중복 가산하지 않습니다.
  const coreActual = newActual + rentalActual + supportActual;
  const businessActual = coreActual;
  const overallActual = coreActual - refundActual + renewalActual;
  const managerFinalActual = businessActual + renewalActual - refundActual;
  return {
    newCount, packageCount, newActual, rentalActual, cashActual, renewalActual,
    orderConsActual, supportActual, refundActual, coreActual, businessActual, overallActual, managerFinalActual
  };
}

function cashTotal(records) {
  return records
    .filter((record) => record.status !== "취소")
    .reduce((sum, record) => sum + toNumber(record.cashAmount), 0);
}


function isWaterPurifierCpRecord(record) {
  const product = String(record?.product || "").trim().normalize("NFKC").toUpperCase();
  return product.startsWith("CP-");
}

function isWaterPurifierSalesRecord(record) {
  // V10.70 공식 정수기 판매실적 기준:
  // 취소가 아니고, 제품명이 CP-로 시작하며,
  // 판매종류가 신규/패키지/재렌탈/일시불인 실제 영업접수행만 인정합니다.
  // 맴버쉽/멤버십은 별도 멤버십 실적이므로 절대 포함하지 않습니다.
  if (!record || record.status === "취소" || !isWaterPurifierCpRecord(record)) return false;
  const category = normalizeCategory(record.category);
  return ["신규", "패키지", "재렌탈", "일시불"].includes(category);
}

function waterPurifierCpCount(records) {
  // count 값과 무관하게 조건에 맞는 접수행 1개 = 정수기 1건
  return (records || []).filter(isWaterPurifierSalesRecord).length;
}

function dashboardConditionPhysicalCount(records, card) {
  // 대시보드 조건 건수현황은 접수리스트의 count 값과 무관하게 매칭된 접수 행을 1건으로 계산합니다.
  return (records || [])
    .filter((record) => recordMatchesCustomCard(record, card))
    .length;
}


function memoKeywordCount(records, keyword) {
  const needle = String(keyword || "").trim().toLowerCase();
  if (!needle) return 0;
  return (records || [])
    .filter((record) => record && record.status !== "취소")
    .filter((record) => {
      if (needle === "지원") return analyticsActivityFlags(record).support;
      if (needle === "컨스" || needle === "콘스") return analyticsActivityFlags(record).cons;
      return String(record.memo || "").toLowerCase().includes(needle);
    })
    .reduce((sum, record) => {
      const count = toNumber(record.count);
      return sum + (count > 0 ? count : 1);
    }, 0);
}

function exactManagerSalesMetrics(records, managerName = "") {
  // 판매종류 기준 실적에 지원을 반영하고, 컨스는 별도 관리 건수로만 표시합니다.
  const base = applyManualStatsToTotals(actuals(records), managerName);
  const newCount = toNumber(base.newCount);
  const packageCount = toNumber(base.packageCount);
  const rentalCount = toNumber(base.rentalActual);
  const cashCount = toNumber(base.cashActual);
  const renewal = toNumber(base.renewalActual);
  const refund = toNumber(base.refundActual);
  const consCount = toNumber(base.orderConsActual);
  const supportCount = toNumber(base.supportActual);
  const business = newCount + packageCount + rentalCount + cashCount + supportCount;
  const final = business + renewal - refund;
  return {
    newCount, packageCount, rentalCount, cashCount, consCount, supportCount,
    waterPurifierCount: waterPurifierCpCount(records),
    business,
    renewal,
    refund,
    final
  };
}

function promotionOverlapsMonthPeriod(promo, month = $("#monthFilter")?.value || monthIso()) {
  const setting = monthSetting(month);
  const periodStart = setting.periodStart || `${month}-01`;
  const periodEnd = setting.periodEnd || lastDayOfMonth(month);
  const start = String(promo?.startDate || "");
  const end = String(promo?.endDate || "");
  if (!start && !end) return true;
  const promoStart = start || end;
  const promoEnd = end || start;
  return promoStart <= periodEnd && promoEnd >= periodStart;
}

function hundredPointPromotion(month = $("#monthFilter")?.value || monthIso()) {
  return (state.promotions || [])
    .map(normalizePromotion)
    .filter((promo) => String(promo.name || "").replace(/\s+/g, "").includes("100점"))
    .filter((promo) => promotionOverlapsMonthPeriod(promo, month))
    .sort((left, right) => String(right.startDate || "").localeCompare(String(left.startDate || "")))
    .at(0) || null;
}

function promotionRuleRecordMatches(record, promo, rule) {
  if (!record || record.status === "취소") return false;
  const dateValue = record.receivedDate || "";
  if (!inDateRange(dateValue, promo.startDate, promo.endDate)) return false;
  const text = String(record.product || "").toLowerCase();
  const keywords = Array.isArray(rule.keywords) && rule.keywords.length ? rule.keywords : [rule.keyword];
  const excludes = Array.isArray(rule.excludeKeywords) ? rule.excludeKeywords : [];
  const hasInclude = keywords.some((keyword) => keyword && text.includes(String(keyword).toLowerCase()));
  const hasExclude = excludes.some((keyword) => keyword && text.includes(String(keyword).toLowerCase()));
  return hasInclude && !hasExclude;
}

function hundredPointPromotionScoresForManager(managerName, promo = hundredPointPromotion(), sourceRecords = null) {
  if (!promo) return { rules: [], values: [], total: 0 };
  const rules = promoKeywordRules(promo);
  // 100점 누적점수는 설치 여부와 무관하게, 취소되지 않은 대상 접수건 전체를 합산합니다.
  // 미설치 건은 현황의 미설치 건수로 계속 보여 주되 점수에서는 제외하지 않습니다.
  const records = (sourceRecords || state.records || [])
    .filter((record) => promoCreditManagerName(record, promotionReferenceMonth(promo)) === managerName)
    .filter((record) => promoBaseRecordMatches(record, promo));
  const values = rules.map((rule, ruleIndex) => records.reduce((sum, record) => {
    const matched = matchedPromoKeyword(record, promo);
    const matchedIndex = rules.findIndex((candidate) =>
      candidate.title === matched?.title && candidate.keyword === matched?.keyword
    );
    return matchedIndex === ruleIndex ? sum + promoRecordScore(record, promo) : sum;
  }, 0));
  return { rules, values, total: values.reduce((sum, value) => sum + toNumber(value), 0) };
}


function categoryColorClass(category) {
  const normalized = normalizeCategory(category);
  const map = {
    "신규": "cat-new",
    "패키지": "cat-package",
    "재렌탈": "cat-rental",
    "일시불": "cat-cash",
    "맴버쉽": "cat-membership"
  };
  return map[normalized] || "cat-etc";
}

function statusColorClass(status) {
  const map = {
    "접수": "status-received",
    "요청": "status-request",
    "확인": "status-confirm",
    "완료": "status-done",
    "보류": "status-hold",
    "취소": "status-cancel"
  };
  return map[status] || "status-received";
}



function sellerOptionsForCategory(category, selectedValue = "") {
  if (normalizeCategory(category) === "맴버쉽") return membershipContactRoles;
  const managerNames = managerInputNames(selectedValue);
  return Array.from(new Set([...sellerRoles, ...managerNames, selectedValue].filter((value) => value !== undefined)));
}

function updateSellerInputOptions(selectedValue = $("#sellerInput")?.value || "") {
  const sellerInput = $("#sellerInput");
  if (!sellerInput) return;
  const category = $("#categoryInput")?.value || "";
  const label = $("#sellerInputLabel");
  if (label) label.childNodes[0].nodeValue = normalizeCategory(category) === "맴버쉽" ? "컨텍자" : "실판매자";
  setOptions(sellerInput, sellerOptionsForCategory(category).map((value) => ({ value, label: value || "선택" })), selectedValue);
}


function manualStatsMonthKey() {
  return $("#monthFilter")?.value || monthIso();
}

function normalizeManualStatsBucket() {
  if (!state.managerManualStats || typeof state.managerManualStats !== "object") state.managerManualStats = {};
  const keys = Object.keys(state.managerManualStats);
  const hasLegacyManagerValues = keys.some((key) => {
    const value = state.managerManualStats[key];
    return value && typeof value === "object" && (
      Object.prototype.hasOwnProperty.call(value, "renewal") ||
      Object.prototype.hasOwnProperty.call(value, "orderCons") ||
      Object.prototype.hasOwnProperty.call(value, "consDue") ||
      Object.prototype.hasOwnProperty.call(value, "consPaid") ||
      Object.prototype.hasOwnProperty.call(value, "consMemo") ||
      Object.prototype.hasOwnProperty.call(value, "support") ||
      Object.prototype.hasOwnProperty.call(value, "refund")
    );
  });
  if (hasLegacyManagerValues) {
    const legacy = state.managerManualStats;
    const month = manualStatsMonthKey();
    state.managerManualStats = { [month]: legacy };
  }
}

function manualStatsForMonth(month = manualStatsMonthKey()) {
  normalizeManualStatsBucket();
  if (!state.managerManualStats[month] || typeof state.managerManualStats[month] !== "object") {
    state.managerManualStats[month] = {};
  }
  return state.managerManualStats[month];
}

function manualStatFor(managerName, month = manualStatsMonthKey()) {
  const bucket = manualStatsForMonth(month);
  if (!bucket[managerName]) {
    bucket[managerName] = { renewal: 0, orderCons: 0, consDue: 0, consPaid: 0, consMemo: "", support: 0, refund: 0 };
  }
  const stat = bucket[managerName];
  if (stat.consDue === undefined && stat.consTarget !== undefined) stat.consDue = stat.consTarget;
  stat.renewal = toNumber(stat.renewal);
  stat.orderCons = toNumber(stat.orderCons); // legacy field; new sales count is automatic from 접수 구분
  stat.consPaid = toNumber(stat.consPaid);
  stat.consMemo = String(stat.consMemo || "");
  stat.support = toNumber(stat.support);
  stat.refund = toNumber(stat.refund);
  return stat;
}


function managerGoalMonthKey() {
  return $("#monthFilter")?.value || monthIso();
}

function ensureManagerMonthlyGoals(month = managerGoalMonthKey()) {
  if (!state.managerMonthlyGoals || typeof state.managerMonthlyGoals !== "object") state.managerMonthlyGoals = {};
  if (!state.managerMonthlyGoals[month] || typeof state.managerMonthlyGoals[month] !== "object") state.managerMonthlyGoals[month] = {};
  state.managers.forEach((rawManager) => {
    const manager = normalizeManager(rawManager);
    if (manager.name && state.managerMonthlyGoals[month][manager.name] === undefined) {
      state.managerMonthlyGoals[month][manager.name] = toNumber(manager.goal);
    }
  });
  return state.managerMonthlyGoals[month];
}

function managerGoalFor(managerName, month = managerGoalMonthKey()) {
  const bucket = ensureManagerMonthlyGoals(month);
  if (bucket[managerName] === undefined) {
    const manager = state.managers.map(normalizeManager).find((item) => item.name === managerName);
    bucket[managerName] = manager ? toNumber(manager.goal) : 0;
  }
  return toNumber(bucket[managerName]);
}

function setManagerGoalFor(managerName, goal, month = managerGoalMonthKey()) {
  const bucket = ensureManagerMonthlyGoals(month);
  if (managerName) bucket[managerName] = toNumber(goal);
}

function migrateLegacyManagerGoalsToMonth(month = managerGoalMonthKey()) {
  ensureManagerMonthlyGoals(month);
}

function normalizeManagerMonth(value = "") {
  const month = String(value || "").trim();
  return /^\d{4}-\d{2}$/.test(month) ? month : "";
}

function normalizeTeamName(value) {
  return String(value || "").trim();
}

function normalizeTeamNames(value, managers = []) {
  const explicit = Array.isArray(value)
    ? value.map(normalizeTeamName).filter(Boolean)
    : [];
  if (explicit.length) return [...new Set(explicit)];

  const detected = [...new Set(
    (Array.isArray(managers) ? managers : [])
      .flatMap((manager) => {
        const history = Array.isArray(manager?.teamHistory) ? manager.teamHistory.map((item) => item?.team) : [];
        return [manager?.team, ...history];
      })
      .map(normalizeTeamName)
      .filter(Boolean)
  )];

  return detected.length >= 2 ? detected : ["원팀"];
}

function configuredTeamNames() {
  const names = normalizeTeamNames(state?.teamNames, state?.managers || []);
  return names.length ? names : ["원팀"];
}

function defaultTeamName() {
  return configuredTeamNames()[0] || "원팀";
}

function normalizeTeamHistoryForNames(history, fallbackTeam, joinedMonth, teamNames = configuredTeamNames()) {
  const names = teamNames.length ? teamNames : ["원팀"];
  const explicitFallback = normalizeTeamName(fallbackTeam);
  const fallback = explicitFallback || names[0];
  const source = Array.isArray(history) ? history : [];
  const normalized = source.map((item) => {
    const team = normalizeTeamName(item?.team);
    return {
      // 저장된 소속팀을 현재 팀 목록의 첫 팀으로 강제 치환하지 않습니다.
      team: team || fallback,
      startMonth: normalizeManagerMonth(item?.startMonth),
      endMonth: normalizeManagerMonth(item?.endMonth)
    };
  }).filter((item) => item.team);

  if (!normalized.length) normalized.push({ team: fallback, startMonth: normalizeManagerMonth(joinedMonth), endMonth: "" });
  normalized.sort((a, b) => String(a.startMonth || "").localeCompare(String(b.startMonth || "")));
  return normalized;
}

function normalizeManagerTeamHistory(history, fallbackTeam = defaultTeamName(), joinedMonth = "") {
  return normalizeTeamHistoryForNames(history, fallbackTeam, joinedMonth);
}

function normalizeStatusHistory(history, fallbackStatus = "active", joinedMonth = "") {
  const source = Array.isArray(history) ? history : [];
  const normalized = source.map((item) => ({
    status: item?.status === "inactive" ? "inactive" : "active",
    startMonth: normalizeManagerMonth(item?.startMonth),
    endMonth: normalizeManagerMonth(item?.endMonth)
  })).filter((item) => item.startMonth || item.status);
  if (!normalized.length) {
    normalized.push({ status: fallbackStatus === "inactive" ? "inactive" : "active", startMonth: normalizeManagerMonth(joinedMonth), endMonth: "" });
  }
  normalized.sort((a, b) => String(a.startMonth || "").localeCompare(String(b.startMonth || "")));
  return normalized;
}

function managerStatusForMonth(managerOrName, month = currentDashboardMonth()) {
  const manager = typeof managerOrName === "string" ? managerByName(managerOrName) : managerOrName;
  if (!manager?.name) return "inactive";
  const targetMonth = normalizeManagerMonth(month) || monthIso();
  const history = normalizeStatusHistory(manager.statusHistory, manager.status === "inactive" ? "inactive" : "active", manager.joinedMonth);
  const matching = history.filter((item) =>
    (!item.startMonth || item.startMonth <= targetMonth) && (!item.endMonth || targetMonth <= item.endMonth)
  ).sort((a,b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0];
  if (matching?.status) return matching.status;
  const first = history[0];
  if (first?.startMonth && targetMonth < first.startMonth) return "inactive";
  return manager.status === "inactive" ? "inactive" : "active";
}

function normalizeManager(manager = {}, teamNamesOverride = null) {
  const overrideNames = Array.isArray(teamNamesOverride)
    ? teamNamesOverride.map(normalizeTeamName).filter(Boolean)
    : [];
  const names = overrideNames.length ? [...new Set(overrideNames)] : configuredTeamNames();
  const explicitTeam = normalizeTeamName(manager.team);
  const fallbackTeam = explicitTeam || names[0] || "원팀";
  const joinedMonth = normalizeManagerMonth(manager.joinedMonth || manager.startMonth);
  const inactiveMonth = normalizeManagerMonth(manager.inactiveMonth || manager.endMonth);
  const status = manager.status === "inactive" || manager.active === false ? "inactive" : "active";
  const teamHistory = normalizeTeamHistoryForNames(manager.teamHistory, fallbackTeam, joinedMonth, names);
  const latestHistory = [...teamHistory].sort((a, b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0];
  let statusHistory = normalizeStatusHistory(manager.statusHistory, status, joinedMonth);
  // 구버전의 inactiveMonth만 존재하는 데이터는 기존 비활성 시작월을 이력으로 승격합니다.
  if ((!Array.isArray(manager.statusHistory) || !manager.statusHistory.length) && inactiveMonth) {
    const activeStart = joinedMonth || "";
    statusHistory = [{ status: "active", startMonth: activeStart, endMonth: shiftMonth(inactiveMonth, -1) }, { status: "inactive", startMonth: inactiveMonth, endMonth: "" }];
  }
  return {
    id: manager.id || uid("m"),
    name: String(manager.name || "").trim(),
    team: explicitTeam || latestHistory?.team || fallbackTeam,
    areas: Array.isArray(manager.areas)
      ? manager.areas.map((item) => String(item || "").trim()).filter(Boolean)
      : String(manager.areas || "").split(",").map((item) => item.trim()).filter(Boolean),
    goal: toNumber(manager.goal),
    displayOrder: Math.max(0, Math.floor(toNumber(manager.displayOrder))),
    status,
    joinedMonth,
    inactiveMonth,
    statusHistory,
    teamHistory,
    changeHistory: Array.isArray(manager.changeHistory)
      ? manager.changeHistory.filter((entry) => entry && normalizeManagerMonth(entry.month))
        .map((entry) => ({ month: normalizeManagerMonth(entry.month), type: String(entry.type || ""), team: String(entry.team || ""), status: entry.status === "inactive" ? "inactive" : "active" }))
      : [],
    historyEdits: Array.isArray(manager.historyEdits) ? manager.historyEdits.filter((entry) => entry && typeof entry.at === "string" && entry.before && entry.after).map((entry) => ({at: entry.at, before: entry.before, after: entry.after})) : [],
    createdAt: String(manager.createdAt || ""),
    updatedAt: String(manager.updatedAt || "")
  };
}

function managerById(managerId, managers = state.managers || []) {
  const id = String(managerId || "").trim();
  if (!id) return null;
  return managerIndex(managers).byId.get(id) || null;
}

function managerByName(managerName, managers = state.managers || []) {
  const name = String(managerName || "").trim();
  if (!name) return null;
  return managerIndex(managers).byName.get(name) || null;
}



function managerTeamForMonth(managerOrName, month = currentDashboardMonth()) {
  const manager = typeof managerOrName === "string" ? managerByName(managerOrName) : normalizeManager(managerOrName || {});
  if (!manager?.name) return manager?.team || defaultTeamName();
  const targetMonth = normalizeManagerMonth(month) || monthIso();
  const history = normalizeManagerTeamHistory(manager.teamHistory, manager.team, manager.joinedMonth);
  const matching = history
    .filter((item) => (!item.startMonth || item.startMonth <= targetMonth) && (!item.endMonth || targetMonth <= item.endMonth))
    .sort((a, b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0];
  return matching?.team || manager.team || defaultTeamName();
}

function managerIsActiveForMonth(managerOrName, month = currentDashboardMonth()) {
  const manager = typeof managerOrName === "string" ? managerByName(managerOrName) : normalizeManager(managerOrName || {});
  if (!manager?.name) return false;
  const targetMonth = normalizeManagerMonth(month) || monthIso();
  if (manager.joinedMonth && targetMonth < manager.joinedMonth) return false;
  return managerStatusForMonth(manager, targetMonth) === "active";
}

function applyManagerStatusChange(manager, nextStatus, effectiveMonth) {
  const normalized = normalizeManager(manager);
  const status = nextStatus === "inactive" ? "inactive" : "active";
  const month = normalizeManagerMonth(effectiveMonth) || monthIso();
  const history = normalizeStatusHistory(normalized.statusHistory, normalized.status, normalized.joinedMonth).map((item) => ({ ...item }));
  const currentMonth = monthIso();
  const currentEntry = historyEntryForMonth(history, currentMonth, "status");

  // 적용월을 현재 이력보다 과거로 옮기는 것은 '새 이력 추가'가 아니라
  // 현재 행의 상태 시작월을 앞당기는 수정입니다. 기존 현재 이력을 미래로 남기면
  // 비활성/재직이 다음 달에 다시 원래 상태로 돌아가는 문제가 발생합니다.
  if (currentEntry?.startMonth && month <= currentEntry.startMonth) {
    const others = history.filter((item) => item !== currentEntry);
    const prior = others.filter((item) => (!item.startMonth || item.startMonth < month))
      .sort((a,b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")));
    const future = others.filter((item) => item.startMonth && item.startMonth > month)
      .sort((a,b) => String(a.startMonth || "").localeCompare(String(b.startMonth || "")));
    const nextStart = future[0]?.startMonth || "";
    if (prior[0] && (!prior[0].endMonth || prior[0].endMonth >= month)) prior[0].endMonth = shiftMonth(month, -1);
    const moved = { ...currentEntry, status, startMonth: month, endMonth: nextStart ? shiftMonth(nextStart, -1) : "" };
    return normalizeStatusHistory([...prior, moved, ...future], status, normalized.joinedMonth);
  }

  const prior = history.filter((item) => (!item.startMonth || item.startMonth < month));
  const future = history.filter((item) => item.startMonth && item.startMonth > month);
  const exact = history.find((item) => item.startMonth === month);
  const previous = prior.slice().sort((a,b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0];
  const nextStart = future.slice().sort((a,b) => String(a.startMonth || "").localeCompare(String(b.startMonth || "")))[0]?.startMonth || "";
  if (previous && (!previous.endMonth || previous.endMonth >= month)) previous.endMonth = shiftMonth(month, -1);
  if (exact) {
    exact.status = status;
    exact.endMonth = nextStart ? shiftMonth(nextStart, -1) : "";
    return normalizeStatusHistory(history, status, normalized.joinedMonth);
  }
  return normalizeStatusHistory([...prior, { status, startMonth: month, endMonth: nextStart ? shiftMonth(nextStart, -1) : "" }, ...future], status, normalized.joinedMonth);
}

function managerHistoryLabel(managerOrName) {
  const manager = typeof managerOrName === "string"
    ? managerByName(managerOrName)
    : normalizeManager(managerOrName || {});
  if (!manager?.name) return "";
  const history = manager.teamHistory.map((item) => {
    const period = item.startMonth
      ? `${formatMonthLabel(item.startMonth)}${item.endMonth ? `~${formatMonthLabel(item.endMonth)}` : "~"}`
      : "기존";
    return `${period} ${item.team}`;
  });
  const statusHistory = normalizeStatusHistory(manager.statusHistory, manager.status, manager.joinedMonth);
  statusHistory.forEach((item) => {
    if (item.status === "inactive") history.push(`${formatMonthLabel(item.startMonth)}부터 비활성`);
    else if (item.startMonth) history.push(`${formatMonthLabel(item.startMonth)}부터 재직`);
  });
  return history.join(" · ");
}

function applyManagerTeamChange(manager, nextTeam, effectiveMonth) {
  const normalized = normalizeManager(manager);
  const names = configuredTeamNames();
  const team = names.includes(normalizeTeamName(nextTeam)) ? normalizeTeamName(nextTeam) : defaultTeamName();
  const month = normalizeManagerMonth(effectiveMonth) || monthIso();
  const history = normalizeManagerTeamHistory(normalized.teamHistory, normalized.team, normalized.joinedMonth)
    .map((item) => ({ ...item }));

  // 이 입력창은 '새 이력 추가'가 아니라 현재 행의 팀/적용월을 수정하는 UI입니다.
  // 따라서 기존 최신/현재 이력의 시작월을 사용자가 과거로 옮긴 경우에는 기존 이력을
  // 미래 이력으로 남겨두지 않고 그 이력 자체의 시작월을 이동시켜야 합니다.
  // 이전 구현은 09월 이력에서 적용월을 08월로 바꾸면 08월 새 이력 + 09월 기존 이력을
  // 동시에 만들어 09월에 다시 원래 팀으로 돌아오는 현상이 발생했습니다.
  const sorted = history.slice().sort((a, b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")));
  const currentEntry = sorted.find((item) => !item.endMonth || item.endMonth >= monthIso()) || sorted[0];
  const currentStart = currentEntry?.startMonth || normalized.joinedMonth || monthIso();

  // 현재 행이 나타내는 기존 이력의 시작월을 직접 이동하는 경우
  // (특히 적용월을 현재/과거 월로 변경하는 경우) 해당 이력을 수정합니다.
  if (currentEntry?.startMonth && month <= currentStart) {
    const others = history.filter((item) => item !== currentEntry);
    const prior = others.filter((item) => (!item.startMonth || item.startMonth < month))
      .sort((a, b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")));
    const future = others.filter((item) => item.startMonth && item.startMonth > month)
      .sort((a, b) => String(a.startMonth || "").localeCompare(String(b.startMonth || "")));
    const nextStart = future[0]?.startMonth || "";
    const movedEntry = {
      ...currentEntry,
      team,
      startMonth: month,
      endMonth: nextStart ? shiftMonth(nextStart, -1) : ""
    };
    if (prior[0] && (!prior[0].endMonth || prior[0].endMonth >= month)) {
      prior[0].endMonth = shiftMonth(month, -1);
    }
    return normalizeManagerTeamHistory([...prior, movedEntry, ...future], team, normalized.joinedMonth);
  }

  // 미래월로 새 팀 이동을 예약하는 경우에는 현재 이력을 유지하고 새 경계를 추가합니다.
  const prior = history.filter((item) => (!item.startMonth || item.startMonth < month));
  const future = history.filter((item) => item.startMonth && item.startMonth > month);
  const exact = history.find((item) => item.startMonth === month);
  const previous = prior.slice().sort((a, b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0];
  const nextStart = future.slice().sort((a, b) => String(a.startMonth || "").localeCompare(String(b.startMonth || "")))[0]?.startMonth || "";

  if (previous && (!previous.endMonth || previous.endMonth >= month)) {
    previous.endMonth = shiftMonth(month, -1);
  }
  if (exact) {
    exact.team = team;
    exact.endMonth = nextStart ? shiftMonth(nextStart, -1) : "";
    return normalizeManagerTeamHistory(history, team, normalized.joinedMonth);
  }
  return normalizeManagerTeamHistory([
    ...prior,
    { team, startMonth: month, endMonth: nextStart ? shiftMonth(nextStart, -1) : "" },
    ...future
  ], team, normalized.joinedMonth);
}

function managerDisplayOrderValue(managerOrName) {
  const manager = typeof managerOrName === "string"
    ? managerByName(managerOrName)
    : managerOrName;
  const order = Math.floor(toNumber(manager?.displayOrder));
  return order > 0 ? order : Number.MAX_SAFE_INTEGER;
}

function sortManagersByDisplayOrder(managers = []) {
  const normalized = managerIndex(managers).normalized.slice();
  return normalized.sort((a, b) => {
    const orderDiff = managerDisplayOrderValue(a) - managerDisplayOrderValue(b);
    if (orderDiff !== 0) return orderDiff;
    return String(a.name || "").localeCompare(String(b.name || ""), "ko");
  });
}

function sortManagerNamesByDisplayOrder(names = []) {
  const unique = [...new Set((names || []).map((name) => String(name || "").trim()).filter(Boolean))];
  const index = managerIndex();
  return unique.sort((a, b) => {
    const aManager = index.byName.get(a);
    const bManager = index.byName.get(b);
    if (aManager && bManager) {
      const orderDiff = managerDisplayOrderValue(aManager) - managerDisplayOrderValue(bManager);
      if (orderDiff !== 0) return orderDiff;
    } else if (aManager) {
      return -1;
    } else if (bManager) {
      return 1;
    }
    return a.localeCompare(b, "ko");
  });
}


// 목표월의 조회 범위와 매니저의 재직/소속 적용월은 서로 다릅니다.
// 조회기간이 두 달에 걸치면, 두 달 중 실제 재직한 기간이 있는 매니저를 보존합니다.
function calendarMonthsForGoalPeriod(month) {
  const goalMonth = normalizeManagerMonth(month) || monthIso();
  const period = monthPeriod(goalMonth);
  const start = organizationMonthForDate(period.start, goalMonth);
  const end = organizationMonthForDate(period.end, goalMonth);
  if (start > end) return [goalMonth];
  const months = [];
  for (let current = start; current <= end && months.length < 24; current = shiftMonth(current, 1)) {
    months.push(current);
  }
  return months.length ? months : [goalMonth];
}

function activeManagerForCalendarMonth(manager, calendarMonth) {
  const enabled = managerIsActiveForMonth(manager, calendarMonth);
  if (!enabled) return false;
  return teamOperationMode(calendarMonth) === "1"
    || managerTeamForMonth(manager, calendarMonth) === currentUserTeamName(calendarMonth);
}

function teamManagers(month = currentDashboardMonth()) {
  const targetMonth = normalizeManagerMonth(month) || monthIso();
  const calendarMonths = calendarMonthsForGoalPeriod(targetMonth);
  const { start, end } = monthPeriod(targetMonth);
  // 매니저마다 전체 접수를 다시 검색하면 데이터가 많을 때 화면이 느려집니다.
  // 접수는 한 번만 순회하여 해당 목표기간의 담당 매니저를 찾습니다.
  const recordManagerIds = new Set();
  const recordManagerNames = new Set();
  (state.records || []).forEach((record) => {
    if (!record || isMembershipRecord(record) || !inDateRange(record.receivedDate || "", start, end)) return;
    if (!recordBelongsToCurrentUserTeam(record, targetMonth)) return;
    if (record.managerId) recordManagerIds.add(record.managerId);
    if (record.manager) recordManagerNames.add(String(record.manager).trim());
  });
  return managerIndex().normalized
    .filter((manager) => {
      // 새 목표월 전체에 퇴사자를 무조건 나열하지 않습니다. 현재월 재직자이거나
      // 목표산정기간에 실제 과거 접수/조정내역이 있는 매니저만 표시합니다.
      const currentMember = activeManagerForCalendarMonth(manager, targetMonth);
      const historicalMember = calendarMonths.some((calendarMonth) => activeManagerForCalendarMonth(manager, calendarMonth));
      const hasPeriodRecords = recordManagerIds.has(manager.id) || recordManagerNames.has(manager.name);
      const adjustment = state.managerManualStats?.[targetMonth]?.[manager.name];
      const hasAdjustment = adjustment && (Number(adjustment.renewal || 0) !== 0 || Number(adjustment.refund || 0) !== 0);
      return currentMember || hasPeriodRecords || (historicalMember && hasAdjustment);
    })
    .slice()
    .sort((a, b) => {
      const orderDiff = managerDisplayOrderValue(a) - managerDisplayOrderValue(b);
      if (orderDiff !== 0) return orderDiff;
      return String(a.name || "").localeCompare(String(b.name || ""), "ko");
    });
}

// 신규 접수는 목표기간의 다른 달이 아닌 실제 접수일의 달력월을 기준으로 선택합니다.
function activeTeamManagerNames(month = monthIso()) {
  const calendarMonth = normalizeManagerMonth(month) || monthIso();
  return sortManagersByDisplayOrder(managerIndex().normalized
    .filter((manager) => activeManagerForCalendarMonth(manager, calendarMonth)))
    .map((manager) => manager.name);
}

function allManagerNames() {
  if (allManagerNamesCache.revision === stateRevision) {
    return allManagerNamesCache.names.slice();
  }

  const names = new Set(managerIndex().normalized.map((manager) => manager.name).filter(Boolean));
  const managerNames = managerIndex().byName;

  (state.records || []).forEach((record) => {
    if (record?.manager) names.add(String(record.manager).trim());
    if (record?.seller && managerNames.has(String(record.seller).trim())) {
      names.add(String(record.seller).trim());
    }
  });

  const sorted = sortManagerNamesByDisplayOrder([...names].filter(Boolean));
  allManagerNamesCache = { revision: stateRevision, names: sorted };
  return sorted.slice();
}

function teamManagerNames(month = currentDashboardMonth()) {
  return teamManagers(month).map((manager) => manager.name);
}

function recordEntryMonth(explicitMonth = "") {
  const receivedDate = String($("#receivedDateInput")?.value || "");
  const explicit = normalizeManagerMonth(explicitMonth);
  if (receivedDate) return organizationMonthForDate(receivedDate, explicit || normalizeManagerMonth($("#monthFilter")?.value));
  return explicit
    || normalizeManagerMonth($("#recordMonthFilter")?.value)
    || normalizeManagerMonth($("#monthFilter")?.value)
    || monthIso();
}

function managerInputNames(selectedValue = "", month = "", preserveSelected = true) {
  const targetMonth = recordEntryMonth(month);
  const names = new Set(activeTeamManagerNames(targetMonth));
  const selected = String(selectedValue || "").trim();
  if (preserveSelected && selected) names.add(selected);
  return sortManagerNamesByDisplayOrder([...names].filter(Boolean));
}

function refreshRecordManagerOptions(month = "", preferredValue = "", preserveSelected = Boolean($("#recordId")?.value)) {
  const input = $("#managerInput");
  if (!input) return;
  const current = String(preferredValue || input.value || "").trim();
  const targetMonth = recordEntryMonth(month);
  const options = managerInputNames(current, targetMonth, preserveSelected);
  const nextValue = current && options.includes(current) ? current : (options[0] || "");
  setOptions(input, options, nextValue);
  input.value = nextValue;
}

function ensureRecordManagerReference(record, managers = state.managers || []) {
  if (!record || typeof record !== "object") return record;
  const recordMonth = organizationMonthForDate(record.receivedDate || record.installDate, monthIso());

  const registeredManager = managerById(record.managerId, managers) || managerByName(record.manager, managers);
  if (registeredManager) {
    record.managerId = registeredManager.id;
    record.managerNameAtRecord = record.managerNameAtRecord || String(record.manager || registeredManager.name);
    record.managerTeamAtRecord = record.managerTeamAtRecord || managerTeamForMonth(registeredManager, recordMonth);
  }

  const sellerManager = managerById(record.sellerId, managers) || managerByName(record.seller, managers);
  if (sellerManager) {
    record.sellerId = sellerManager.id;
    record.sellerNameAtRecord = record.sellerNameAtRecord || String(record.seller || sellerManager.name);
  }

  return record;
}

function ensureManagerDataIntegrity(targetState = state) {
  if (!targetState || typeof targetState !== "object") return targetState;

  const targetTeamNames = normalizeTeamNames(targetState.teamNames, targetState.managers);
  targetState.managers = (Array.isArray(targetState.managers) ? targetState.managers : [])
    .map((manager, index) => {
      const normalized = normalizeManager(manager, targetTeamNames);
      if (!(normalized.displayOrder > 0)) normalized.displayOrder = index + 1;
      return normalized;
    });

  const ids = new Set();
  targetState.managers.forEach((manager) => {
    if (!manager.id || ids.has(manager.id)) manager.id = uid("m");
    ids.add(manager.id);
  });

  targetState.managers = targetState.managers
    .slice()
    .sort((a, b) => {
      const orderDiff = managerDisplayOrderValue(a) - managerDisplayOrderValue(b);
      if (orderDiff !== 0) return orderDiff;
      return String(a.name || "").localeCompare(String(b.name || ""), "ko");
    })
    .map((manager, index) => ({ ...manager, displayOrder: index + 1 }));

  const byId = new Map(targetState.managers.map((manager) => [manager.id, manager]));
  const byName = new Map(targetState.managers.map((manager) => [manager.name, manager]));

  targetState.records = (Array.isArray(targetState.records) ? targetState.records : [])
    .map((record) => {
      if (!record || typeof record !== "object") return record;
      const recordMonth = organizationMonthForDate(record.receivedDate || record.installDate, monthIso());

      const registeredManager = byId.get(String(record.managerId || ""))
        || byName.get(String(record.manager || "").trim());
      if (registeredManager) {
        record.managerId = registeredManager.id;
        record.managerNameAtRecord = record.managerNameAtRecord || String(record.manager || registeredManager.name);
        record.managerTeamAtRecord = record.managerTeamAtRecord || managerTeamForMonth(registeredManager, recordMonth);
        record.managerStatusAtRecord = record.managerStatusAtRecord || managerStatusForMonth(registeredManager, recordMonth);
      }

      const sellerManager = byId.get(String(record.sellerId || ""))
        || byName.get(String(record.seller || "").trim());
      if (sellerManager) {
        record.sellerId = sellerManager.id;
        record.sellerNameAtRecord = record.sellerNameAtRecord || String(record.seller || sellerManager.name);
      }

      return record;
    });

  return targetState;
}

function manualTotals(month = manualStatsMonthKey()) {
  return teamManagers(month).reduce((sum, manager) => {
    const stat = manualStatFor(manager.name, month);
    sum.renewal += toNumber(stat.renewal);
    sum.orderCons += toNumber(stat.orderCons);
    sum.support += toNumber(stat.support);
    sum.refund += toNumber(stat.refund);
    return sum;
  }, { renewal: 0, orderCons: 0, support: 0, refund: 0 });
}

function applyManualStatsToTotals(totals, managerName = "", month = manualStatsMonthKey()) {
  const manual = managerName ? manualStatFor(managerName, month) : manualTotals(month);
  totals.renewalActual += toNumber(manual.renewal);
  // 컨스/지원은 접수리스트의 구분값으로 자동 집계된 값을 그대로 유지합니다.
  // legacy orderCons/support 수기값은 과거 데이터 호환을 위해 보존하지만 실적 계산에는 사용하지 않습니다.
  totals.refundActual += toNumber(manual.refund);
  totals.overallActual = totals.coreActual - totals.refundActual + totals.renewalActual;
  totals.managerFinalActual = totals.businessActual + totals.renewalActual - totals.refundActual;
  return totals;
}




function defaultDashboardCustomCards() {
  return [
    { title: "정수기", field: "product", operator: "startsWith", value: "CP", enabled: true, conditions: [{ field: "product", operator: "startsWith", value: "CP", connector: "" }] },
    { title: "조건2", field: "product", operator: "contains", value: "", enabled: false, conditions: [] },
    { title: "조건3", field: "product", operator: "contains", value: "", enabled: false, conditions: [] },
    { title: "조건4", field: "product", operator: "contains", value: "", enabled: false, conditions: [] }
  ];
}
function normalizeDashboardCondition(condition = {}, fallback = {}) {
  const allowedFields = ["product", "category"];
  return {
    field: allowedFields.includes(condition.field) ? condition.field : (fallback.field || "product"),
    operator: ["startsWith", "contains", "equals"].includes(condition.operator) ? condition.operator : (fallback.operator || "contains"),
    value: String(condition.value ?? fallback.value ?? "").trim(),
    connector: condition.connector === "AND" ? "AND" : (condition.connector === "OR" ? "OR" : "")
  };
}
function normalizeDashboardCustomCard(raw = {}, index = 0) {
  const defaults = defaultDashboardCustomCards()[index] || defaultDashboardCustomCards()[0];
  const enabled = raw.enabled === undefined ? Boolean(defaults.enabled) : Boolean(raw.enabled);
  const normalizedTitle = String(raw.title ?? defaults.title ?? `조건${index + 1}`).trim() || `조건${index + 1}`;
  if (enabled && normalizedTitle === "매트리스") {
    const conditions = [
      { field: "product", operator: "startsWith", value: "CRM-", connector: "" },
      { field: "product", operator: "contains", value: "6C", connector: "AND" },
      { field: "product", operator: "contains", value: "12C", connector: "OR" },
      { field: "product", operator: "contains", value: "4C", connector: "OR" },
      { field: "product", operator: "contains", value: "케어B", connector: "OR" }
    ];
    return {
      title: normalizedTitle,
      field: "product",
      operator: "startsWith",
      value: "CRM-",
      enabled: true,
      conditions
    };
  }


  // 사용하지 않는 카드에는 예전 버전에서 남은 조건 데이터를 표시하지 않는다.
  // 카드가 다시 사용으로 켜지면 + 조건 추가로 새 조건을 만들 수 있다.
  if (!enabled) {
    return {
      title: String(raw.title ?? defaults.title ?? `조건${index + 1}`).trim() || `조건${index + 1}`,
      field: "product",
      operator: "contains",
      value: "",
      enabled: false,
      conditions: []
    };
  }

  let conditions = [];
  if (Array.isArray(raw.conditions)) {
    conditions = raw.conditions
      .map((condition, i) =>
        normalizeDashboardCondition(condition, defaults.conditions?.[i] || defaults.conditions?.[0] || {})
      )
      .filter(c => c.value);
  }

  // 이전 그룹형 데이터 호환
  if (!conditions.length && Array.isArray(raw.conditionGroups)) {
    const groups = raw.conditionGroups
      .map(group => Array.isArray(group)
        ? group.map(c => normalizeDashboardCondition(c)).filter(c => c.value)
        : [])
      .filter(group => group.length);

    if (groups.length) {
      const flattened = [];
      groups.forEach((group) => {
        group.forEach((c, itemIndex) => {
          flattened.push({
            ...c,
            connector: flattened.length === 0 ? "" : (itemIndex === 0 ? "OR" : "AND")
          });
        });
      });
      conditions = flattened;
    }
  }

  // 이전의 쉼표 구분형 데이터 호환
  if (!conditions.length && raw.value) {
    const values = String(raw.value).split(",").map(v => v.trim()).filter(Boolean);
    const field = raw.field === "category" ? "category" : "product";
    const operator = ["startsWith", "contains", "equals"].includes(raw.operator)
      ? raw.operator
      : "contains";

    conditions = values.map((value, i) => ({
      field,
      operator,
      value,
      connector: i === 0 ? "" : "OR"
    }));
  }

  // 정수기 기본카드만 기본 조건 유지
  if (!conditions.length && Array.isArray(defaults.conditions) && defaults.conditions.length) {
    conditions = defaults.conditions.map(c => ({ ...c }));
  }

  const first = conditions[0] || {
    field: "product",
    operator: "contains",
    value: "",
    connector: ""
  };

  conditions = conditions.map((c, i) => ({
    field: first.field,
    operator: ["startsWith", "contains", "equals"].includes(c.operator) ? c.operator : "contains",
    value: String(c.value || "").trim(),
    connector: i === 0 ? "" : (c.connector === "AND" ? "AND" : "OR")
  })).filter(c => c.value);

  return {
    title: String(raw.title ?? defaults.title ?? `조건${index + 1}`).trim() || `조건${index + 1}`,
    field: first.field,
    operator: first.operator,
    value: first.value,
    enabled: true,
    conditions
  };
}

function dashboardCustomCards() {
  if (!Array.isArray(state.dashboardCustomCards)) state.dashboardCustomCards = defaultDashboardCustomCards();
  const cards = Array.from({ length: 4 }, (_, index) =>
    normalizeDashboardCustomCard(state.dashboardCustomCards[index], index)
  );
  state.dashboardCustomCards = cards;
  return cards;
}
function customCardFieldValue(record, field) {
  const map = { product: record.product, category: normalizeCategory(record.category) };
  return String(map[field] ?? "").trim();
}
function recordMatchesDashboardCondition(record, condition) {
  const actual = customCardFieldValue(record, condition.field).toLowerCase();
  const expected = String(condition.value || "").trim().toLowerCase();
  if (!expected) return false;
  if (condition.operator === "startsWith") return actual.startsWith(expected);
  if (condition.operator === "equals") return actual === expected;
  return actual.includes(expected);
}
function recordMatchesCustomCard(record, card) {
  if (!record || record.status === "취소") return false;
  if (!card.enabled) return false;

  // 매트리스 카드는 경영평가와 동일한 고정 기준을 사용합니다.
  // CRM-으로 시작하면서 6C/12C/4C/케어B 중 하나가 포함된 경우만 1건.
  if (String(card.title || "").trim() === "매트리스") {
    return dashboardMattressCareMatch(record);
  }

  if (!Array.isArray(card.conditions) || !card.conditions.length) return false;
  const conditions = card.conditions.filter(c => String(c.value || "").trim());
  if (!conditions.length) return false;

  const firstMatch = recordMatchesDashboardCondition(record, conditions[0]);
  if (conditions.length === 1) return firstMatch;

  // Flat UI semantics: A AND (B OR C OR D)
  if (conditions[1].connector === "AND" && conditions.slice(2).every(c => c.connector === "OR")) {
    return firstMatch && conditions.slice(1).some(c => recordMatchesDashboardCondition(record, c));
  }

  let result = firstMatch;
  for (let i = 1; i < conditions.length; i += 1) {
    const match = recordMatchesDashboardCondition(record, conditions[i]);
    result = conditions[i].connector === "AND" ? (result && match) : (result || match);
  }
  return result;
}

function customCardCount(records, card) {
  // 모든 집중관리 조건카드는 매칭된 접수 행을 1건으로 계산합니다.
  return (records || []).filter(record => recordMatchesCustomCard(record, card)).length;
}

function waterPurifierMonthRecords(month = currentDashboardMonth()) {
  const period = monthPeriod(month);
  return (state.records || []).filter((record) =>
    isWaterPurifierSalesRecord(record) &&
    inDateRange(record.receivedDate || "", period.start, period.end)
  );
}

function waterPurifierEvaluationMetrics(month = currentDashboardMonth()) {
  // V10.70: 대시보드와 경영평가 모두 동일한 실제 CP- 영업접수행 목록을 사용합니다.
  // 월별 목표산정기간 내 CP- 제품 중 신규/패키지/재렌탈/일시불 영업접수행만 1행=1건으로 집계합니다.
  const period = monthPeriod(month);
  const sourceRecords = waterPurifierMonthRecords(month);
  const goals = calculatedGoals(month);
  const policy = managementEvaluationPolicy(month);
  const policyItem = policy.policyItems.find(item =>
    item?.id === "policy-water" || (item.kind === "rate" && String(item.title || "").includes("정수기"))
  ) || defaultManagementEvaluationPolicyItem("rate");
  const targetRate = toNumber(policyItem.targetRate) || 55;
  const goal = (toNumber(goals.newGoal) + toNumber(goals.rentalGoal)) * (targetRate / 100);
  const current = sourceRecords.length; // V10.70: 이미 CP- + 실제 영업종류만 필터된 목록
  const achievementRate = goal > 0 ? current / goal * 100 : 0;
  return { month, current, goal, targetRate, achievementRate, period };
}
function renderDashboardCustomCards(records) {
  const cards = dashboardCustomCards();
  const activeCount = cards.filter(card =>
    card.enabled && card.conditions?.some(c => String(c.value || "").trim())
  ).length;
  const customGrid = $("#customConditionChipGrid");
  if (customGrid) customGrid.classList.toggle("single-active-custom-card", activeCount === 1);
  cards.forEach((card, index) => {
    const labelNode = $(`#customCardLabel${index + 1}`);
    const valueNode = $(`#customCardValue${index + 1}`);
    if (!labelNode || !valueNode) return;
    const active = Boolean(card.enabled && card.conditions?.some(c => String(c.value || "").trim()));
    labelNode.textContent = active ? card.title : `조건${index + 1}`;
    if (active && String(card.title || "").trim() === "정수기" && card.field === "product") {
      // 정수기 선택카드는 경영평가와 동일하게 목표월의 목표산정기간 전체를 기준으로 집계합니다.
      // 대시보드의 임의 날짜/매니저/검색 필터 때문에 경영평가 수량과 달라지지 않도록 filtered records를 넘기지 않습니다.
      const water = waterPurifierEvaluationMetrics(currentDashboardMonth());
      valueNode.innerHTML =
        `<span class="water-card-count">${formatNumber(water.current)}</span><small class="water-card-rate">${formatNumber(Math.round(water.achievementRate * 10) / 10)}%</small>`;
      valueNode.classList.add("water-card-value");
    } else {
      valueNode.textContent = active ? formatNumber(customCardCount(records, card)) : "";
      valueNode.classList.remove("water-card-value");
    }
    const chip = valueNode.closest(".custom-condition-chip");
    if (chip) {
      if (active && String(card.title || "").trim() === "정수기") {
        chip.title = "목표산정기간 · 취소/멤버십 제외 · CP- 시작 · 신규/패키지/재렌탈/일시불 · 1행=1건";
      } else {
        chip.removeAttribute("title");
      }
      chip.classList.toggle("disabled", !active);
      chip.hidden = !active;
    }
  });
}

const CUSTOM_CARD_OPERATOR_LABELS = {
  startsWith: "시작함",
  contains: "포함함",
  equals: "같음"
};

function renderCustomDashboardCardSettings() {
  const box = $("#customDashboardCardSettings");
  if (!box) return;

  const cards = dashboardCustomCards();
  const fieldOptions = [
    ["product", "제품명"],
    ["category", "판매종류"]
  ];
  const operatorOptions = Object.entries(CUSTOM_CARD_OPERATOR_LABELS);

  box.innerHTML = cards.map((card, cardIndex) => {
    // 빈 조건은 절대로 화면에 렌더링하지 않는다.
    const conditions = Array.isArray(card.conditions)
      ? card.conditions.filter(c => String(c.value || "").trim())
      : [];

    return `
      <div class="custom-card-setting-row custom-card-setting-row-flat" data-custom-card-index="${cardIndex}">
        <div class="custom-card-setting-head">
          <label class="custom-card-enable">
            <input type="checkbox" class="custom-card-enabled" ${card.enabled ? "checked" : ""}>
            <span>사용</span>
          </label>
          <label class="custom-card-title-field">
            카드명
            <input class="custom-card-title" value="${escapeHtml(card.title)}" placeholder="예: 매트리스">
          </label>
        </div>

        <div class="custom-card-condition-builder">
          ${conditions.map((condition, conditionIndex) => `
            <div class="custom-condition-line ${conditionIndex === 0 ? "is-first" : "is-follow"}">
              ${conditionIndex === 0 ? `
                <select class="custom-condition-field" aria-label="조건 기준">
                  ${fieldOptions.map(([value, label]) =>
                    `<option value="${value}"${condition.field === value ? " selected" : ""}>${label}</option>`
                  ).join("")}
                </select>
                <span class="custom-condition-connector-placeholder"></span>
              ` : `
                <span class="custom-condition-field-spacer" aria-hidden="true"></span>
                <select class="custom-condition-connector" aria-label="조건 연결">
                  <option value="AND"${condition.connector === "AND" ? " selected" : ""}>AND</option>
                  <option value="OR"${condition.connector === "OR" ? " selected" : ""}>OR</option>
                </select>
              `}
              <select class="custom-condition-operator" aria-label="조건 방식">
                ${operatorOptions.map(([value, label]) =>
                  `<option value="${value}"${condition.operator === value ? " selected" : ""}>${label}</option>`
                ).join("")}
              </select>
              <input class="custom-condition-value" value="${escapeHtml(condition.value)}" placeholder="조건값">
              ${conditionIndex > 0
                ? `<button type="button" class="ghost-button small custom-remove-condition">삭제</button>`
                : `<span class="custom-remove-placeholder"></span>`}
            </div>
          `).join("")}

          <button type="button" class="ghost-button small custom-add-condition">
            + 조건 추가
          </button>
        </div>
      </div>
    `;
  }).join("");

  bindCustomDashboardCardEvents(box);
}

function bindCustomDashboardCardEvents(box) {
  if (!box || box.dataset.conditionEventsBound === "1") return;
  box.dataset.conditionEventsBound = "1";

  // 이벤트 위임은 이 컨테이너 하나에만 걸고,
  // 클릭한 버튼의 가장 가까운 카드만 수정한다.
  box.addEventListener("click", (event) => {
    const addButton = event.target.closest(".custom-add-condition");
    const removeButton = event.target.closest(".custom-remove-condition");

    if (addButton) {
      event.preventDefault();
      event.stopPropagation();
      const card = addButton.closest(".custom-card-setting-row");
      if (card) addCustomDashboardCondition(card);
      return;
    }

    if (removeButton) {
      event.preventDefault();
      event.stopPropagation();
      removeCustomDashboardCondition(removeButton);
    }
  });

  box.addEventListener("change", (event) => {
    const enabledInput = event.target.closest(".custom-card-enabled");
    if (enabledInput) {
      const card = enabledInput.closest(".custom-card-setting-row");
      if (card && !enabledInput.checked) {
        $$(".custom-condition-line", card).forEach(line => line.remove());
      }
      return;
    }

    const fieldSelect = event.target.closest(".custom-condition-field");
    if (fieldSelect) {
      const card = fieldSelect.closest(".custom-card-setting-row");
      if (!card) return;
      const field = fieldSelect.value === "category" ? "판매종류" : "제품명";
      $$(".custom-condition-field-spacer", card).forEach(spacer => {
        spacer.dataset.field = field;
      });
    }
  });
}

function addCustomDashboardCondition(card) {
  if (!card || !card.matches(".custom-card-setting-row")) return;

  const enabled = card.querySelector(".custom-card-enabled");
  if (enabled && !enabled.checked) enabled.checked = true;

  const builder = card.querySelector(".custom-card-condition-builder");
  if (!builder) return;

  const lines = Array.from(builder.querySelectorAll(":scope > .custom-condition-line"));

  // 빈 줄이 이미 있으면 새 줄을 만들지 않고 그 줄에 입력하도록 한다.
  // 이것으로 + 조건 추가를 여러 번 눌렀을 때 빈 줄이 계속 쌓이는 문제를 차단한다.
  const emptyLine = lines.find(line => !String(line.querySelector(".custom-condition-value")?.value || "").trim());
  if (emptyLine) {
    emptyLine.querySelector(".custom-condition-value")?.focus();
    return;
  }

  const field = lines[0]?.querySelector(".custom-condition-field")?.value || "product";

  if (lines.length === 0) {
    const button = builder.querySelector(".custom-add-condition");
    if (!button) return;

    button.insertAdjacentHTML("beforebegin", `
      <div class="custom-condition-line is-first">
        <select class="custom-condition-field" aria-label="조건 기준">
          <option value="product"${field === "product" ? " selected" : ""}>제품명</option>
          <option value="category"${field === "category" ? " selected" : ""}>판매종류</option>
        </select>
        <span class="custom-condition-connector-placeholder"></span>
        <select class="custom-condition-operator" aria-label="조건 방식">
          ${Object.entries(CUSTOM_CARD_OPERATOR_LABELS).map(([value, label]) =>
            `<option value="${value}">${label}</option>`).join("")}
        </select>
        <input class="custom-condition-value" placeholder="조건값" autofocus>
        <span class="custom-remove-placeholder"></span>
      </div>
    `);
    builder.querySelector(".custom-condition-value:last-of-type")?.focus();
    return;
  }

  const last = lines[lines.length - 1];
  last.insertAdjacentHTML("afterend", `
    <div class="custom-condition-line is-follow">
      <span class="custom-condition-field-spacer" aria-hidden="true"></span>
      <select class="custom-condition-connector" aria-label="조건 연결">
        <option value="AND">AND</option>
        <option value="OR" selected>OR</option>
      </select>
      <select class="custom-condition-operator" aria-label="조건 방식">
        ${Object.entries(CUSTOM_CARD_OPERATOR_LABELS).map(([value, label]) =>
          `<option value="${value}">${label}</option>`).join("")}
      </select>
      <input class="custom-condition-value" placeholder="조건값">
      <button type="button" class="ghost-button small custom-remove-condition">삭제</button>
    </div>
  `);

  const newLines = Array.from(builder.querySelectorAll(":scope > .custom-condition-line"));
  newLines[newLines.length - 1]?.querySelector(".custom-condition-value")?.focus();
}

function removeCustomDashboardCondition(button) {
  const line = button?.closest(".custom-condition-line");
  const card = button?.closest(".custom-card-setting-row");
  if (!line || !card) return;
  line.remove();

  // 첫 조건을 삭제했을 때 두 번째 줄이 첫 줄이 되도록 구조를 다시 정리한다.
  const lines = Array.from(card.querySelectorAll(":scope .custom-card-condition-builder > .custom-condition-line"));
  if (!lines.length) return;

  const first = lines[0];
  const oldConnector = first.querySelector(".custom-condition-connector");
  if (oldConnector) oldConnector.remove();

  if (!first.querySelector(".custom-condition-field")) {
    const currentField = card.querySelector(".custom-condition-field")?.value || "product";
    const spacer = first.querySelector(".custom-condition-field-spacer");
    if (spacer) {
      spacer.outerHTML = `
        <select class="custom-condition-field" aria-label="조건 기준">
          <option value="product"${currentField === "product" ? " selected" : ""}>제품명</option>
          <option value="category"${currentField === "category" ? " selected" : ""}>판매종류</option>
        </select>
      `;
    }
  }

  first.classList.remove("is-follow");
  first.classList.add("is-first");
  if (!first.querySelector(".custom-condition-connector-placeholder")) {
    first.insertAdjacentHTML("afterbegin", '<span class="custom-condition-connector-placeholder"></span>');
    const children = Array.from(first.children);
    // placeholder가 field 앞에 들어간 경우 순서를 바로잡는다.
    const fieldNode = first.querySelector(".custom-condition-field");
    const placeholder = first.querySelector(".custom-condition-connector-placeholder");
    if (fieldNode && placeholder && placeholder !== fieldNode.previousElementSibling) {
      first.insertBefore(fieldNode, first.firstChild);
      first.insertBefore(placeholder, fieldNode.nextSibling);
    }
  }
}

function collectCustomDashboardCards() {
  const rows = $$("#customDashboardCardSettings .custom-card-setting-row");

  state.dashboardCustomCards = rows.map((row, index) => {
    const enabled = Boolean(row.querySelector(".custom-card-enabled")?.checked);
    const title = String(row.querySelector(".custom-card-title")?.value || `조건${index + 1}`).trim();

    if (!enabled) {
      return normalizeDashboardCustomCard({
        title,
        enabled: false,
        conditions: []
      }, index);
    }

    const lines = Array.from(row.querySelectorAll(":scope .custom-card-condition-builder > .custom-condition-line"));
    const firstField = lines[0]?.querySelector(".custom-condition-field")?.value || "product";

    const conditions = lines.map((line, lineIndex) => ({
      field: firstField,
      operator: line.querySelector(".custom-condition-operator")?.value || "contains",
      value: String(line.querySelector(".custom-condition-value")?.value || "").trim(),
      connector: lineIndex === 0 ? "" : (line.querySelector(".custom-condition-connector")?.value || "OR")
    })).filter(condition => condition.value);

    return normalizeDashboardCustomCard({
      title,
      enabled: true,
      field: firstField,
      operator: conditions[0]?.operator || "contains",
      value: conditions[0]?.value || "",
      conditions
    }, index);
  });
}

function resetCustomDashboardCards() {
  state.dashboardCustomCards = defaultDashboardCustomCards();
  persistState();
  renderCustomDashboardCardSettings();
  renderDashboard();
  showToast("대시보드 조건카드를 기본값으로 되돌렸습니다.");
}


const CONTACT_NOTE_STATUSES = ["진행중", "재탈완료", "맴버완료", "보류", "소유", "타사", "거부"];

function normalizeContactAddress(value) {
  return String(value || "").replace(/부산광역시\s*/g, "").replace(/\s{2,}/g, " ").trim();
}

function formatContactAddressHtml(value) {
  const address = normalizeContactAddress(value);
  if (!address) return "";
  const safe = escapeHtml(address);
  // 괄호로 시작하는 상세주소는 새 줄에서 시작하고, 긴 주소는 CSS 자동 줄바꿈
  return safe.replace(/\s+\(/g, "<br>(");
}

function normalizeContactProgressRow(row = {}) {
  return {
    id: row.id || uid("cpr"),
    date: normalizeImportedDate(row.date || row.contactDate || "") || "",
    content: String(row.content || row.memo || row.note || "").trim()
  };
}

function normalizeContactNote(raw = {}) {
  return {
    id: raw.id || uid("contact"),
    status: CONTACT_NOTE_STATUSES.includes(raw.status) ? raw.status : "진행중",
    manager: String(raw.manager || "").trim(),
    customerNo: String(raw.customerNo || "").trim(),
    product: String(raw.product || "").trim(),
    customerName: String(raw.customerName || "").trim(),
    phone: formatPhoneNumber(raw.phone || ""),
    contactPhone: formatPhoneNumber(raw.contactPhone || ""),
    address: normalizeContactAddress(raw.address || ""),
    memo: String(raw.memo || "").trim(),
    progress: Array.isArray(raw.progress) ? raw.progress.map(normalizeContactProgressRow).filter(row => row.date || row.content) : [],
    createdAt: raw.createdAt || new Date().toISOString(),
    updatedAt: raw.updatedAt || new Date().toISOString(),
    source: raw.source || "manual"
  };
}

function contactNoteLatestProgress(note) {
  const rows = Array.isArray(note.progress) ? [...note.progress] : [];
  rows.sort((a, b) => String(b.date || "").localeCompare(String(a.date || "")));
  return rows[0] || null;
}

function contactStatusClass(status) {
  const map = { "진행중": "progress", "재탈완료": "done", "맴버완료": "done", "보류": "hold", "소유": "owned", "타사": "other", "거부": "reject" };
  return `contact-status-${map[status] || "progress"}`;
}

function renderContactNoteControls() {
  const statusInput = $("#contactNoteStatusInput");
  if (statusInput) setOptions(statusInput, CONTACT_NOTE_STATUSES.map(value => ({ value, label: value })), statusInput.value || "진행중");
  const statusFilter = $("#contactNoteStatusFilter");
  if (statusFilter) setOptions(statusFilter, [{ value: "", label: "전체 진행여부" }, ...CONTACT_NOTE_STATUSES.map(value => ({ value, label: value }))], statusFilter.value);
  const names = sortManagerNamesByDisplayOrder([...(state.managers || []).map((manager) => manager.name), ...(state.contactNotes || []).map((note) => note.manager)].filter(Boolean));
  const managerFilter = $("#contactNoteManagerFilter");
  if (managerFilter) setOptions(managerFilter, [{ value: "", label: "전체 매니저" }, ...names.map(value => ({ value, label: value }))], managerFilter.value);
  const datalist = $("#contactNoteManagerOptions");
  if (datalist) datalist.innerHTML = names.map(name => `<option value="${escapeHtml(name)}"></option>`).join("");
  const headerStatus = $("#contactHeaderStatusFilter");
  if (headerStatus) setOptions(headerStatus, [{ value: "", label: "진행여부" }, ...CONTACT_NOTE_STATUSES.map(value => ({ value, label: value }))], headerStatus.value);
  const headerManager = $("#contactHeaderManagerFilter");
  if (headerManager) setOptions(headerManager, [{ value: "", label: "매니저" }, ...names.map(value => ({ value, label: value }))], headerManager.value);
}

function addContactProgressRow(row = {}) {
  const container = $("#contactProgressRows");
  if (!container) return;
  const normalized = normalizeContactProgressRow(row);
  const div = document.createElement("div");
  div.className = "contact-progress-row";
  div.dataset.progressId = normalized.id;
  div.innerHTML = `<input class="contact-progress-date" type="date" value="${escapeHtml(normalized.date)}"><textarea class="contact-progress-content" rows="2" placeholder="컨텍 진행내용">${escapeHtml(normalized.content)}</textarea><button class="icon-button contact-progress-remove" type="button" title="행 삭제" onclick="removeContactProgressRow(this)">×</button>`;
  container.appendChild(div);
}

function collectContactProgressRows() {
  return $$("#contactProgressRows .contact-progress-row").map(row => normalizeContactProgressRow({
    id: row.dataset.progressId,
    date: row.querySelector(".contact-progress-date")?.value || "",
    content: row.querySelector(".contact-progress-content")?.value || ""
  })).filter(row => row.date || row.content);
}

function removeContactProgressRow(button) {
  const row = button?.closest?.(".contact-progress-row");
  if (row) row.remove();
  const container = $("#contactProgressRows");
  if (container && !container.children.length) addContactProgressRow({ date: todayIso(), content: "" });
}
window.removeContactProgressRow = removeContactProgressRow;

function resetContactNoteForm() {
  selectedContactNoteId = "";
  $("#contactNoteForm")?.reset();
  const idInput = $("#contactNoteIdInput"); if (idInput) idInput.value = "";
  const title = $("#contactNoteFormTitle"); if (title) title.textContent = "컨텍내용등록";
  const del = $("#deleteContactNoteBtn"); if (del) del.hidden = true;
  const rows = $("#contactProgressRows"); if (rows) rows.innerHTML = "";
  addContactProgressRow({ date: todayIso(), content: "" });
  renderContactNoteControls();
  const status = $("#contactNoteStatusInput"); if (status) status.value = "진행중";
}

function fillContactNoteForm(note) {
  if (!note) return;
  selectedContactNoteId = note.id;
  $("#contactNoteIdInput").value = note.id;
  $("#contactNoteFormTitle").textContent = "컨텍 수정";
  $("#contactNoteStatusInput").value = note.status;
  $("#contactNoteManagerInput").value = note.manager;
  $("#contactNoteCustomerNoInput").value = note.customerNo;
  $("#contactNoteProductInput").value = note.product;
  $("#contactNoteCustomerNameInput").value = note.customerName;
  $("#contactNotePhoneInput").value = note.phone;
  $("#contactNoteContactPhoneInput").value = note.contactPhone;
  $("#contactNoteAddressInput").value = note.address;
  $("#contactNoteMemoInput").value = note.memo;
  $("#contactProgressRows").innerHTML = "";
  (note.progress.length ? note.progress : [{ date: todayIso(), content: "" }]).forEach(addContactProgressRow);
  $("#deleteContactNoteBtn").hidden = false;
  $("#contactNoteForm")?.scrollIntoView({ behavior: "smooth", block: "start" });
}

function saveContactNoteFromForm(event) {
  event.preventDefault();
  const existing = selectedContactNoteId ? state.contactNotes.find(item => item.id === selectedContactNoteId) : null;
  const note = normalizeContactNote({
    ...(existing || {}),
    id: existing?.id || uid("contact"),
    status: $("#contactNoteStatusInput").value,
    manager: $("#contactNoteManagerInput").value,
    customerNo: $("#contactNoteCustomerNoInput").value,
    product: $("#contactNoteProductInput").value,
    customerName: $("#contactNoteCustomerNameInput").value,
    phone: $("#contactNotePhoneInput").value,
    contactPhone: $("#contactNoteContactPhoneInput").value,
    address: $("#contactNoteAddressInput").value,
    memo: $("#contactNoteMemoInput").value,
    progress: collectContactProgressRows(),
    createdAt: existing?.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    source: existing?.source || "manual"
  });
  if (!note.customerName && !note.customerNo && !note.phone) {
    showToast("고객명, 고객번호 또는 연락처 중 하나는 입력해주세요."); return;
  }
  if (existing) Object.assign(existing, note); else state.contactNotes.unshift(note);
  persistState();
  resetContactNoteForm();
  renderContactNotes();
  showToast(existing ? "컨텍노트를 수정했습니다." : "컨텍노트를 등록했습니다.");
}

function deleteContactNote() {
  if (!selectedContactNoteId) return;
  const note = state.contactNotes.find(item => item.id === selectedContactNoteId);
  if (!note || !window.confirm(`'${note.customerName || note.customerNo || "고객"}' 컨텍노트를 삭제할까요?`)) return;
  state.contactNotes = state.contactNotes.filter(item => item.id !== selectedContactNoteId);
  persistState(); resetContactNoteForm(); renderContactNotes(); showToast("컨텍노트를 삭제했습니다.");
}

function renderContactNotes() {
  const body = $("#contactNoteTableBody"); if (!body) return;
  renderContactNoteControls();
  const status = $("#contactHeaderStatusFilter")?.value || "";
  const manager = $("#contactHeaderManagerFilter")?.value || "";
  const rows = (state.contactNotes || [])
    .filter(note => !status || note.status === status)
    .filter(note => !manager || note.manager === manager)
    .sort((a,b)=>String(b.updatedAt).localeCompare(String(a.updatedAt)));
  const count = $("#contactNoteCountLabel"); if (count) count.textContent = `${rows.length}건`;

  body.innerHTML = rows.length ? rows.map(note => {
    const history = (note.progress || []).slice().sort((a,b)=>String(b.date).localeCompare(String(a.date)));
    const historyBadge = history.length
      ? `<button class="contact-history-toggle" type="button" data-contact-history-toggle="${escapeHtml(note.id)}">이력 ${history.length}건 ▾</button>`
      : '<span class="contact-history-empty">기록 없음</span>';
    const statusOptions = CONTACT_NOTE_STATUSES.map(value => `<option value="${escapeHtml(value)}"${note.status === value ? " selected" : ""}>${escapeHtml(value)}</option>`).join("");
    return `<tr class="contact-note-main-row contact-note-card-row" data-contact-note-id="${escapeHtml(note.id)}">
      <td><select class="contact-status-inline ${contactStatusClass(note.status)}" data-contact-status-id="${escapeHtml(note.id)}" aria-label="진행여부 변경">${statusOptions}</select></td>
      <td><strong class="contact-manager-only">${escapeHtml(note.manager)}</strong></td>
      <td><div class="contact-customer-no-combined"><strong>${escapeHtml(note.customerName)}</strong>${note.phone ? `<span>${escapeHtml(note.phone)}</span>` : ""}${note.customerNo ? `<small>${escapeHtml(note.customerNo)}</small>` : ""}</div></td>
      <td><div class="contact-product-address"><strong>${escapeHtml(note.product)}</strong><span>${formatContactAddressHtml(note.address)}</span></div></td>
      <td><div class="contact-main-content-grid">
        <div class="contact-inline-phone-field"><input class="contact-inline-phone" data-contact-phone-id="${escapeHtml(note.id)}" value="${escapeHtml(note.contactPhone)}" placeholder="컨텍연락처"></div>
        <div class="contact-inline-memo"><textarea class="contact-inline-memo-input" data-contact-memo-id="${escapeHtml(note.id)}" rows="2" placeholder="특이사항">${escapeHtml(note.memo)}</textarea></div>
        <div class="contact-inline-progress-compact"><div class="contact-inline-progress-editor"><input class="contact-inline-progress-date" data-contact-progress-date-id="${escapeHtml(note.id)}" type="date" value="${todayIso()}"><textarea class="contact-inline-progress-content" data-contact-progress-content-id="${escapeHtml(note.id)}" rows="1" placeholder="컨텍내용 입력"></textarea><button class="primary-button small contact-inline-progress-save" type="button" data-contact-progress-save="${escapeHtml(note.id)}">기록</button></div>${historyBadge}</div>
      </div></td>
    </tr>
    <tr class="contact-note-detail-row" data-contact-note-detail="${escapeHtml(note.id)}" hidden><td class="contact-detail-spacer"></td><td colspan="4"><div class="contact-note-detail-box"><div class="contact-detail-actions"><button class="ghost-button small" type="button" data-contact-edit="${escapeHtml(note.id)}">수정</button></div><div class="contact-history-list">${history.length ? history.map(row => `<div><time>${escapeHtml(row.date || "-")}</time><p>${escapeHtml(row.content || "")}</p></div>`).join("") : '<span class="muted">등록된 컨텍내용이 없습니다.</span>'}</div></div></td></tr>`;
  }).join("") : '<tr><td colspan="5" class="empty">등록된 만기컨텐 고객이 없습니다.</td></tr>';
}


function contactRequestProgressRowsFromForm() {
  return Array.from(document.querySelectorAll("#contactRequestProgressRows .contact-progress-row")).map(row => normalizeContactProgressRow({
    id: row.dataset.progressId || uid("cpr"),
    date: row.querySelector(".contact-progress-date")?.value || "",
    content: row.querySelector(".contact-progress-content")?.value || ""
  })).filter(row => row.date || row.content);
}

function addContactRequestProgressRow(row = {}) {
  const wrap = $("#contactRequestProgressRows"); if (!wrap) return;
  const item = normalizeContactProgressRow(row);
  const div = document.createElement("div");
  div.className = "contact-progress-row";
  div.dataset.progressId = item.id;
  div.innerHTML = `<input class="contact-progress-date" type="date" value="${escapeHtml(item.date || todayIso())}"><textarea class="contact-progress-content" rows="2" placeholder="컨텍내용">${escapeHtml(item.content)}</textarea><button class="icon-button contact-request-progress-remove" type="button">×</button>`;
  wrap.appendChild(div);
}

function resetContactRequestForm() {
  selectedContactRequestId = "";
  $("#contactRequestIdInput").value = "";
  ["contactRequestManagerInput","contactRequestCustomerNoInput","contactRequestProductInput","contactRequestCustomerNameInput","contactRequestPhoneInput","contactRequestContactPhoneInput","contactRequestAddressInput","contactRequestMemoInput"].forEach(id => { const node=$("#"+id); if(node) node.value=""; });
  const status=$("#contactRequestStatusInput"); if(status) status.value="진행중";
  const wrap=$("#contactRequestProgressRows"); if(wrap) wrap.innerHTML="";
  addContactRequestProgressRow({date:todayIso(),content:""});
  const del=$("#deleteContactRequestBtn"); if(del) del.hidden=true;
}

function saveContactRequestFromForm(event) {
  event.preventDefault();
  const existing = (state.contactRequests || []).find(item => item.id === selectedContactRequestId);
  const note = normalizeContactNote({
    id: existing?.id || uid("creq"), status: $("#contactRequestStatusInput").value,
    manager: $("#contactRequestManagerInput").value, customerNo: $("#contactRequestCustomerNoInput").value,
    product: $("#contactRequestProductInput").value, customerName: $("#contactRequestCustomerNameInput").value,
    phone: $("#contactRequestPhoneInput").value, contactPhone: $("#contactRequestContactPhoneInput").value,
    address: $("#contactRequestAddressInput").value, memo: $("#contactRequestMemoInput").value,
    progress: contactRequestProgressRowsFromForm(), createdAt: existing?.createdAt || new Date().toISOString(), updatedAt: new Date().toISOString(), source:"manual"
  });
  if (!note.customerName && !note.customerNo && !note.phone) { showToast("고객명, 고객번호 또는 연락처 중 하나는 입력해주세요."); return; }
  if (!Array.isArray(state.contactRequests)) state.contactRequests=[];
  if (existing) Object.assign(existing,note); else state.contactRequests.unshift(note);
  persistState(); resetContactRequestForm(); renderContactRequests(); showToast(existing ? "컨텍요청을 수정했습니다." : "컨텍요청을 등록했습니다.");
}

function fillContactRequestForm(note) {
  if (!note) return; selectedContactRequestId=note.id;
  $("#contactRequestIdInput").value=note.id; $("#contactRequestStatusInput").value=note.status; $("#contactRequestManagerInput").value=note.manager;
  $("#contactRequestCustomerNoInput").value=note.customerNo; $("#contactRequestProductInput").value=note.product; $("#contactRequestCustomerNameInput").value=note.customerName;
  $("#contactRequestPhoneInput").value=note.phone; $("#contactRequestContactPhoneInput").value=note.contactPhone; $("#contactRequestAddressInput").value=note.address; $("#contactRequestMemoInput").value=note.memo;
  const wrap=$("#contactRequestProgressRows"); if(wrap) wrap.innerHTML=""; (note.progress||[]).forEach(addContactRequestProgressRow); if(!(note.progress||[]).length) addContactRequestProgressRow({date:todayIso(),content:""});
  const del=$("#deleteContactRequestBtn"); if(del) del.hidden=false; $("#contactRequestForm")?.scrollIntoView({behavior:"smooth",block:"start"});
}

function deleteContactRequest() {
  if(!selectedContactRequestId) return; const note=(state.contactRequests||[]).find(x=>x.id===selectedContactRequestId); if(!note) return;
  if(!window.confirm(`'${note.customerName || note.customerNo || "고객"}' 컨텍요청을 삭제할까요?`)) return;
  state.contactRequests=(state.contactRequests||[]).filter(x=>x.id!==selectedContactRequestId); persistState(); resetContactRequestForm(); renderContactRequests(); showToast("컨텍요청을 삭제했습니다.");
}

function renderContactRequests() {
  const list=$("#contactRequestList"); if(!list) return;
  const managers=allManagerNames(); setOptions($("#contactRequestStatusInput"), CONTACT_NOTE_STATUSES.map(v=>({value:v,label:v})), $("#contactRequestStatusInput")?.value || "진행중");
  const dl=$("#contactRequestManagerOptions"); if(dl) dl.innerHTML=managers.map(name=>`<option value="${escapeHtml(name)}"></option>`).join("");
  const rows=(state.contactRequests||[]).slice().sort((a,b)=>String(b.updatedAt).localeCompare(String(a.updatedAt)));
  const count=$("#contactRequestCountLabel"); if(count) count.textContent=`${rows.length}건`;
  list.innerHTML=rows.length?rows.map(note=>{const history=(note.progress||[]).slice().sort((a,b)=>String(b.date).localeCompare(String(a.date)));return `<article class="contact-request-card" data-contact-request-id="${escapeHtml(note.id)}"><div class="contact-request-summary"><span class="contact-status-pill ${contactStatusClass(note.status)}">${escapeHtml(note.status)}</span><strong>${escapeHtml(note.customerName || note.customerNo)}</strong><span>${escapeHtml(note.manager)}</span><span>${escapeHtml(note.product)}</span><span>${escapeHtml(note.phone)}</span><button class="ghost-button small" type="button" data-contact-request-edit="${escapeHtml(note.id)}">수정</button></div><div class="contact-request-sub"><span>${escapeHtml(note.address)}</span><span>${escapeHtml(note.memo)}</span>${history.length?`<button class="contact-history-toggle" type="button" data-contact-request-history="${escapeHtml(note.id)}">이력 ${history.length}건 ▾</button>`:'<span class="muted">기록 없음</span>'}</div><div class="contact-request-history" data-contact-request-history-box="${escapeHtml(note.id)}" hidden>${history.map(row=>`<div><time>${escapeHtml(row.date||"-")}</time><span>${escapeHtml(row.content||"")}</span></div>`).join("")}</div></article>`}).join(""):'<div class="empty">등록된 컨텍요청이 없습니다.</div>';
}

function saveInlineContactPhone(noteId, value) {
  const note = (state.contactNotes || []).find(item => item.id === noteId);
  if (!note) return;
  note.contactPhone = String(value || "").trim();
  note.updatedAt = new Date().toISOString();
  persistState();
}

function saveInlineContactMemo(id, value) {
  const note = (state.contactNotes || []).find(item => item.id === id);
  if (!note) return;
  const next = String(value || "").trim();
  if (note.memo === next) return;
  note.memo = next;
  note.updatedAt = new Date().toISOString();
  persistState();
  showToast("특이사항을 저장했습니다.");
}

function addInlineContactProgress(noteId) {
  const note = (state.contactNotes || []).find(item => item.id === noteId);
  if (!note) return;
  const dateInput = document.querySelector(`[data-contact-progress-date-id="${CSS.escape(noteId)}"]`);
  const contentInput = document.querySelector(`[data-contact-progress-content-id="${CSS.escape(noteId)}"]`);
  const date = String(dateInput?.value || todayIso()).trim();
  const content = String(contentInput?.value || "").trim();
  if (!content) { showToast("컨텍내용을 입력해주세요."); contentInput?.focus(); return; }
  note.progress = Array.isArray(note.progress) ? note.progress : [];
  note.progress.push(normalizeContactProgressRow({ date, content }));
  note.updatedAt = new Date().toISOString();
  persistState();
  renderContactNotes();
  showToast("컨텍내용을 기록했습니다.");
}

function saveInlineContactStatus(id, status) {
  const note = (state.contactNotes || []).find(item => item.id === id);
  if (!note || !CONTACT_NOTE_STATUSES.includes(status)) return;
  note.status = status;
  note.updatedAt = new Date().toISOString();
  persistState();
  renderContactNotes();
  showToast(`진행여부를 '${status}'로 변경했습니다.`);
}

function excelColumnSortIndex(name) {
  return String(name || "").toUpperCase().split("").reduce((sum, char) => sum * 26 + (char.charCodeAt(0) - 64), 0);
}

function excelColumnName(index) {
  let value = Number(index);
  let name = "";
  while (value > 0) {
    value -= 1;
    name = String.fromCharCode(65 + (value % 26)) + name;
    value = Math.floor(value / 26);
  }
  return name;
}

function worksheetRowsToMatrix(rows = []) {
  const maxColumn = rows.reduce((max, row) => {
    const rowMax = Object.keys(row || {}).reduce((innerMax, key) => Math.max(innerMax, excelColumnSortIndex(key)), 0);
    return Math.max(max, rowMax);
  }, 0);
  if (!maxColumn) return [];
  return rows.map((row) => Array.from({ length: maxColumn }, (_, index) => row?.[excelColumnName(index + 1)] ?? ""));
}

function contactNoteHeaderValue(row, header, labels) {
  const normalizedLabels = labels.map(normalizeHeaderKey);
  const index = header.findIndex(item => normalizedLabels.includes(normalizeHeaderKey(item)));
  return index >= 0 ? row[index] : "";
}

function contactNoteFromArrayRow(row, header) {
  const value = (...labels) => contactNoteHeaderValue(row, header, labels);
  const customerName = String(value("계약자명","고객명","성명") || "").trim();
  const customerNo = String(value("고객번호","계약번호","회원번호") || "").trim();
  const phone = String(value("연락처","휴대폰","휴대전화","전화번호") || "").trim();
  if (!customerName && !customerNo && !phone) return null;
  const progressDate = normalizeImportedDate(value("컨텍날짜","컨텍일자","연락일","접촉일","상담일"));
  const progressContent = String(value("컨텍내용등록","컨텍내용","컨텍진행내용","진행내용","연락내용","상담내용") || "").trim();
  return normalizeContactNote({
    status: String(value("진행여부","진행상태","상태") || "진행중").trim(),
    manager: String(value("기사정보","매니저","담당매니저","담당자") || "").trim(),
    customerNo,
    product: String(value("상품명","제품명","모델명","상품") || "").trim(),
    customerName,
    phone,
    contactPhone: String(value("컨텍연락처","컨택연락처","연락가능번호") || "").trim(),
    address: String(value("주소","설치주소","고객주소") || "").trim(),
    memo: String(value("특이사항","메모","비고") || "").trim(),
    progress: (progressDate || progressContent) ? [{ date: progressDate, content: progressContent }] : [],
    source: "excel"
  });
}

function findContactNoteHeaderIndex(rows) {
  return rows.findIndex(row => {
    const keys = row.map(normalizeHeaderKey);
    const joined = keys.join("|");
    return joined.includes("계약자명") || joined.includes("고객명") || joined.includes("고객번호") || joined.includes("기사정보");
  });
}

async function importContactNoteFile(file) {
  if (!file) return;
  try {
    let imported = [];
    if (isCsvFile(file)) {
      const text = await readDelimitedTextFile(file);
      const rows = parseCsvText(text);
      const headerIndex = findContactNoteHeaderIndex(rows);
      if (headerIndex >= 0) imported = rows.slice(headerIndex + 1).map(row => contactNoteFromArrayRow(row, rows[headerIndex])).filter(Boolean);
    } else {
      const workbook = await readXlsx(file);
      workbook.sheets.forEach(sheet => {
        // 빈 셀이 있는 엑셀도 열 위치가 밀리지 않도록 A열부터 마지막 열까지 빈칸을 유지합니다.
        const matrix = worksheetRowsToMatrix(sheet.rows || []);
        const headerIndex = findContactNoteHeaderIndex(matrix);
        if (headerIndex >= 0) imported.push(...matrix.slice(headerIndex + 1).map(row => contactNoteFromArrayRow(row, matrix[headerIndex])).filter(Boolean));
      });
    }
    if (!imported.length) { showToast("컨텍노트로 가져올 고객 데이터를 찾지 못했습니다."); return; }
    const key = note => [note.customerNo,note.customerName,note.phone,note.product].join("|");
    const existing = new Map((state.contactNotes || []).map(note => [key(note), note]));
    let added = 0, updated = 0;
    imported.forEach(note => {
      const k = key(note); const old = existing.get(k);
      if (old) {
        const progressMap = new Map([...(old.progress || []), ...(note.progress || [])].map((row) => [`${row.date || ""}|${row.content || ""}`, row]));
        const combinedProgress = [...progressMap.values()];
        Object.assign(old, { ...note, id: old.id, createdAt: old.createdAt, progress: combinedProgress, updatedAt: new Date().toISOString() });
        updated += 1;
      } else { state.contactNotes.push(note); existing.set(k,note); added += 1; }
    });
    persistState(); renderContactNotes(); showToast(`컨텍노트 ${added}건 추가, ${updated}건 업데이트했습니다.`);
  } catch (error) {
    console.error(error); showToast(`컨텍노트 파일을 읽지 못했습니다: ${error.message || "알 수 없는 오류"}`);
  }
}

const CHECKLIST_STATUSES = ["시작", "진행중", "보류", "이월", "완료"];
const CHECKLIST_PRIORITIES = ["긴급", "높음", "보통", "낮음"];

function normalizeChecklistItem(item = {}) {
  const month = /^\d{4}-\d{2}$/.test(String(item.month || "")) ? item.month : String(item.date || todayIso()).slice(0, 7);
  const status = CHECKLIST_STATUSES.includes(item.status) ? item.status : "시작";
  return {
    id: item.id || uid("task"),
    title: String(item.title || "").trim(),
    memo: String(item.memo || "").trim(),
    date: /^\d{4}-\d{2}-\d{2}$/.test(String(item.date || "")) ? item.date : `${month}-01`,
    month,
    status,
    priority: CHECKLIST_PRIORITIES.includes(item.priority) ? item.priority : "보통",
    appointmentTime: /^([01]\d|2[0-3]):[0-5]\d$/.test(String(item.appointmentTime || "")) ? String(item.appointmentTime) : "",
    remindBefore: Number.isFinite(Number(item.remindBefore)) ? Number(item.remindBefore) : -1,
    lastAlarmKey: String(item.lastAlarmKey || ""),
    rolloverMonth: /^\d{4}-\d{2}$/.test(String(item.rolloverMonth || "")) ? item.rolloverMonth : "",
    createdAt: item.createdAt || new Date().toISOString(),
    updatedAt: item.updatedAt || item.createdAt || new Date().toISOString()
  };
}

function checklistItemsForMonth(month = checklistMonth) {
  return (state.checklistItems || [])
    .map(normalizeChecklistItem)
    .filter((item) => {
      if (item.month === month || item.rolloverMonth === month) return true;
      // 이전 달 미완료 업무는 완료 처리될 때까지 다음 달에도 계속 표시합니다.
      return item.month < month && item.status !== "완료";
    });
}

function checklistStatusClass(status) {
  return {
    "시작": "status-start",
    "진행중": "status-progress",
    "보류": "status-hold",
    "이월": "status-rollover",
    "완료": "status-done"
  }[status] || "status-start";
}

function checklistPriorityClass(priority) {
  return {
    "긴급": "priority-urgent",
    "높음": "priority-high",
    "보통": "priority-normal",
    "낮음": "priority-low"
  }[priority] || "priority-normal";
}

function checklistReminderLabel(minutes) {
  const value = Number(minutes);
  if (value < 0 || !Number.isFinite(value)) return "알림 없음";
  if (value === 0) return "정시 알림";
  if (value === 60) return "1시간 전 알림";
  return `${value}분 전 알림`;
}

function checklistTimeLabel(value) {
  const match = String(value || "").match(/^(\d{2}):(\d{2})$/);
  if (!match) return "";
  const hour = Number(match[1]);
  const minute = match[2];
  const period = hour < 12 ? "오전" : "오후";
  const displayHour = hour % 12 || 12;
  return `${period} ${displayHour}:${minute}`;
}

function checklistAlarmKey(item) {
  if (!item?.appointmentTime || Number(item.remindBefore) < 0) return "";
  return `${item.date}T${item.appointmentTime}|${Number(item.remindBefore)}`;
}

function setChecklistReminderEnabled() {
  const timeInput = $("#checklistTimeInput");
  const reminderInput = $("#checklistReminderInput");
  if (!reminderInput) return;
  const enabled = Boolean(timeInput?.value);
  reminderInput.disabled = !enabled;
  if (!enabled) reminderInput.value = "-1";
}

function checklistModalItems() {
  const items = checklistItemsForMonth(checklistMonth);
  const filtered = checklistModalStatus === "전체" ? items : items.filter((item) => item.status === checklistModalStatus);
  const priorityOrder = { "긴급": 0, "높음": 1, "보통": 2, "낮음": 3 };
  return filtered.sort((a, b) => {
    const doneCompare = Number(a.status === "완료") - Number(b.status === "완료");
    if (doneCompare) return doneCompare;
    const dateCompare = `${a.date} ${a.appointmentTime || ""}`.localeCompare(`${b.date} ${b.appointmentTime || ""}`);
    if (dateCompare) return dateCompare;
    return (priorityOrder[a.priority] || 9) - (priorityOrder[b.priority] || 9);
  });
}

function renderChecklistStatusModal() {
  const modal = $("#checklistStatusModal");
  const list = $("#checklistStatusModalList");
  if (!modal || !list || modal.hidden) return;
  const items = checklistModalItems();
  const title = $("#checklistStatusModalTitle");
  if (title) title.textContent = `${formatMonthLabel(checklistMonth)} · ${checklistModalStatus} ${items.length}건`;
  list.innerHTML = items.length ? items.map((item) => {
    const timeText = item.appointmentTime ? `${checklistTimeLabel(item.appointmentTime)} · ${checklistReminderLabel(item.remindBefore)}` : "시간 미지정";
    return `<article class="checklist-popup-item ${checklistStatusClass(item.status)} ${item.status === "완료" ? "is-done" : ""}">
      <div class="checklist-popup-item-main">
        <span class="checklist-popup-item-date">${escapeHtml(item.date)} · ${escapeHtml(timeText)}</span>
        <strong>${escapeHtml(item.title)}</strong>
        ${item.memo ? `<p>${escapeHtml(item.memo).replace(/\n/g, "<br>")}</p>` : ""}
      </div>
      <span class="checklist-priority ${checklistPriorityClass(item.priority)}">${escapeHtml(item.priority)}</span>
      <select class="checklist-status-select ${checklistStatusClass(item.status)}" data-checklist-popup-status-id="${escapeHtml(item.id)}">${CHECKLIST_STATUSES.map((status) => `<option value="${status}"${status === item.status ? " selected" : ""}>${status}</option>`).join("")}</select>
      ${item.status !== "완료" ? `<button class="primary-button small" type="button" data-checklist-popup-complete="${escapeHtml(item.id)}">완료</button>` : '<span class="checklist-popup-done-label">완료됨</span>'}
    </article>`;
  }).join("") : `<div class="checklist-popup-empty">${escapeHtml(checklistModalStatus)} 상태의 업무가 없습니다.</div>`;
}

function openChecklistStatusModal(status = "전체") {
  checklistModalStatus = CHECKLIST_STATUSES.includes(status) ? status : "전체";
  const modal = $("#checklistStatusModal");
  if (!modal) return;
  modal.hidden = false;
  document.body.classList.add("checklist-popup-open");
  renderChecklistStatusModal();
}

function closeChecklistStatusModal() {
  const modal = $("#checklistStatusModal");
  if (modal) modal.hidden = true;
  document.body.classList.remove("checklist-popup-open");
}

function openChecklistAlarmModal(item) {
  if (!item) return;
  activeChecklistAlarmId = item.id;
  const modal = $("#checklistAlarmModal");
  if (!modal) return;
  const title = $("#checklistAlarmTitle");
  const time = $("#checklistAlarmTime");
  const memo = $("#checklistAlarmMemo");
  if (title) title.textContent = item.title || "업무 알림";
  if (time) time.textContent = `${item.date} ${checklistTimeLabel(item.appointmentTime)} · ${checklistReminderLabel(item.remindBefore)}`;
  if (memo) {
    memo.innerHTML = item.memo ? escapeHtml(item.memo).replace(/\n/g, "<br>") : "등록된 메모가 없습니다.";
    memo.classList.toggle("is-empty", !item.memo);
  }
  modal.hidden = false;
  document.body.classList.add("checklist-popup-open");
  try {
    if ("Notification" in window && Notification.permission === "granted") {
      new Notification(`업무 알림 · ${item.title}`, { body: `${item.date} ${checklistTimeLabel(item.appointmentTime)}` });
    }
  } catch (error) {
    console.debug("desktop notification unavailable", error);
  }
}

function showNextChecklistAlarm() {
  const modal = $("#checklistAlarmModal");
  if (modal && !modal.hidden) return;
  while (checklistAlarmQueue.length) {
    const id = checklistAlarmQueue.shift();
    const item = state.checklistItems.find((entry) => entry.id === id);
    if (item && item.status !== "완료") {
      openChecklistAlarmModal(item);
      return;
    }
  }
}

function closeChecklistAlarmModal() {
  const modal = $("#checklistAlarmModal");
  if (modal) modal.hidden = true;
  activeChecklistAlarmId = "";
  document.body.classList.remove("checklist-popup-open");
  window.setTimeout(showNextChecklistAlarm, 120);
}

function completeChecklistAlarmItem() {
  if (activeChecklistAlarmId) updateChecklistStatus(activeChecklistAlarmId, "완료");
  closeChecklistAlarmModal();
}

function checkChecklistAlarms() {
  const now = new Date();
  let changed = false;
  const queued = new Set(checklistAlarmQueue);
  (state.checklistItems || []).forEach((raw) => {
    const item = normalizeChecklistItem(raw);
    if (item.status === "완료" || !item.appointmentTime || Number(item.remindBefore) < 0) return;
    const appointmentAt = new Date(`${item.date}T${item.appointmentTime}:00`);
    if (Number.isNaN(appointmentAt.getTime())) return;
    const alarmAt = new Date(appointmentAt.getTime() - Number(item.remindBefore) * 60000);
    const alarmKey = checklistAlarmKey(item);
    const withinWindow = now.getTime() >= alarmAt.getTime() && now.getTime() <= appointmentAt.getTime() + 2 * 60 * 60 * 1000;
    if (!withinWindow || item.lastAlarmKey === alarmKey) return;
    const target = state.checklistItems.find((entry) => entry.id === item.id);
    if (target) target.lastAlarmKey = alarmKey;
    if (!queued.has(item.id) && activeChecklistAlarmId !== item.id) {
      checklistAlarmQueue.push(item.id);
      queued.add(item.id);
    }
    changed = true;
  });
  if (changed) persistState();
  showNextChecklistAlarm();
}

function startChecklistAlarmWatcher() {
  if (checklistAlarmTimer) window.clearInterval(checklistAlarmTimer);
  checkChecklistAlarms();
  checklistAlarmTimer = window.setInterval(checkChecklistAlarms, 30000);
}

function resetChecklistForm() {
  selectedChecklistId = "";
  const form = $("#checklistForm");
  if (form) form.reset();
  $("#checklistIdInput").value = "";
  $("#checklistFormTitle").textContent = "새 업무 등록";
  $("#checklistDateInput").value = checklistMonth === monthIso() ? todayIso() : `${checklistMonth}-01`;
  $("#checklistStatusInput").value = "시작";
  $("#checklistPriorityInput").value = "보통";
  $("#checklistTimeInput").value = "";
  $("#checklistReminderInput").value = "-1";
  setChecklistReminderEnabled();
  $("#deleteChecklistBtn").hidden = true;
}

function fillChecklistForm(item) {
  if (!item) return;
  selectedChecklistId = item.id;
  $("#checklistIdInput").value = item.id;
  $("#checklistTitleInput").value = item.title || "";
  $("#checklistDateInput").value = item.date || "";
  $("#checklistStatusInput").value = item.status || "시작";
  $("#checklistPriorityInput").value = item.priority || "보통";
  $("#checklistTimeInput").value = item.appointmentTime || "";
  $("#checklistReminderInput").value = String(Number.isFinite(Number(item.remindBefore)) ? Number(item.remindBefore) : -1);
  setChecklistReminderEnabled();
  $("#checklistMemoInput").value = item.memo || "";
  $("#checklistFormTitle").textContent = "업무 수정";
  $("#deleteChecklistBtn").hidden = false;
  $("#checklistTitleInput").focus();
}

function saveChecklistFromForm(event) {
  event?.preventDefault();
  const title = $("#checklistTitleInput").value.trim();
  if (!title) {
    showToast("업무 제목을 입력해주세요.");
    $("#checklistTitleInput").focus();
    return;
  }
  const date = $("#checklistDateInput").value || `${checklistMonth}-01`;
  const status = $("#checklistStatusInput").value;
  const appointmentTime = $("#checklistTimeInput").value || "";
  const remindBefore = appointmentTime ? Number($("#checklistReminderInput").value ?? -1) : -1;
  const existing = selectedChecklistId ? state.checklistItems.find((item) => item.id === selectedChecklistId) : null;
  const baseMonth = existing?.month || date.slice(0, 7) || checklistMonth;
  const alarmChanged = !existing || existing.date !== date || existing.appointmentTime !== appointmentTime || Number(existing.remindBefore) !== remindBefore;
  const next = normalizeChecklistItem({
    ...(existing || {}),
    id: existing?.id || uid("task"),
    title,
    memo: $("#checklistMemoInput").value.trim(),
    date,
    month: baseMonth,
    status,
    priority: $("#checklistPriorityInput").value,
    appointmentTime,
    remindBefore,
    lastAlarmKey: alarmChanged ? "" : (existing?.lastAlarmKey || ""),
    rolloverMonth: status === "이월" ? shiftMonth(baseMonth, 1) : "",
    createdAt: existing?.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString()
  });
  if (existing) Object.assign(existing, next);
  else state.checklistItems.push(next);
  persistState();
  resetChecklistForm();
  renderChecklist();
  if (next.appointmentTime && Number(next.remindBefore) >= 0) {
    try {
      if ("Notification" in window && Notification.permission === "default") Notification.requestPermission();
    } catch (error) {
      console.debug("notification permission request unavailable", error);
    }
    checkChecklistAlarms();
  }
  showToast(status === "이월" ? `${formatMonthLabel(next.rolloverMonth)}로 이월했습니다.` : "업무를 저장했습니다.");
}

function deleteChecklistItem() {
  if (!selectedChecklistId) return;
  const item = state.checklistItems.find((entry) => entry.id === selectedChecklistId);
  if (!item) return;
  if (!window.confirm(`'${item.title}' 업무를 삭제할까요?`)) return;
  state.checklistItems = state.checklistItems.filter((entry) => entry.id !== selectedChecklistId);
  persistState();
  resetChecklistForm();
  renderChecklist();
  showToast("업무를 삭제했습니다.");
}

function updateChecklistStatus(id, status) {
  const item = state.checklistItems.find((entry) => entry.id === id);
  if (!item || !CHECKLIST_STATUSES.includes(status)) return;
  item.status = status;
  item.rolloverMonth = status === "이월" ? shiftMonth(item.month, 1) : "";
  item.updatedAt = new Date().toISOString();
  persistState();
  renderChecklist();
  renderChecklistStatusModal();
  if (status === "이월") showToast(`${formatMonthLabel(item.rolloverMonth)}에 자동 노출됩니다.`);
}

function renderChecklist() {
  const list = $("#checklistList");
  if (!list) return;
  const monthInput = $("#checklistMonthInput");
  if (monthInput) monthInput.value = checklistMonth;
  const search = String($("#checklistSearchInput")?.value || "").trim().toLowerCase();
  const statusFilter = $("#checklistStatusFilter")?.value || "active";
  const priorityFilter = $("#checklistPriorityFilter")?.value || "";
  const monthItems = checklistItemsForMonth(checklistMonth);
  const counts = Object.fromEntries(CHECKLIST_STATUSES.map((status) => [status, monthItems.filter((item) => item.status === status).length]));
  $("#checklistTotalCount").textContent = monthItems.length;
  $("#checklistStartCount").textContent = counts["시작"] || 0;
  $("#checklistProgressCount").textContent = counts["진행중"] || 0;
  $("#checklistHoldCount").textContent = counts["보류"] || 0;
  $("#checklistRolloverCount").textContent = counts["이월"] || 0;
  $("#checklistDoneCount").textContent = counts["완료"] || 0;
  const guide = $("#checklistMonthGuide");
  if (guide) guide.textContent = `${formatMonthLabel(checklistMonth)} · 이전 달 미완료 업무도 완료될 때까지 계속 표시됩니다. 완료건은 위 완료 카드를 눌러 확인하세요.`;

  const priorityOrder = { "긴급": 0, "높음": 1, "보통": 2, "낮음": 3 };
  const rows = monthItems
    .filter((item) => {
      if (statusFilter === "active") return item.status !== "완료";
      if (statusFilter === "전체") return true;
      return item.status === statusFilter;
    })
    .filter((item) => !priorityFilter || item.priority === priorityFilter)
    .filter((item) => !search || `${item.title} ${item.memo}`.toLowerCase().includes(search))
    .sort((a, b) => {
      const p = priorityOrder[a.priority] - priorityOrder[b.priority];
      if (p) return p;
      return `${a.date} ${a.appointmentTime || ""}`.localeCompare(`${b.date} ${b.appointmentTime || ""}`);
    });

  list.innerHTML = rows.length ? rows.map((item) => {
    const rolledFrom = item.month < checklistMonth;
    const hasReminder = Boolean(item.appointmentTime);
    const hasDetail = Boolean(item.memo || rolledFrom || (item.status === "이월" && item.rolloverMonth) || hasReminder);
    const timeLine = item.appointmentTime ? `<span class="checklist-compact-time">⏰ ${escapeHtml(checklistTimeLabel(item.appointmentTime))}</span>` : "";
    return `<article class="checklist-compact-item ${checklistStatusClass(item.status)} ${item.status === "완료" ? "is-done" : ""}" data-checklist-id="${escapeHtml(item.id)}">
      <div class="checklist-compact-row" ${hasDetail ? `data-checklist-toggle="${escapeHtml(item.id)}"` : ""}>
        <span class="checklist-meta-stack"><span class="checklist-compact-date">${escapeHtml(item.date)}</span>${timeLine}<span class="checklist-priority ${checklistPriorityClass(item.priority)}">${escapeHtml(item.priority)}</span></span>
        <strong class="checklist-compact-title">${escapeHtml(item.title)}</strong>
        ${hasDetail ? '<span class="checklist-expand-mark">▾</span>' : '<span class="checklist-expand-mark empty"></span>'}
        <select class="checklist-status-select ${checklistStatusClass(item.status)}" data-checklist-status-id="${escapeHtml(item.id)}">${CHECKLIST_STATUSES.map((status) => `<option value="${status}"${status === item.status ? " selected" : ""}>${status}</option>`).join("")}</select>
        <button class="ghost-button small" type="button" data-checklist-edit="${escapeHtml(item.id)}">수정</button>
        ${item.status !== "완료" ? `<button class="primary-button small" type="button" data-checklist-complete="${escapeHtml(item.id)}">완료</button>` : '<span class="checklist-complete-spacer"></span>'}
      </div>
      ${hasDetail ? `<div class="checklist-compact-detail" data-checklist-detail="${escapeHtml(item.id)}" hidden>
        ${item.memo ? `<p>${escapeHtml(item.memo).replace(/\n/g, "<br>")}</p>` : ""}
        <div class="checklist-rollover-info">
          ${item.appointmentTime ? `<span>⏰ ${checklistTimeLabel(item.appointmentTime)} · ${checklistReminderLabel(item.remindBefore)}</span>` : ""}
          ${rolledFrom ? `<span>↪ ${formatMonthLabel(item.month)} 미완료 업무 계속 표시</span>` : ""}
          ${item.status === "이월" && item.rolloverMonth ? `<span>→ ${formatMonthLabel(item.rolloverMonth)} 자동 노출</span>` : ""}
        </div>
      </div>` : ""}
    </article>`;
  }).join("") : `<div class="checklist-empty"><strong>${formatMonthLabel(checklistMonth)}에 표시할 업무가 없습니다.</strong><span>새 업무를 등록하거나 위 상태카드를 눌러 완료 업무를 확인하세요.</span></div>`;
}

function changeChecklistMonth(offset) {
  checklistMonth = shiftMonth(checklistMonth, offset);
  closeChecklistStatusModal();
  resetChecklistForm();
  renderChecklist();
}

function statusClass(status) {
  if (status === "완료") return "done";
  if (status === "보류") return "hold";
  return "";
}




function analyticsRound(value, digits = 1) {
  const factor = 10 ** digits;
  return Math.round(toNumber(value) * factor) / factor;
}

function analyticsAverage(values) {
  const nums = (values || []).map(toNumber);
  if (!nums.length) return 0;
  return nums.reduce((sum, value) => sum + value, 0) / nums.length;
}

function analyticsSettings() {
  state.salesAnalyticsSettings = normalizeSalesAnalyticsSettings(state.salesAnalyticsSettings);
  return state.salesAnalyticsSettings;
}

function analyticsAutoStartMonth() {
  const months = new Set();
  Object.keys(state.monthSettings || {}).forEach((month) => { if (/^\d{4}-\d{2}$/.test(month)) months.add(month); });
  Object.keys(state.managerManualStats || {}).forEach((month) => { if (/^\d{4}-\d{2}$/.test(month)) months.add(month); });
  (state.records || []).forEach((record) => {
    const month = recordGoalMonth(record);
    if (/^\d{4}-\d{2}$/.test(month)) months.add(month);
  });
  return [...months].sort()[0] || monthIso();
}

function analyticsEffectiveStartMonth(settings = analyticsSettings()) {
  const configured = String(settings?.branchStartMonth || "");
  if (settings?.branchStartMode === "manual" && /^\d{4}-\d{2}$/.test(configured)) return configured;
  return analyticsAutoStartMonth();
}

function analyticsMonthRange(start, end) {
  const settings = analyticsSettings();
  const safeStart = /^\d{4}-\d{2}$/.test(String(start || "")) ? start : analyticsEffectiveStartMonth(settings);
  const safeEnd = /^\d{4}-\d{2}$/.test(String(end || "")) ? end : monthIso();
  const first = safeStart <= safeEnd ? safeStart : safeEnd;
  const last = safeStart <= safeEnd ? safeEnd : safeStart;
  const months = [];
  let cursor = first;
  let guard = 0;
  while (cursor <= last && guard < 60) {
    months.push(cursor);
    cursor = shiftMonth(cursor, 1);
    guard += 1;
  }
  return months;
}

function analyticsRecordCount(record) {
  const count = toNumber(record?.count);
  return count > 0 ? count : 1;
}

function analyticsRecordMonth(record) {
  return recordGoalMonth(record);
}

function analyticsPersonKey(value) {
  let text = String(value || "").normalize("NFKC").trim().replace(/\s+/g, "");
  text = text.replace(/[()\[\]{}<>]/g, "");
  ["매니저", "마스터", "팀장", "지국장"].forEach((role) => {
    if (text !== role && text.endsWith(role)) text = text.slice(0, -role.length);
  });
  return text;
}

function analyticsCanonicalPersonName(value) {
  const raw = String(value || "").normalize("NFKC").trim().replace(/\s+/g, " ");
  const key = analyticsPersonKey(raw);
  if (!key) return "";
  const settings = analyticsSettings();
  const candidates = [
    ...teamManagerNames(),
    ...settings.sellerAliases.flatMap((rule) => [rule.source, rule.target]),
    "지국장",
    "팀장"
  ];
  const matched = candidates.find((name) => analyticsPersonKey(name) === key);
  return String(matched || raw).trim();
}

function analyticsResolveAliasName(value, month) {
  const settings = analyticsSettings();
  let name = analyticsCanonicalPersonName(value);
  const visited = new Set();
  for (let depth = 0; depth < 8 && name; depth += 1) {
    const key = analyticsPersonKey(name);
    if (!key || visited.has(key)) break;
    visited.add(key);
    const matching = settings.sellerAliases
      .filter((rule) => analyticsPersonKey(rule.source) === key && month >= rule.startMonth)
      .sort((a, b) => b.startMonth.localeCompare(a.startMonth))[0];
    if (!matching) break;
    const target = analyticsCanonicalPersonName(matching.target);
    if (!target || analyticsPersonKey(target) === key) break;
    name = target;
  }
  return analyticsCanonicalPersonName(name);
}

function normalizeActivityType(value) {
  const text = String(value || "").trim().normalize("NFKC");
  if (!text) return "";
  const compact = text.replace(/[\s._\-\/]+/g, "").toLowerCase();
  if (compact.includes("지원")) return "지원";
  if (compact.includes("오다컨스") || compact.includes("컨스") || compact.includes("콘스")) return "컨스";
  return "";
}

function recordActivityType(record) {
  // 구분은 사용자가 직접 선택한 값만 사용합니다.
  // 기타내용/메모에 '컨스' 또는 '지원' 문구가 있어도 자동으로 구분값을 만들지 않습니다.
  return normalizeActivityType(
    record?.activityType ?? record?.distinction ?? record?.supportType ?? record?.consType ?? ""
  );
}

function activityTypeChipClass(value) {
  const type = normalizeActivityType(value);
  if (type === "컨스") return "activity-cons";
  if (type === "지원") return "activity-support";
  return "activity-none";
}

function analyticsActivityText(record) {
  return [
    record?.memo,
    record?.note,
    record?.etc,
    record?.extra,
    record?.activity,
    record?.activityType,
    record?.support,
    record?.supportType,
    record?.cons,
    record?.consType
  ].map((value) => String(value || "")).join(" ").normalize("NFKC").toLowerCase();
}

function analyticsBaseRecordsForMonth(month) {
  const period = monthPeriod(month);
  return (state.records || []).filter((record) => {
    if (!record || record.status === "취소" || isMembershipRecord(record)) return false;
    if (!recordBelongsToCurrentUserTeam(record, month)) return false;
    return inDateRange(record.receivedDate || "", period.start, period.end);
  });
}

function analyticsActivityFlags(record) {
  // 접수리스트의 '구분' 직접 선택값만 집계합니다.
  const type = recordActivityType(record);
  return {
    cons: type === "컨스",
    orderCons: false,
    support: type === "지원",
    excludedFromPure: false
  };
}

function analyticsResolveSellerName(record) {
  const settings = analyticsSettings();
  const month = analyticsRecordMonth(record) || analyticsEffectiveStartMonth(settings);
  const seller = String(record?.seller || "").trim() || String(record?.manager || "").trim();
  return analyticsResolveAliasName(seller, month);
}

function analyticsActualEntityNames() {
  const settings = analyticsSettings();
  const hiddenKeys = new Set(settings.hiddenSellers.map(analyticsPersonKey));
  const names = [];
  const add = (name) => {
    const value = analyticsResolveAliasName(name, analyticsEffectiveStartMonth(settings)) || analyticsCanonicalPersonName(name);
    const key = analyticsPersonKey(value);
    if (!key || hiddenKeys.has(key) || names.some((item) => analyticsPersonKey(item) === key)) return;
    names.push(value);
  };
  teamManagerNames().forEach(add);
  (state.records || []).forEach((record) => {
    const recordMonth = analyticsRecordMonth(record);
    if (recordMonth < analyticsEffectiveStartMonth(settings)) return;
    if (!recordBelongsToCurrentUserTeam(record, recordMonth)) return;
    add(analyticsResolveSellerName(record));
  });
  ["지국장", "팀장"].forEach((role) => {
    if ((state.records || []).some((record) => {
      const recordMonth = analyticsRecordMonth(record);
      return recordMonth >= analyticsEffectiveStartMonth(settings)
        && recordBelongsToCurrentUserTeam(record, recordMonth)
        && analyticsPersonKey(analyticsResolveSellerName(record)) === analyticsPersonKey(role);
    })) add(role);
  });
  return sortManagerNamesByDisplayOrder(names);
}

function analyticsAliasSourcesForTarget(targetName, month) {
  const targetKey = analyticsPersonKey(analyticsResolveAliasName(targetName, month));
  return analyticsSettings().sellerAliases
    .filter((rule) => month >= rule.startMonth && analyticsPersonKey(analyticsResolveAliasName(rule.target, month)) === targetKey)
    .map((rule) => analyticsCanonicalPersonName(rule.source));
}

function analyticsReportedManagerName(record) {
  const month = analyticsRecordMonth(record) || analyticsEffectiveStartMonth();
  return analyticsResolveAliasName(record?.manager, month);
}

function analyticsManualStat(managerName, month) {
  normalizeManualStatsBucket();
  const bucket = state.managerManualStats?.[month] || {};
  if (managerName) {
    const names = [managerName, ...analyticsAliasSourcesForTarget(managerName, month)];
    return names.reduce((sum, name) => {
      const stat = bucket[name] || {};
      sum.renewal += toNumber(stat.renewal);
      sum.orderCons += toNumber(stat.orderCons);
      sum.support += toNumber(stat.support);
      sum.refund += toNumber(stat.refund);
      return sum;
    }, { renewal: 0, orderCons: 0, support: 0, refund: 0 });
  }
  return Object.values(bucket).reduce((sum, item) => {
    sum.renewal += toNumber(item?.renewal);
    sum.orderCons += toNumber(item?.orderCons);
    sum.support += toNumber(item?.support);
    sum.refund += toNumber(item?.refund);
    return sum;
  }, { renewal: 0, orderCons: 0, support: 0, refund: 0 });
}

function analyticsGoalFor(managerName, month) {
  if (!managerName) return toNumber(calculatedGoals(month).overallGoal);
  const bucket = state.managerMonthlyGoals?.[month];
  if (bucket && Object.prototype.hasOwnProperty.call(bucket, managerName)) return toNumber(bucket[managerName]);
  const manager = state.managers.map(normalizeManager).find((item) => item.name === managerName);
  return toNumber(manager?.goal);
}

function analyticsReportedRecords(month, managerName = "") {
  return analyticsBaseRecordsForMonth(month).filter((record) => !managerName || analyticsReportedManagerName(record) === managerName);
}

function analyticsActualRecords(month, entityName = "", includeExcluded = true) {
  const entityKey = analyticsPersonKey(entityName);
  return analyticsBaseRecordsForMonth(month).filter((record) => {
    const seller = analyticsResolveSellerName(record);
    if (!seller || (entityKey && analyticsPersonKey(seller) !== entityKey)) return false;
    return true;
  });
}

function analyticsActivityRecords(month, entityName = "") {
  const entityKey = analyticsPersonKey(entityName);
  return analyticsBaseRecordsForMonth(month).filter((record) => {
    const flags = analyticsActivityFlags(record);
    if (!flags.cons && !flags.orderCons && !flags.support) return false;
    if (!entityKey) return true;
    const sellerKey = analyticsPersonKey(analyticsResolveSellerName(record));
    const managerKey = analyticsPersonKey(analyticsReportedManagerName(record));
    // 컨스·지원은 실제 판매자뿐 아니라 해당 실적을 받은 등록 매니저에도 귀속합니다.
    return sellerKey === entityKey || managerKey === entityKey;
  });
}

function analyticsAttributionBreakdown(records, entityName) {
  const targetKey = analyticsPersonKey(entityName);
  const sources = new Map();
  let direct = 0;
  (records || []).forEach((record) => {
    const raw = String(record?.seller || "").trim() || String(record?.manager || "").trim();
    const resolved = analyticsResolveSellerName(record);
    if (analyticsPersonKey(resolved) !== targetKey) return;
    const count = analyticsRecordCount(record);
    const rawName = analyticsCanonicalPersonName(raw) || entityName;
    if (analyticsPersonKey(rawName) === targetKey) direct += count;
    else sources.set(rawName, (sources.get(rawName) || 0) + count);
  });
  return {
    direct,
    sources: [...sources.entries()].sort((a, b) => b[1] - a[1])
  };
}

function analyticsAttributionLabel(records, entityName) {
  const result = analyticsAttributionBreakdown(records, entityName);
  const parts = [];
  if (result.direct) parts.push(`직접 ${formatNumber(result.direct)}건`);
  result.sources.forEach(([name, count]) => parts.push(`${name} 통합 ${formatNumber(count)}건`));
  return parts.join(" · ") || "해당월 실제 판매 귀속 없음";
}

function analyticsCategoryTotals(records) {
  const totals = { newCount: 0, packageCount: 0, rentalCount: 0, cashCount: 0 };
  (records || []).forEach((record) => {
    const count = analyticsRecordCount(record);
    const category = normalizeCategory(record.category);
    if (category === "신규") totals.newCount += count;
    else if (category === "패키지") totals.packageCount += count;
    else if (category === "재렌탈") totals.rentalCount += count;
    else if (category === "일시불") totals.cashCount += count;
  });
  return totals;
}

function analyticsKeywordActivityCount(records, key) {
  return (records || []).reduce((sum, record) => {
    const flags = analyticsActivityFlags(record);
    return sum + (flags[key] ? analyticsRecordCount(record) : 0);
  }, 0);
}

function analyticsMonthHasData(month) {
  if (analyticsBaseRecordsForMonth(month).length) return true;
  const bucket = state.managerManualStats?.[month];
  if (bucket && Object.values(bucket).some((item) => toNumber(item?.renewal) || toNumber(item?.orderCons) || toNumber(item?.support) || toNumber(item?.refund))) return true;
  return false;
}

function analyticsMonthStatus(month) {
  const settings = analyticsSettings();
  if (month < analyticsEffectiveStartMonth(settings)) return "분석제외";
  const current = monthIso();
  if (month === current) return "진행중";
  if (month > current) return "예정";
  if (analyticsMonthHasData(month)) return "입력완료";
  if (settings.monthStatusMode === "manual") {
    const explicit = settings.monthDataStatus?.[month];
    if (explicit) return explicit;
  }
  return "미입력";
}

function analyticsIsCompletedMonth(month) {
  return analyticsMonthStatus(month) === "입력완료" && Boolean(monthPeriod(month).end < todayIso());
}

function analyticsMonthlyMetrics(month, entityName = "") {
  const reportedRecords = analyticsReportedRecords(month, entityName);
  const actualAllRecords = analyticsActualRecords(month, entityName, true);
  const actualPureRecords = analyticsActualRecords(month, entityName, false);
  const reported = analyticsCategoryTotals(reportedRecords);
  const actual = analyticsCategoryTotals(actualPureRecords);
  const manual = analyticsManualStat(entityName, month);
  const consCount = analyticsKeywordActivityCount(reportedRecords, "cons");
  const supportCount = analyticsKeywordActivityCount(reportedRecords, "support");
  const actualSupportCount = analyticsKeywordActivityCount(actualPureRecords, "support");
  // 대시보드와 동일하게 지원은 영업실적에 1회 반영하고, 컨스는 별도 관리 건수로만 집계합니다.
  const reportBusiness = reported.newCount + reported.packageCount + reported.rentalCount + reported.cashCount + supportCount;
  const renewal = toNumber(manual.renewal);
  const refund = toNumber(manual.refund);
  const reportFinal = reportBusiness + renewal - refund;
  // 실제 판매자 기준 접수실적도 같은 기준으로 지원을 1회 반영합니다.
  const actualPure = actual.newCount + actual.packageCount + actual.rentalCount + actual.cashCount + actualSupportCount;
  const actualEvaluated = actual.newCount + actual.packageCount + actual.rentalCount;
  const goal = analyticsGoalFor(entityName, month);
  return {
    month,
    status: analyticsMonthStatus(month),
    reportedRecords,
    actualAllRecords,
    actualPureRecords,
    reportNew: reported.newCount,
    reportPackage: reported.packageCount,
    reportRental: reported.rentalCount,
    reportCash: reported.cashCount,
    reportBusiness,
    renewal,
    orderCons: 0,
    refund,
    reportFinal,
    actualNew: actual.newCount,
    actualPackage: actual.packageCount,
    actualRental: actual.rentalCount,
    actualCash: actual.cashCount,
    actualPure,
    actualEvaluated,
    consCount,
    supportCount,
    difference: reportFinal - actualPure,
    goal,
    rate: goal > 0 ? reportFinal / goal * 100 : 0
  };
}

function analyticsEntityHasActualEvidence(entityName, month) {
  return analyticsActualRecords(month, entityName, true).length > 0;
}

function analyticsEntityActiveMonths(entityName, completedMonths) {
  const settings = analyticsSettings();
  const configuredStart = settings.sellerStartMonths?.[entityName] || "";
  const inferredStart = completedMonths.find((month) => analyticsEntityHasActualEvidence(entityName, month)) || "";
  const startMonth = configuredStart || inferredStart;
  if (!startMonth) return [];
  const branchStart = analyticsEffectiveStartMonth(settings);
  const effectiveStart = startMonth < branchStart ? branchStart : startMonth;
  return completedMonths.filter((month) => month >= effectiveStart);
}

function analyticsMetricAverages(metrics) {
  const keys = [
    "actualNew", "actualPackage", "actualRental", "actualCash", "actualPure", "actualEvaluated",
    "reportFinal", "renewal", "refund", "consCount", "supportCount"
  ];
  const result = { months: metrics.length };
  keys.forEach((key) => { result[key] = analyticsAverage(metrics.map((item) => item[key])); });
  return result;
}

function analyticsManagerSummary(entityName, months) {
  const completed = months.filter(analyticsIsCompletedMonth);
  const activeMonths = analyticsEntityActiveMonths(entityName, completed);
  const history = activeMonths.map((month) => analyticsMonthlyMetrics(month, entityName));
  const average = analyticsMetricAverages(history);
  const currentMonth = months[months.length - 1] || monthIso();
  const current = analyticsMonthlyMetrics(currentMonth, entityName);
  const recent = history.slice(-3);
  const previous = history.slice(-6, -3);
  const recentAverage = analyticsAverage(recent.map((item) => item.actualEvaluated));
  const previousAverage = analyticsAverage(previous.map((item) => item.actualEvaluated));
  const trendRate = previousAverage > 0
    ? (recentAverage - previousAverage) / previousAverage * 100
    : (recentAverage > 0 ? 100 : 0);
  return {
    managerName: entityName,
    activeMonths,
    analysisStartMonth: activeMonths[0] || analyticsSettings().sellerStartMonths?.[entityName] || "",
    history,
    average,
    current,
    recentAverage,
    previousAverage,
    trendRate
  };
}

function analyticsCategoryLabel(key) {
  return { actualNew: "신규", actualPackage: "패키지", actualRental: "재렌탈", actualCash: "일시불" }[key] || key;
}

function analyticsTeamCategoryAverages(managerSummaries) {
  const valid = managerSummaries.filter((item) => item.average.months > 0);
  const result = {};
  ["actualNew", "actualPackage", "actualRental"].forEach((key) => {
    result[key] = analyticsAverage(valid.map((item) => item.average[key]));
  });
  return result;
}

function analyticsManagerType(summary, teamCategoryAverages) {
  if (!summary.average.months) return "자료부족";
  const keys = ["actualNew", "actualPackage", "actualRental"];
  const total = keys.reduce((sum, key) => sum + toNumber(summary.average[key]), 0);
  const shares = keys.map((key) => ({ key, share: total > 0 ? summary.average[key] / total : 0 }));
  const strongest = [...shares].sort((a, b) => b.share - a.share)[0];
  const labels = [];
  if (strongest?.share >= 0.60) labels.push("단일영역 편중형");
  else labels.push("다영역형");
  const relative = keys.map((key) => {
    const team = toNumber(teamCategoryAverages[key]);
    return { key, rate: team > 0 ? (summary.average[key] - team) / team : (summary.average[key] > 0 ? 1 : 0) };
  }).sort((a, b) => b.rate - a.rate);
  if (relative[0] && summary.average[relative[0].key] > 0) labels.push(`${analyticsCategoryLabel(relative[0].key)} 강점형`);
  if (summary.trendRate >= 10) labels.push("성장형");
  else if (summary.trendRate <= -10) labels.push("하락주의");
  return labels.slice(0, 3).join(" · ");
}

function analyticsRangeValues() {
  const settings = analyticsSettings();
  const fallbackEnd = $("#monthFilter")?.value || monthIso();
  const rawEnd = $("#analyticsEndMonth")?.value || fallbackEnd;
  const end = /^\d{4}-\d{2}$/.test(rawEnd) ? rawEnd : fallbackEnd;
  const rawStart = $("#analyticsStartMonth")?.value || shiftMonth(end, -(analyticsRangePreset - 1));
  const start = /^\d{4}-\d{2}$/.test(rawStart)
    ? (rawStart < analyticsEffectiveStartMonth(settings) ? analyticsEffectiveStartMonth(settings) : rawStart)
    : analyticsEffectiveStartMonth(settings);
  return { start, end, months: analyticsMonthRange(start, end) };
}

function setAnalyticsRangePreset(count) {
  analyticsRangePreset = Number(count) || 6;
  const settings = analyticsSettings();
  const end = $("#analyticsEndMonth")?.value || $("#monthFilter")?.value || monthIso();
  let start = shiftMonth(end, -(analyticsRangePreset - 1));
  if (start < analyticsEffectiveStartMonth(settings)) start = analyticsEffectiveStartMonth(settings);
  if ($("#analyticsStartMonth")) $("#analyticsStartMonth").value = start;
  if ($("#analyticsEndMonth")) $("#analyticsEndMonth").value = end;
  $$("[data-analysis-months]").forEach((button) => button.classList.toggle("active", Number(button.dataset.analysisMonths) === analyticsRangePreset));
  renderAnalytics();
}

function analyticsPeriodProgress(month) {
  const period = monthPeriod(month);
  const start = new Date(`${period.start}T00:00:00`);
  const end = new Date(`${period.end}T00:00:00`);
  const today = new Date(`${todayIso()}T00:00:00`);
  const totalDays = Math.max(1, Math.round((end - start) / 86400000) + 1);
  let elapsedDays = 0;
  if (today >= end) elapsedDays = totalDays;
  else if (today >= start) elapsedDays = Math.max(1, Math.round((today - start) / 86400000) + 1);
  return { ...period, totalDays, elapsedDays, remainingDays: Math.max(totalDays - elapsedDays, 0), progress: Math.min(1, elapsedDays / totalDays) };
}

function analyticsProductItems(record) {
  const raw = String(record?.product || "").trim();
  if (!raw) return [];
  const parts = raw.split(/\r?\n|\s+\+\s+|\s*&\s*|\s*;\s*|\s*／\s*/g).map((item) => item.trim()).filter(Boolean);
  return parts.length ? parts : [raw];
}

function analyticsPhysicalProductCount(record) {
  const performanceCount = analyticsRecordCount(record);
  // 판매실적 0.5건이어도 실제 제품이 한 개이면 제품분석에서는 1대로 계산합니다.
  return Math.max(1, Math.ceil(performanceCount));
}

function analyticsModelName(productText) {
  const text = String(productText || "").trim();
  if (!text) return "미입력";
  const match = text.match(/^([A-Za-z0-9][A-Za-z0-9\-_.]+)/);
  return match ? match[1].toUpperCase() : text.slice(0, 45);
}

function analyticsProductFamily(productText) {
  const text = String(productText || "").trim();
  const upper = text.toUpperCase();
  for (const rule of analyticsSettings().productRules) {
    const matched = rule.terms.some((term) => {
      const value = String(term || "").trim();
      if (!value) return false;
      const upperTerm = value.toUpperCase();
      if (/^[A-Z0-9]+-$/.test(upperTerm)) return upper.startsWith(upperTerm);
      return upper.includes(upperTerm);
    });
    if (matched) return ["인덕션/전기레인지", "전기레인지"].includes(rule.family) ? "인덕션" : rule.family;
  }
  return "미분류";
}

function analyticsProductStats(months, entityName = "") {
  const familyMap = new Map();
  const modelMap = new Map();
  const validMonths = months.filter((month) => !["미입력", "분석제외", "예정"].includes(analyticsMonthStatus(month)));
  validMonths.forEach((month) => {
    analyticsActualRecords(month, entityName, false).forEach((record) => {
      const productCount = analyticsPhysicalProductCount(record);
      const items = analyticsProductItems(record);
      const familiesInContract = new Set();
      items.forEach((item) => {
        const family = analyticsProductFamily(item);
        const model = analyticsModelName(item);
        familiesInContract.add(family);
        const familyItem = familyMap.get(family) || { family, contracts: 0, units: 0, models: new Map() };
        familyItem.units += productCount;
        familyItem.models.set(model, (familyItem.models.get(model) || 0) + productCount);
        familyMap.set(family, familyItem);
        const modelKey = `${family}::${model}`;
        const modelItem = modelMap.get(modelKey) || { family, model, units: 0, contracts: 0 };
        modelItem.units += productCount;
        modelItem.contracts += productCount;
        modelMap.set(modelKey, modelItem);
      });
      familiesInContract.forEach((family) => {
        const familyItem = familyMap.get(family);
        if (familyItem) familyItem.contracts += productCount;
      });
    });
  });
  const families = [...familyMap.values()].sort((a, b) => b.units - a.units);
  const models = [...modelMap.values()].sort((a, b) => b.units - a.units);
  const totalUnits = families.reduce((sum, item) => sum + item.units, 0);
  const totalContracts = validMonths.reduce((sum, month) => sum + analyticsActualRecords(month, entityName, false)
    .reduce((monthSum, record) => monthSum + analyticsPhysicalProductCount(record), 0), 0);
  return { families, models, totalUnits, totalContracts, validMonthCount: validMonths.length };
}

function analyticsTopProductFamily(entityName, months) {
  return analyticsProductStats(months, entityName).families[0] || null;
}

function analyticsStrengthWeakness(summary, teamAverages, months = []) {
  if (!summary?.average?.months) {
    return {
      strength: "입력완료된 월 데이터가 충분하지 않아 실제 강점을 확정하기 어렵습니다.",
      weakness: "최소 2개월 이상 실제 판매자 기준 데이터가 쌓이면 지국 평균과 비교합니다.",
      action: "실판매자와 제품명을 빠짐없이 입력하고, 미입력월은 경영평가 기준 설정에서 상태를 관리하세요."
    };
  }
  const keys = ["actualNew", "actualPackage", "actualRental"];
  const comparisons = keys.map((key) => {
    const managerValue = toNumber(summary.average[key]);
    const teamValue = toNumber(teamAverages[key]);
    return { key, managerValue, teamValue, diffRate: teamValue > 0 ? (managerValue - teamValue) / teamValue * 100 : (managerValue > 0 ? 100 : 0) };
  }).sort((a, b) => b.diffRate - a.diffRate);
  const best = comparisons[0];
  const weak = comparisons[comparisons.length - 1];
  const topProduct = analyticsTopProductFamily(summary.managerName, months);
  let strength = `${analyticsCategoryLabel(best.key)} 실제실적 월평균 ${formatNumber(best.managerValue)}건으로 지국 평균 ${formatNumber(best.teamValue)}건 대비 ${best.diffRate >= 0 ? `${Math.abs(Math.round(best.diffRate))}% 높습니다` : "현재 본인 판매영역 중 가장 안정적입니다"}.`;
  if (topProduct) strength += ` 판매제품은 ${topProduct.family}가 ${formatNumber(topProduct.units)}대로 가장 강합니다.`;
  const weakness = `${analyticsCategoryLabel(weak.key)} 실제실적 월평균은 ${formatNumber(weak.managerValue)}건으로 지국 평균 ${formatNumber(weak.teamValue)}건 대비 ${Math.abs(Math.round(weak.diffRate))}% ${weak.diffRate < 0 ? "낮습니다" : "차이가 크지 않습니다"}.`;
  let action = `${analyticsCategoryLabel(weak.key)}를 주간 실행목표로 별도 관리하고, 강점인 ${analyticsCategoryLabel(best.key)} 상담에서 연계 제안을 강화하는 것이 좋습니다.`;
  if (summary.trendRate <= -10) action += ` 최근 실제실적 평균이 이전 기간보다 ${Math.abs(Math.round(summary.trendRate))}% 낮아져 활동량과 컨텍 후속관리를 즉시 점검해야 합니다.`;
  else if (summary.trendRate >= 10) action += ` 최근 실제실적은 ${Math.round(summary.trendRate)}% 상승세이므로 현재 방식은 유지하되 약한 영역만 보완하는 편이 효율적입니다.`;
  return { strength, weakness, action, best, weak };
}

function analyticsStaleContactCount(managerName) {
  const cutoff = addDaysIso(todayIso(), -7);
  return (state.contactNotes || []).map(normalizeContactNote).filter((note) => {
    if (managerName && note.manager !== managerName) return false;
    if (!["진행중", "보류"].includes(note.status)) return false;
    const latest = contactNoteLatestProgress(note);
    return !latest?.date || latest.date <= cutoff;
  }).length;
}

function analyticsBuildRecommendations(months, summaries, teamAverages) {
  const endMonth = months[months.length - 1] || monthIso();
  const overall = analyticsMonthlyMetrics(endMonth, "");
  const period = analyticsPeriodProgress(endMonth);
  const items = [];
  const shortage = Math.max(overall.goal - overall.reportFinal, 0);
  if (shortage > 0) {
    items.push({ id: "overall-goal", scope: "지국 전체", priority: "긴급", title: `${formatMonthLabel(endMonth)} 최종실적 목표 부족 ${formatNumber(shortage)}건 집중관리`, detail: period.remainingDays > 0 ? `남은 ${period.remainingDays}일 동안 최종실적 기준 하루 평균 ${formatNumber(shortage / period.remainingDays)}건이 필요합니다.` : `공식 목표 대비 ${formatNumber(shortage)}건이 부족합니다.` });
  }
  if (overall.difference > 0) {
    items.push({ id: "overall-pure-gap", scope: "지국 전체", priority: "높음", title: `최종실적과 실제 접수실적 차이 ${formatNumber(overall.difference)}건 점검`, detail: `재약정 ${formatNumber(overall.renewal)}건을 더하고 환수 ${formatNumber(overall.refund)}건을 차감한 최종실적입니다. 컨스 등록 ${formatNumber(overall.consCount)}건은 지급관리 현황으로 확인하며, 지원은 ${formatNumber(overall.supportCount)}건입니다.` });
  }
  summaries.forEach((summary, index) => {
    if (!summary.average.months) return;
    const diagnosis = analyticsStrengthWeakness(summary, teamAverages, months);
    if (summary.current.actualEvaluated < summary.average.actualEvaluated * 0.8) {
      items.push({ id: `manager-low-${index}`, scope: summary.managerName, priority: "긴급", title: `${summary.managerName} 평가영업 회복 점검`, detail: `선택월 평가대상 영업 ${formatNumber(summary.current.actualEvaluated)}건으로 본인 완료월 평가평균 ${formatNumber(summary.average.actualEvaluated)}건보다 낮습니다. 강약점 평가는 신규·패키지·재렌탈 기준입니다.` });
    }
    if (diagnosis.weak) {
      const configuredMasterName = compactValue((state.appMeta || sampleState.appMeta || {}).masterName, "");
      const adminSeller = [configuredMasterName, "지국장"].filter(Boolean).includes(summary.managerName);
      const excludedRentalRecommendation = adminSeller && diagnosis.weak.key === "actualRental";
      if (!excludedRentalRecommendation) {
        items.push({ id: `manager-weak-${index}`, scope: summary.managerName, priority: "높음", title: `${summary.managerName} ${analyticsCategoryLabel(diagnosis.weak.key)} 실행목표 설정`, detail: diagnosis.weakness });
      }
    }
  });
  return items.slice(0, 20);
}

function analyticsAddRecommendationTask(id) {
  const item = analyticsRecommendationCache.find((entry) => entry.id === id);
  if (!item) return;
  const duplicate = (state.checklistItems || []).map(normalizeChecklistItem).find((task) => task.title === item.title && task.status !== "완료");
  if (duplicate) {
    showToast("같은 실행업무가 이미 업무체크리스트에 있습니다.");
    return;
  }
  const date = todayIso();
  state.checklistItems.push(normalizeChecklistItem({
    id: uid("task"), title: item.title, memo: `[영업분석 자동제안 · ${item.scope}] ${item.detail}`,
    date, month: date.slice(0, 7), status: "시작", priority: item.priority === "긴급" ? "긴급" : "높음",
    appointmentTime: "", remindBefore: -1, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString()
  }));
  persistState();
  renderChecklist();
  showToast("실행제안을 업무체크리스트에 등록했습니다.");
}

function setAnalyticsTab(tab) {
  analyticsActiveTab = ["overview", "monthly", "managers", "products", "actions", "combined"].includes(tab) ? tab : "overview";
  $$("[data-analysis-tab]").forEach((button) => button.classList.toggle("active", button.dataset.analysisTab === analyticsActiveTab));
  $$("[data-analysis-panel]").forEach((panel) => panel.classList.toggle("active", panel.dataset.analysisPanel === analyticsActiveTab));
  if (analyticsActiveTab === "overview") window.requestAnimationFrame(drawAnalyticsTrendChart);
}

function analyticsStatusBadge(status) {
  const className = { "입력완료": "complete", "미입력": "missing", "진행중": "current", "예정": "future", "분석제외": "excluded" }[status] || "";
  return `<span class="analytics-data-status ${className}">${escapeHtml(status)}</span>`;
}

function renderAnalyticsCategoryMix(metrics) {
  const box = $("#analyticsCategoryMix");
  if (!box) return;
  const rows = [["actualNew", "신규", "new"], ["actualPackage", "패키지", "package"], ["actualRental", "재렌탈", "rental"], ["actualCash", "일시불", "cash"]];
  // 이 차트는 '판매종류 구성'만 보여주므로 화면에 표시하는 4개 판매종류의 합을 분모로 사용합니다.
  // 지원/컨스 현황 때문에 비율 합계가 100%가 되지 않던 표시 오류를 방지합니다.
  const categoryTotal = rows.reduce((sum, [key]) => sum + toNumber(metrics[key]), 0);
  const total = Math.max(1, categoryTotal);
  box.innerHTML = rows.map(([key, label, className]) => {
    const value = toNumber(metrics[key]);
    const rate = value / total * 100;
    return `<div class="analytics-mix-row ${className}"><div><strong>${label}</strong><span>${formatNumber(value)}건 · ${Math.round(rate)}%</span></div><div class="analytics-mix-track"><span style="width:${Math.min(100, rate)}%"></span></div></div>`;
  }).join("");
}

function renderAnalyticsDiagnosis(endMetrics, completedMetrics, period, summaries) {
  const list = $("#analyticsDiagnosisList");
  if (!list) return;
  const diagnostics = [];
  if (endMetrics.status === "미입력") {
    diagnostics.push({ tone: "danger", text: `${formatMonthLabel(endMetrics.month)}은 데이터 상태가 미입력입니다. 월평균과 강약점 분석에 포함하지 않습니다.` });
  } else {
    diagnostics.push({ tone: endMetrics.difference > 0 ? "watch" : "good", text: `${formatMonthLabel(endMetrics.month)} 최종실적은 ${formatNumber(endMetrics.reportFinal)}건, 실제 접수실적은 ${formatNumber(endMetrics.actualPure)}건입니다. 재약정 ${formatNumber(endMetrics.renewal)}건을 더하고 환수 ${formatNumber(endMetrics.refund)}건을 차감했습니다.` });
    diagnostics.push({ tone: endMetrics.actualPure > 0 ? "good" : "danger", text: `판매종류 구성은 신규 ${formatNumber(endMetrics.actualNew)}, 패키지 ${formatNumber(endMetrics.actualPackage)}, 재렌탈 ${formatNumber(endMetrics.actualRental)}, 일시불 ${formatNumber(endMetrics.actualCash)}건이며, 컨스 등록 ${formatNumber(endMetrics.consCount)}건 · 지원 ${formatNumber(endMetrics.supportCount)}건입니다.` });
  }
  const average = analyticsAverage(completedMetrics.map((item) => item.actualEvaluated));
  diagnostics.push({ tone: completedMetrics.length >= 2 ? "good" : "watch", text: `입력완료된 완료월 ${completedMetrics.length}개월의 평가대상 영업 평균은 ${formatNumber(average)}건입니다. 일시불은 단순 건수로만 표시하고 평가에서는 제외합니다. 미입력월과 현재 진행월은 평균에서 제외했습니다.` });
  if (endMetrics.goal > 0) {
    const shortage = Math.max(endMetrics.goal - endMetrics.reportFinal, 0);
    diagnostics.push({ tone: shortage > 0 ? "danger" : "good", text: shortage > 0 ? `공식 보고목표까지 ${formatNumber(shortage)}건이 부족하며 남은 ${period.remainingDays}일 기준 하루 평균 ${formatNumber(period.remainingDays ? shortage / period.remainingDays : shortage)}건이 필요합니다.` : `공식 보고목표를 ${formatNumber(endMetrics.reportFinal - endMetrics.goal)}건 초과했습니다.` });
  }
  const lowManagers = summaries.filter((item) => item.average.months && item.current.actualEvaluated < item.average.actualEvaluated * 0.8);
  if (lowManagers.length) diagnostics.push({ tone: "watch", text: `본인 평가 월평균보다 20% 이상 낮은 판매자는 ${lowManagers.map((item) => item.managerName).join(", ")}입니다.` });
  list.innerHTML = diagnostics.map((item, index) => `<div class="analytics-diagnosis-item ${item.tone}"><span>${index + 1}</span><p>${escapeHtml(item.text)}</p></div>`).join("");
}

function renderAnalyticsManagerSnapshot(summaries, teamAverages, months) {
  const body = $("#analyticsManagerSnapshotBody");
  if (!body) return;
  body.innerHTML = summaries.map((summary) => {
    const average = summary.average.actualEvaluated;
    const delta = summary.current.actualEvaluated - average;
    const diagnosis = analyticsStrengthWeakness(summary, teamAverages, months);
    return `<tr data-analytics-manager-row="${escapeHtml(summary.managerName)}"><td><strong>${escapeHtml(summary.managerName)}</strong><small class="analytics-attribution-note">${escapeHtml(analyticsAttributionLabel(summary.current.actualAllRecords, summary.managerName))}</small></td><td>${formatNumber(summary.current.reportFinal)}</td><td class="pure-metric">${formatNumber(summary.current.actualPure)}</td><td>${summary.average.months ? formatNumber(average) : "-"}</td><td class="${delta >= 0 ? "positive" : "negative"}">${summary.average.months ? `${delta >= 0 ? "+" : ""}${formatNumber(delta)}` : "-"}</td><td>${formatNumber(summary.current.renewal)}</td><td class="refund-text">${formatNumber(summary.current.refund)}</td><td>${formatNumber(summary.current.consCount)}</td><td>${formatNumber(summary.current.supportCount)}</td><td><span class="analytics-type-badge">${escapeHtml(analyticsManagerType(summary, teamAverages))}</span><small>${escapeHtml(diagnosis.weakness)}</small></td></tr>`;
  }).join("") || `<tr><td colspan="10" class="empty">분석할 실판매자 데이터가 없습니다.</td></tr>`;
}

function renderAnalyticsMonthlyTable(months, entityName) {
  const body = $("#analyticsMonthlyBody");
  if (!body) return;
  body.innerHTML = months.map((month) => {
    const item = analyticsMonthlyMetrics(month, entityName);
    const unavailable = ["미입력", "분석제외", "예정"].includes(item.status);
    const v = (value) => unavailable ? "-" : formatNumber(value);
    return `<tr class="${item.status === "진행중" ? "current-month" : ""} ${item.status === "미입력" ? "missing-month" : ""}"><td><strong>${formatMonthLabel(item.month)}</strong>${analyticsStatusBadge(item.status)}</td><td class="analytics-report-number">${v(item.reportFinal)}</td><td class="pure-metric analytics-actual-number">${v(item.actualPure)}</td><td>${v(item.renewal)}</td><td class="refund-text">${v(item.refund)}</td><td>${v(item.consCount)}</td><td>${v(item.supportCount)}</td><td class="analytics-difference-number ${item.difference > 0 ? "negative" : "positive"}">${v(item.difference)}</td><td>${v(item.actualNew)}</td><td>${v(item.actualPackage)}</td><td>${v(item.actualRental)}</td><td>${v(item.actualCash)}</td><td>${v(item.goal)}</td><td>${unavailable ? "-" : `${Math.round(item.rate)}%`}</td></tr>`;
  }).join("");
  if ($("#analyticsMonthlyScope")) $("#analyticsMonthlyScope").textContent = entityName || "지국 전체";
}

function renderAnalyticsMatrix(months, entityNames) {
  const head = $("#analyticsMatrixHead");
  const body = $("#analyticsMatrixBody");
  if (!head || !body) return;
  head.innerHTML = `<tr><th>월</th>${entityNames.map((name) => `<th>${escapeHtml(name)}<small>최종 / 실제실적</small></th>`).join("")}</tr>`;
  body.innerHTML = months.map((month) => {
    const status = analyticsMonthStatus(month);
    return `<tr><td><strong>${formatMonthLabel(month)}</strong>${analyticsStatusBadge(status)}</td>${entityNames.map((name) => {
      if (["미입력", "분석제외", "예정"].includes(status)) return "<td>-</td>";
      const item = analyticsMonthlyMetrics(month, name);
      return `<td class="analytics-matrix-number"><strong>${formatNumber(item.reportFinal)}</strong><small>${formatNumber(item.actualPure)}</small></td>`;
    }).join("")}</tr>`;
  }).join("");
}

function renderAnalyticsManagerAverage(summaries, teamAverages) {
  const body = $("#analyticsManagerAverageBody");
  if (!body) return;
  body.innerHTML = summaries.map((summary) => `<tr data-analytics-manager-row="${escapeHtml(summary.managerName)}"><td><button class="analytics-manager-link" type="button" data-analytics-manager-select="${escapeHtml(summary.managerName)}">${escapeHtml(summary.managerName)}</button></td><td>${summary.average.months}개월</td><td>${formatNumber(summary.average.actualNew)}</td><td>${formatNumber(summary.average.actualPackage)}</td><td>${formatNumber(summary.average.actualRental)}</td><td class="pure-metric">${formatNumber(summary.average.actualEvaluated)}</td><td>${formatNumber(summary.average.renewal)}</td><td class="refund-text">${formatNumber(summary.average.refund)}</td><td>${formatNumber(summary.average.consCount)}</td><td>${formatNumber(summary.average.supportCount)}</td><td><span class="analytics-type-badge">${escapeHtml(analyticsManagerType(summary, teamAverages))}</span></td></tr>`).join("") || `<tr><td colspan="11" class="empty">입력완료된 실제 판매자 데이터가 없습니다.</td></tr>`;
}

function renderAnalyticsManagerDetail(summary, teamAverages, months) {
  if (!summary) return;
  if ($("#analyticsManagerDetailTitle")) $("#analyticsManagerDetailTitle").textContent = `${summary.managerName} 실제 영업 상세분석`;
  const diagnosis = analyticsStrengthWeakness(summary, teamAverages, months);
  const topProduct = analyticsTopProductFamily(summary.managerName, months);
  const kpis = $("#analyticsManagerDetailKpis");
  if (kpis) kpis.innerHTML = `<article><span>분석개월</span><strong>${summary.average.months}</strong><em>${summary.analysisStartMonth ? `${formatMonthLabel(summary.analysisStartMonth)}부터` : "자료없음"}</em></article><article><span>평가 월평균</span><strong>${formatNumber(summary.average.actualEvaluated)}</strong><em>신규·패키지·재렌탈 기준</em></article><article><span>선택월 최종실적</span><strong>${formatNumber(summary.current.reportFinal)}</strong><em>접수 ${formatNumber(summary.current.actualPure)}건</em></article><article><span>환수</span><strong>${formatNumber(summary.current.refund)}</strong><em>최종실적에서 차감</em></article><article><span>주력 제품군</span><strong>${escapeHtml(topProduct?.family || "-")}</strong><em>${topProduct ? `${formatNumber(topProduct.units)}대` : "자료없음"}</em></article><article><span>최근 추세</span><strong>${summary.trendRate >= 0 ? "+" : ""}${Math.round(summary.trendRate)}%</strong><em>최근3개월 vs 이전3개월</em></article><article><span>컨스·지원 현황</span><strong>${formatNumber(summary.current.consCount + summary.current.supportCount)}</strong><em>컨스 ${formatNumber(summary.current.consCount)} · 지원 ${formatNumber(summary.current.supportCount)}</em></article>`;
  const bars = $("#analyticsManagerCompareBars");
  if (bars) bars.innerHTML = ["actualNew", "actualPackage", "actualRental"].map((key) => {
    const managerValue = summary.average[key];
    const teamValue = teamAverages[key];
    const max = Math.max(managerValue, teamValue, 1);
    return `<div class="analytics-compare-row"><strong>${analyticsCategoryLabel(key)}</strong><div class="analytics-compare-values"><span>본인 ${formatNumber(managerValue)}</span><span>지국평균 ${formatNumber(teamValue)}</span></div><div class="analytics-dual-track"><i style="width:${managerValue / max * 100}%"></i><b style="width:${teamValue / max * 100}%"></b></div></div>`;
  }).join("");
  if ($("#analyticsStrengthText")) $("#analyticsStrengthText").innerHTML = `<p>${escapeHtml(diagnosis.strength)}</p>`;
  if ($("#analyticsWeaknessText")) $("#analyticsWeaknessText").innerHTML = `<p>${escapeHtml(diagnosis.weakness)}</p>`;
  if ($("#analyticsManagerActionText")) $("#analyticsManagerActionText").innerHTML = `<p>${escapeHtml(diagnosis.action)}</p>`;
  const historyBody = $("#analyticsManagerHistoryBody");
  if (historyBody) historyBody.innerHTML = summary.history.map((item) => {
    const delta = item.actualEvaluated - summary.average.actualEvaluated;
    return `<tr><td>${formatMonthLabel(item.month)}</td><td>${analyticsStatusBadge(item.status)}</td><td>${formatNumber(item.reportFinal)}</td><td>${formatNumber(item.actualNew)}</td><td>${formatNumber(item.actualPackage)}</td><td>${formatNumber(item.actualRental)}</td><td>${formatNumber(item.actualCash)}</td><td class="pure-metric">${formatNumber(item.actualPure)}</td><td>${formatNumber(item.renewal)}</td><td class="refund-text">${formatNumber(item.refund)}</td><td>${formatNumber(item.consCount)}</td><td>${formatNumber(item.supportCount)}</td><td class="${delta >= 0 ? "positive" : "negative"}">${delta >= 0 ? "+" : ""}${formatNumber(delta)}</td></tr>`;
  }).join("") || `<tr><td colspan="13" class="empty">입력완료된 월 이력이 없습니다.</td></tr>`;
}

function renderAnalyticsProducts(months, entityName, entityNames) {
  const stats = analyticsProductStats(months, entityName);
  if ($("#analyticsProductScope")) $("#analyticsProductScope").textContent = `${entityName || "지국 전체"} · 실제실적 기준`;
  const topFamily = stats.families[0];
  const summary = $("#analyticsProductSummaryCards");
  if (summary) summary.innerHTML = `<article><span>순수 계약건수</span><strong>${formatNumber(stats.totalContracts)}</strong><em>패키지는 계약 1건 기준</em></article><article><span>판매제품 수량</span><strong>${formatNumber(stats.totalUnits)}</strong><em>복수제품은 각각 집계</em></article><article><span>판매 제품군</span><strong>${stats.families.length}</strong><em>미분류 포함</em></article><article><span>주력 제품군</span><strong>${escapeHtml(topFamily?.family || "-")}</strong><em>${topFamily ? `${formatNumber(topFamily.units)}대` : "자료없음"}</em></article>`;
  const familyBody = $("#analyticsProductFamilyBody");
  if (familyBody) familyBody.innerHTML = stats.families.map((item) => {
    const topModels = [...item.models.entries()].sort((a, b) => b[1] - a[1]).slice(0, 3).map(([name]) => name).join(", ");
    const rate = stats.totalUnits > 0 ? item.units / stats.totalUnits * 100 : 0;
    return `<tr><td><strong>${escapeHtml(item.family)}</strong></td><td>${formatNumber(item.contracts)}</td><td>${formatNumber(item.units)}</td><td>${formatNumber(stats.validMonthCount ? item.units / stats.validMonthCount : 0)}</td><td>${Math.round(rate)}%</td><td>${escapeHtml(topModels || "-")}</td></tr>`;
  }).join("") || `<tr><td colspan="6" class="empty">제품분석 데이터가 없습니다.</td></tr>`;
  const allStats = analyticsProductStats(months, "");
  const topFamilies = allStats.families.slice(0, 8).map((item) => item.family);
  const matrixHead = $("#analyticsProductMatrixHead");
  const matrixBody = $("#analyticsProductMatrixBody");
  if (matrixHead && matrixBody) {
    matrixHead.innerHTML = `<tr><th>실제 판매자</th>${topFamilies.map((family) => `<th>${escapeHtml(family)}</th>`).join("")}<th>합계</th></tr>`;
    matrixBody.innerHTML = entityNames.map((name) => {
      const sellerStats = analyticsProductStats(months, name);
      const map = new Map(sellerStats.families.map((item) => [item.family, item.units]));
      return `<tr><td><strong>${escapeHtml(name)}</strong></td>${topFamilies.map((family) => `<td>${formatNumber(map.get(family) || 0)}</td>`).join("")}<td class="pure-metric">${formatNumber(sellerStats.totalUnits)}</td></tr>`;
    }).join("") || `<tr><td colspan="${topFamilies.length + 2}" class="empty">실판매자 제품 데이터가 없습니다.</td></tr>`;
  }
  const modelBody = $("#analyticsProductModelBody");
  if (modelBody) modelBody.innerHTML = stats.models.slice(0, 15).map((item, index) => `<tr><td>${index + 1}</td><td>${escapeHtml(item.family)}</td><td class="model-name-cell" title="${escapeHtml(item.model)}">${escapeHtml(item.model)}</td><td>${formatNumber(item.units)}</td><td>${formatNumber(item.contracts)}</td></tr>`).join("") || `<tr><td colspan="5" class="empty">판매모델 데이터가 없습니다.</td></tr>`;
}

function renderAnalyticsActions(recommendations) {
  analyticsRecommendationCache = recommendations;
  const summary = $("#analyticsActionSummary");
  if (summary) {
    const urgent = recommendations.filter((item) => item.priority === "긴급").length;
    const groups = new Set(recommendations.map((item) => item.scope));
    summary.innerHTML = `<article><span>전체 제안</span><strong>${recommendations.length}</strong></article><article class="danger"><span>긴급 점검</span><strong>${urgent}</strong></article><article><span>구분된 대상</span><strong>${groups.size}</strong></article>`;
  }
  const list = $("#analyticsActionList");
  if (!list) return;
  if (!recommendations.length) {
    list.innerHTML = `<div class="empty analytics-empty">현재 즉시 실행이 필요한 제안이 없습니다.</div>`;
    return;
  }
  const grouped = new Map();
  recommendations.forEach((item) => {
    const key = item.scope || "기타";
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(item);
  });
  const order = [...grouped.keys()].sort((a, b) => {
    if (a === "지국 전체") return -1;
    if (b === "지국 전체") return 1;
    return a.localeCompare(b, "ko");
  });
  list.innerHTML = order.map((scope) => {
    const items = grouped.get(scope) || [];
    return `<section class="analytics-action-manager-group">
      <div class="analytics-action-manager-head">
        <h3>${escapeHtml(scope)}</h3>
        <span>${items.length}개 실행제안</span>
      </div>
      <div class="analytics-action-manager-items">
        ${items.map((item) => `<article class="analytics-action-card ${item.priority === "긴급" ? "urgent" : ""}"><div class="analytics-action-priority">${escapeHtml(item.priority)}</div><div><h3>${escapeHtml(item.title)}</h3><p>${escapeHtml(item.detail)}</p></div><button class="primary-button small" type="button" data-analytics-task="${escapeHtml(item.id)}">업무로 등록</button></article>`).join("")}
      </div>
    </section>`;
  }).join("");
}



function renderAnalyticsCombinedReport(selectedEntity, months, summaries, teamAverages, recommendations) {
  const empty = $("#analyticsCombinedEmpty");
  const content = $("#analyticsCombinedContent");
  const printButton = $("#analyticsCombinedPrintBtn");
  const printTitle = $("#analyticsCombinedPrintTitle");
  if (!empty || !content) return;

  if (!selectedEntity) {
    empty.hidden = false;
    content.hidden = true;
    content.innerHTML = "";
    if (printButton) printButton.disabled = true;
    if (printTitle) printTitle.textContent = "매니저 통합분석 보고서";
    return;
  }

  const summary = summaries.find((item) => item.managerName === selectedEntity);
  if (!summary) {
    empty.hidden = false;
    empty.textContent = "선택한 매니저의 분석자료가 없습니다.";
    content.hidden = true;
    if (printButton) printButton.disabled = true;
    return;
  }

  const { start, end } = analyticsRangeValues();
  const current = summary.current;
  const diagnosis = analyticsStrengthWeakness(summary, teamAverages, months);
  const productStats = analyticsProductStats(months, selectedEntity);
  const topFamily = productStats.families[0];
  const managerActions = recommendations.filter((item) => item.scope === selectedEntity);
  const achievement = current.goal > 0 ? current.reportFinal / current.goal * 100 : 0;

  const monthlyRows = months.map((month) => {
    const item = analyticsMonthlyMetrics(month, selectedEntity);
    const unavailable = ["미입력", "분석제외", "예정"].includes(item.status);
    const value = (number) => unavailable ? "-" : formatNumber(number);
    return `<tr>
      <td>${formatMonthLabel(month)}${analyticsStatusBadge(item.status)}</td>
      <td>${value(item.reportBusiness)}</td>
      <td>${value(item.renewal)}</td>
      <td class="refund-text">${value(item.refund)}</td>
      <td class="analytics-report-number">${value(item.reportFinal)}</td>
      <td>${value(item.actualNew)}</td>
      <td>${value(item.actualPackage)}</td>
      <td>${value(item.actualRental)}</td>
      <td>${value(item.actualCash)}</td>
      <td class="pure-metric">${value(item.actualPure)}</td>
      <td>${value(item.consCount)}</td>
      <td>${value(item.supportCount)}</td>
    </tr>`;
  }).join("");

  const familyRows = productStats.families.slice(0, 10).map((item) => {
    const rate = productStats.totalUnits > 0 ? item.units / productStats.totalUnits * 100 : 0;
    const models = [...item.models.entries()].sort((a, b) => b[1] - a[1]).slice(0, 3).map(([name]) => name).join(", ");
    return `<tr><td><strong>${escapeHtml(item.family)}</strong></td><td>${formatNumber(item.contracts)}</td><td>${formatNumber(item.units)}</td><td>${Math.round(rate)}%</td><td>${escapeHtml(models || "-")}</td></tr>`;
  }).join("") || `<tr><td colspan="5" class="empty">판매제품 자료가 없습니다.</td></tr>`;

  const modelRows = productStats.models.slice(0, 10).map((item, index) =>
    `<tr><td>${index + 1}</td><td>${escapeHtml(item.family)}</td><td>${escapeHtml(item.model)}</td><td>${formatNumber(item.units)}</td></tr>`
  ).join("") || `<tr><td colspan="4" class="empty">판매모델 자료가 없습니다.</td></tr>`;

  const actionRows = managerActions.length
    ? managerActions.map((item) => `<article class="analytics-action-card ${item.priority === "긴급" ? "urgent" : ""}">
        <div class="analytics-action-priority">${escapeHtml(item.priority)}</div>
        <div><h3>${escapeHtml(item.title)}</h3><p>${escapeHtml(item.detail)}</p></div>
      </article>`).join("")
    : `<div class="empty analytics-empty">현재 이 매니저에게 생성된 실행제안이 없습니다.</div>`;

  content.innerHTML = `
    <section class="panel analytics-combined-section analytics-combined-cover">
      <div class="panel-head">
        <h2>${escapeHtml(selectedEntity)} 매니저 통합분석</h2>
        <span>${formatMonthLabel(start)} ~ ${formatMonthLabel(end)}</span>
      </div>
      <div class="analytics-kpi-grid analytics-kpi-grid-six">
        <article class="analytics-kpi-card report"><span>선택월 최종실적</span><strong>${formatNumber(current.reportFinal)}</strong><em>접수+재약정−환수</em></article>
        <article class="analytics-kpi-card pure"><span>실제 접수실적</span><strong>${formatNumber(current.actualPure)}</strong><em>신규·패키지·재렌탈·일시불</em></article>
        <article class="analytics-kpi-card"><span>재약정</span><strong>${formatNumber(current.renewal)}</strong><em>최종실적에 가산</em></article>
        <article class="analytics-kpi-card danger"><span>환수</span><strong>${formatNumber(current.refund)}</strong><em>최종실적에서 차감</em></article>
        <article class="analytics-kpi-card goal"><span>공식목표 달성률</span><strong>${formatNumber(achievement)}%</strong><em>목표 ${formatNumber(current.goal)}건</em></article>
        <article class="analytics-kpi-card"><span>평가 월평균</span><strong>${formatNumber(summary.average.actualEvaluated)}</strong><em>${summary.average.months}개월 기준</em></article>
      </div>
      <div class="analytics-combined-category-grid">
        <article><span>신규</span><strong>${formatNumber(current.actualNew)}</strong></article>
        <article><span>패키지</span><strong>${formatNumber(current.actualPackage)}</strong></article>
        <article><span>재렌탈</span><strong>${formatNumber(current.actualRental)}</strong></article>
        <article><span>일시불</span><strong>${formatNumber(current.actualCash)}</strong></article>
        <article><span>컨스</span><strong>${formatNumber(current.consCount)}</strong></article>
        <article><span>지원</span><strong>${formatNumber(current.supportCount)}</strong></article>
      </div>
    </section>

    <section class="panel analytics-combined-section">
      <div class="panel-head"><h2>월별현황</h2><span>최종실적에 환수 차감 반영</span></div>
      <div class="table-wrap"><table class="analytics-table analytics-combined-monthly-table">
        <thead><tr><th>월</th><th>접수보고</th><th>재약정</th><th>환수</th><th>최종실적</th><th>신규</th><th>패키지</th><th>재렌탈</th><th>일시불</th><th>실제접수</th><th>컨스</th><th>지원</th></tr></thead>
        <tbody>${monthlyRows}</tbody>
      </table></div>
    </section>

    <section class="panel analytics-combined-section">
      <div class="panel-head"><h2>매니저분석</h2><span>${escapeHtml(analyticsManagerType(summary, teamAverages))}</span></div>
      <div class="analytics-detail-kpis">
        <article><span>분석 시작</span><strong>${summary.analysisStartMonth ? formatMonthLabel(summary.analysisStartMonth) : "-"}</strong><em>${summary.average.months}개월 분석</em></article>
        <article><span>최근 추세</span><strong>${summary.trendRate >= 0 ? "+" : ""}${Math.round(summary.trendRate)}%</strong><em>최근3개월 대비</em></article>
        <article><span>주력 제품군</span><strong>${escapeHtml(topFamily?.family || "-")}</strong><em>${topFamily ? `${formatNumber(topFamily.units)}대` : "자료없음"}</em></article>
      </div>
      <div class="analytics-strength-grid">
        <article><h3>잘하고 있는 부분</h3><p>${escapeHtml(diagnosis.strength)}</p></article>
        <article><h3>보완이 필요한 부분</h3><p>${escapeHtml(diagnosis.weakness)}</p></article>
        <article><h3>추천 실행방향</h3><p>${escapeHtml(diagnosis.action)}</p></article>
      </div>
    </section>

    <section class="panel analytics-combined-section">
      <div class="panel-head"><h2>판매제품분석</h2><span>0.5 실적도 실제 제품은 1대로 계산</span></div>
      <div class="analytics-product-summary">
        <article><span>계약건수</span><strong>${formatNumber(productStats.totalContracts)}</strong><em>제품 기준 정수 집계</em></article>
        <article><span>판매제품 수량</span><strong>${formatNumber(productStats.totalUnits)}</strong><em>복수제품 각각 집계</em></article>
        <article><span>제품군</span><strong>${productStats.families.length}</strong><em>미분류 포함</em></article>
        <article><span>주력 제품군</span><strong>${escapeHtml(topFamily?.family || "-")}</strong><em>${topFamily ? `${formatNumber(topFamily.units)}대` : "자료없음"}</em></article>
      </div>
      <div class="analytics-combined-product-grid">
        <div class="table-wrap"><table class="analytics-table"><thead><tr><th>제품군</th><th>계약건</th><th>제품수량</th><th>비중</th><th>주요 모델</th></tr></thead><tbody>${familyRows}</tbody></table></div>
        <div class="table-wrap"><table class="analytics-table"><thead><tr><th>순위</th><th>제품군</th><th>모델</th><th>수량</th></tr></thead><tbody>${modelRows}</tbody></table></div>
      </div>
    </section>

    <section class="panel analytics-combined-section">
      <div class="panel-head"><h2>실행제안</h2><span>${managerActions.length}개</span></div>
      <div class="analytics-action-manager-items">${actionRows}</div>
    </section>
  `;

  empty.hidden = true;
  content.hidden = false;
  if (printButton) {
    printButton.disabled = false;
    printButton.dataset.printTitle = `${selectedEntity} 매니저 통합분석`;
  }
  if (printTitle) printTitle.textContent = `${selectedEntity} 매니저 통합분석 보고서`;
}



function splitAnalyticsPrintTableBlock(node, maxRows = 16) {
  if (!node) return [];
  const tables = [...node.querySelectorAll("table")];
  if (tables.length !== 1) return [node];

  const rows = [...tables[0].querySelectorAll("tbody tr")];
  if (rows.length <= maxRows) return [node];

  const blocks = [];
  for (let startIndex = 0; startIndex < rows.length; startIndex += maxRows) {
    const copy = node.cloneNode(true);
    const copyRows = [...copy.querySelectorAll("tbody tr")];
    copyRows.forEach((row, rowIndex) => {
      if (rowIndex < startIndex || rowIndex >= startIndex + maxRows) row.remove();
    });

    const title = copy.querySelector(".panel-head h2");
    if (title) {
      const pageNumber = Math.floor(startIndex / maxRows) + 1;
      const totalPages = Math.ceil(rows.length / maxRows);
      title.textContent = `${title.textContent} (${pageNumber}/${totalPages})`;
    }

    blocks.push(copy);
  }

  return blocks;
}

function splitAnalyticsPrintActionBlock(node, maxCards = 8) {
  if (!node) return [];
  const cards = [...node.querySelectorAll(".analytics-action-card")];
  if (cards.length <= maxCards) return [node];

  const blocks = [];
  for (let startIndex = 0; startIndex < cards.length; startIndex += maxCards) {
    const copy = node.cloneNode(true);
    const copyCards = [...copy.querySelectorAll(".analytics-action-card")];
    copyCards.forEach((card, cardIndex) => {
      if (cardIndex < startIndex || cardIndex >= startIndex + maxCards) card.remove();
    });

    const title = copy.querySelector(".panel-head h2");
    if (title) {
      const pageNumber = Math.floor(startIndex / maxCards) + 1;
      const totalPages = Math.ceil(cards.length / maxCards);
      title.textContent = `${title.textContent} (${pageNumber}/${totalPages})`;
    }

    blocks.push(copy);
  }

  return blocks;
}

function analyticsPrintBlockWeight(node) {
  if (!node) return 0;
  const rows = node.querySelectorAll("tbody tr").length;
  const cards = node.querySelectorAll(
    ".analytics-kpi-card, .analytics-product-summary article, .analytics-detail-kpis article, .analytics-strength-grid article, .analytics-action-card"
  ).length;
  const charts = node.querySelectorAll("canvas, img.analytics-print-chart").length;
  const textLength = String(node.textContent || "").trim().length;
  return 1.5 + rows * 0.42 + cards * 0.48 + charts * 3 + Math.min(2.5, textLength / 900);
}

function buildAnalyticsPrintPages(clone, meta = {}) {
  const {
    reportTitle = "영업분석",
    branch = "",
    entity = "지국 전체",
    start = "",
    end = "",
    printedAt = ""
  } = meta;

  const groups = [];
  const combinedContent = clone.querySelector(".analytics-combined-content");

  if (combinedContent) {
    const sections = [...combinedContent.children]
      .filter((node) => node instanceof HTMLElement)
      .filter((node) => !node.hidden)
      .filter((node) => !node.classList.contains("analytics-combined-empty"));

    const [cover, monthly, managerAnalysis, productAnalysis, actions] = sections;
    const monthlyRows = monthly?.querySelectorAll("tbody tr").length || 0;
    const productRows = productAnalysis?.querySelectorAll("tbody tr").length || 0;
    const actionCards = actions?.querySelectorAll(".analytics-action-card").length || 0;

    const onePageEligible = Boolean(
      cover
      && monthly
      && managerAnalysis
      && productAnalysis
      && monthlyRows <= 8
      && productRows <= 24
      && actionCards <= 4
    );

    if (onePageEligible) {
      cover.classList.add("report-cover-section");
      monthly.classList.add("report-monthly-section");
      managerAnalysis.classList.add("report-manager-section");
      productAnalysis.classList.add("report-product-section");

      const nodes = [cover, monthly, managerAnalysis, productAnalysis];

      if (actions) {
        actions.classList.add("report-actions-section");
        nodes.push(actions);
      }

      groups.push({
        layout: "combined-full-layout",
        nodes
      });
    } else {
      const monthlyBlocks = monthly ? splitAnalyticsPrintTableBlock(monthly, 15) : [];
      const actionBlocks = actions ? splitAnalyticsPrintActionBlock(actions, 8) : [];

      if (cover || monthlyBlocks.length) {
        groups.push({
          layout: "summary-layout",
          nodes: [cover, monthlyBlocks[0]].filter(Boolean)
        });

        monthlyBlocks.slice(1).forEach((block) => {
          groups.push({
            layout: "single-layout",
            nodes: [block]
          });
        });
      }

      if (managerAnalysis || productAnalysis) {
        const comparisonNodes = [managerAnalysis, productAnalysis].filter(Boolean);

        if (actionBlocks.length === 1 && actionCards <= 3) {
          actionBlocks[0].classList.add("report-full-width");
          comparisonNodes.push(actionBlocks[0]);
        }

        groups.push({
          layout: "compare-layout",
          nodes: comparisonNodes
        });

        if (actionBlocks.length && !comparisonNodes.includes(actionBlocks[0])) {
          actionBlocks.forEach((block) => {
            groups.push({
              layout: "action-layout",
              nodes: [block]
            });
          });
        }
      } else if (actionBlocks.length) {
        actionBlocks.forEach((block) => {
          groups.push({
            layout: "action-layout",
            nodes: [block]
          });
        });
      }
    }
  } else {
    const topLevel = [...clone.children]
      .filter((node) => node instanceof HTMLElement)
      .filter((node) => !node.hidden)
      .filter((node) => !node.classList.contains("analytics-panel-printbar"))
      .filter((node) => !node.classList.contains("analytics-combined-empty"));

    const candidates = (topLevel.length ? topLevel : [clone]).flatMap((node) => {
      if (node.querySelectorAll(".analytics-action-card").length > 8) {
        return splitAnalyticsPrintActionBlock(node, 8);
      }
      return splitAnalyticsPrintTableBlock(node, 16);
    });

    let current = [];
    let currentWeight = 0;
    const maxWeight = 15;

    candidates.forEach((node) => {
      const weight = analyticsPrintBlockWeight(node);
      const hasLargeTable = node.querySelectorAll("tbody tr").length >= 10;
      if (hasLargeTable) node.classList.add("report-full-width");

      if (current.length && currentWeight + weight > maxWeight) {
        groups.push({
          layout: current.length > 1 ? "generic-layout" : "single-layout",
          nodes: current
        });
        current = [];
        currentWeight = 0;
      }

      current.push(node);
      currentWeight += weight;
    });

    if (current.length) {
      groups.push({
        layout: current.length > 1 ? "generic-layout" : "single-layout",
        nodes: current
      });
    }
  }

  if (!groups.length) {
    groups.push({ layout: "single-layout", nodes: [clone] });
  }

  const wrapper = document.createElement("div");
  wrapper.className = "analytics-report-pages";
  const totalPages = groups.length;
  const periodText = `${formatMonthLabel(start)} ~ ${formatMonthLabel(end)}`;

  groups.forEach((group, index) => {
    const page = document.createElement("section");
    page.className = `analytics-report-page ${group.layout}`;

    const header = document.createElement("header");
    header.className = "report-page-header";
    header.innerHTML = `
      <div class="report-page-title">
        <strong>${escapeHtml(reportTitle)}</strong>
        <span>${escapeHtml(branch)}</span>
      </div>
      <div class="report-page-meta">
        <strong>분석대상: ${escapeHtml(entity)}</strong>
        <span>${escapeHtml(periodText)}</span>
      </div>`;

    const body = document.createElement("main");
    body.className = "analytics-report-page-body";
    group.nodes.forEach((node) => {
      if (node) body.appendChild(node);
    });

    const footer = document.createElement("footer");
    footer.className = "report-page-footer";
    footer.innerHTML = `
      <span>분석대상: ${escapeHtml(entity)}</span>
      <span>${escapeHtml(reportTitle)} · ${escapeHtml(periodText)}</span>
      <strong>${index + 1} / ${totalPages}</strong>`;

    page.appendChild(header);
    page.appendChild(body);
    page.appendChild(footer);
    wrapper.appendChild(page);
  });

  wrapper.dataset.printedAt = printedAt;
  return wrapper.outerHTML;
}
function printAnalyticsPanel(panelId, reportTitle = "영업분석") {
  const source = document.getElementById(panelId);
  if (!source) {
    showToast("출력할 분석화면을 찾지 못했습니다.");
    return;
  }

  const clone = source.cloneNode(true);
  clone.classList.add("analytics-print-source");
  clone.querySelectorAll(".analytics-panel-printbar, button").forEach((node) => node.remove());

  const sourceCanvases = [...source.querySelectorAll("canvas")];
  [...clone.querySelectorAll("canvas")].forEach((canvas, index) => {
    const image = document.createElement("img");
    image.className = "analytics-print-chart";
    try {
      image.src = sourceCanvases[index]?.toDataURL("image/png") || "";
    } catch (error) {
      image.alt = "그래프";
    }
    canvas.replaceWith(image);
  });

  clone.querySelectorAll("select").forEach((select) => {
    const text = document.createElement("span");
    text.className = "analytics-print-selected-value";
    text.textContent = select.options?.[select.selectedIndex]?.textContent || select.value || "";
    select.replaceWith(text);
  });

  clone.querySelectorAll("input, textarea").forEach((input) => {
    const text = document.createElement("span");
    text.className = "analytics-print-selected-value";
    text.textContent = input.value || "";
    input.replaceWith(text);
  });

  const { start, end } = analyticsRangeValues();
  const entity = $("#analyticsManagerFilter")?.value || "지국 전체";
  const meta = state.appMeta || sampleState.appMeta;
  const branch = `${meta.branchName || "SK지국"} ${meta.masterName || "사용자"} ${meta.masterRole || "담당자"}`;
  const printedAt = new Date().toLocaleString("ko-KR");

  const printPages = buildAnalyticsPrintPages(clone, {
    reportTitle,
    branch,
    entity,
    start,
    end,
    printedAt
  });

  const oldFrame = document.getElementById("analytics-print-frame");
  if (oldFrame) oldFrame.remove();

  const iframe = document.createElement("iframe");
  iframe.id = "analytics-print-frame";
  iframe.style.position = "fixed";
  iframe.style.right = "0";
  iframe.style.bottom = "0";
  iframe.style.width = "0";
  iframe.style.height = "0";
  iframe.style.border = "0";
  document.body.appendChild(iframe);

  const doc = iframe.contentWindow.document;
  doc.open();
  doc.write(`<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>${escapeHtml(reportTitle)}</title>
<style>
  @page {
    size: A4 landscape;
    margin: 0;
  }

  * {
    box-sizing: border-box;
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
  }

  html,
  body {
    margin: 0;
    padding: 0;
    color: #111;
    background: #fff;
    font-family: Arial, "Malgun Gothic", sans-serif;
  }

  body {
    font-size: 8.1pt;
    line-height: 1.2;
  }

  .analytics-report-pages {
    width: 100%;
  }

  .analytics-report-page {
    position: relative;
    width: 297mm;
    height: 210mm;
    padding: 5.5mm 6.5mm 9mm;
    overflow: hidden;
    background: #fff;
    break-after: page;
    page-break-after: always;
  }

  .analytics-report-page:last-child {
    break-after: auto;
    page-break-after: auto;
  }

  .report-page-header {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    gap: 4mm;
    align-items: end;
    height: 11mm;
    padding: 0 0 2mm;
    margin: 0 0 2.2mm;
    border-bottom: 1.6px solid #183d2e;
  }

  .report-page-title strong,
  .report-page-title span,
  .report-page-meta strong,
  .report-page-meta span {
    display: block;
  }

  .report-page-title strong {
    color: #10261e;
    font-size: 14pt;
    font-weight: 950;
  }

  .report-page-title span,
  .report-page-meta span {
    margin-top: .6mm;
    color: #42574e;
    font-size: 7.2pt;
    font-weight: 750;
  }

  .report-page-meta {
    text-align: right;
  }

  .report-page-meta strong {
    color: #183d2e;
    font-size: 8.4pt;
    font-weight: 950;
  }

  .analytics-report-page-body {
    display: grid !important;
    gap: 2.2mm !important;
    align-items: start;
    height: 180mm;
    overflow: hidden;
  }

  .summary-layout .analytics-report-page-body {
    grid-template-columns: minmax(77mm, .78fr) minmax(0, 1.62fr);
  }

  .compare-layout .analytics-report-page-body {
    grid-template-columns: minmax(78mm, .82fr) minmax(0, 1.58fr);
  }

  .combined-full-layout .analytics-report-page-body {
    grid-template-columns: 88mm minmax(0, 1fr);
    grid-template-areas:
      "cover monthly"
      "manager product"
      "actions actions";
    grid-template-rows: auto auto auto;
    gap: 1.5mm !important;
    align-content: start;
  }

  .combined-full-layout .report-cover-section {
    grid-area: cover;
  }

  .combined-full-layout .report-monthly-section {
    grid-area: monthly;
  }

  .combined-full-layout .report-manager-section {
    grid-area: manager;
  }

  .combined-full-layout .report-product-section {
    grid-area: product;
  }

  .combined-full-layout .report-actions-section {
    grid-area: actions;
  }

  .combined-full-layout .panel-head {
    min-height: 5.8mm !important;
    padding: .75mm 1.4mm !important;
  }

  .combined-full-layout .panel-head h2 {
    font-size: 8.4pt !important;
  }

  .combined-full-layout .analytics-kpi-grid,
  .combined-full-layout .analytics-product-summary,
  .combined-full-layout .analytics-detail-kpis {
    gap: .8mm !important;
    padding: 1mm !important;
  }

  .combined-full-layout .analytics-kpi-card,
  .combined-full-layout .analytics-product-summary article,
  .combined-full-layout .analytics-detail-kpis article {
    padding: .9mm .65mm !important;
  }

  .combined-full-layout .analytics-kpi-card strong,
  .combined-full-layout .analytics-product-summary strong,
  .combined-full-layout .analytics-detail-kpis strong {
    font-size: 9.6pt !important;
  }

  .combined-full-layout .analytics-combined-category-grid,
  .combined-full-layout .analytics-strength-grid,
  .combined-full-layout .analytics-combined-product-grid {
    gap: .8mm !important;
    padding: 0 1mm 1mm !important;
  }

  .combined-full-layout .analytics-strength-grid article {
    padding: 1mm !important;
  }

  .combined-full-layout .analytics-strength-grid h3 {
    margin-bottom: .35mm !important;
    font-size: 6.8pt !important;
  }

  .combined-full-layout .analytics-strength-grid p {
    font-size: 6.2pt !important;
    line-height: 1.16 !important;
  }

  .combined-full-layout th,
  .combined-full-layout td {
    padding: .55mm .3mm !important;
    font-size: 6.35pt !important;
  }

  .combined-full-layout th {
    font-size: 6.1pt !important;
  }

  .combined-full-layout .analytics-action-manager-items {
    grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
    gap: .8mm !important;
    padding: 1mm !important;
  }

  .combined-full-layout .analytics-action-card {
    padding: 1mm !important;
  }

  .combined-full-layout .analytics-action-card h3 {
    font-size: 6.7pt !important;
  }

  .combined-full-layout .analytics-action-card p {
    font-size: 6pt !important;
    line-height: 1.12 !important;
  }

  .generic-layout .analytics-report-page-body {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .single-layout .analytics-report-page-body,
  .action-layout .analytics-report-page-body {
    grid-template-columns: 1fr;
  }

  .report-full-width {
    grid-column: 1 / -1 !important;
  }

  .report-page-footer {
    position: absolute;
    left: 6.5mm;
    right: 6.5mm;
    bottom: 3.4mm;
    display: grid;
    grid-template-columns: 1fr 1.6fr auto;
    gap: 4mm;
    align-items: center;
    padding-top: 1.5mm;
    border-top: 1px solid #87978f;
    color: #40534b;
    font-size: 7pt;
    font-weight: 750;
  }

  .report-page-footer span:nth-child(2) {
    text-align: center;
  }

  .report-page-footer strong {
    min-width: 18mm;
    text-align: right;
    color: #153d2c;
    font-size: 8pt;
    font-weight: 950;
  }

  .analytics-panel,
  .analytics-combined-content {
    display: contents !important;
  }

  .analytics-combined-empty[hidden],
  .analytics-combined-content[hidden] {
    display: none !important;
  }

  .panel,
  .analytics-kpi-card,
  .analytics-basis-note,
  .analytics-action-manager-group {
    min-width: 0 !important;
    border: 1px solid #46564e !important;
    border-radius: 2px !important;
    background: #fff !important;
    box-shadow: none !important;
    color: #111 !important;
  }

  .panel {
    margin: 0 !important;
    padding: 0 !important;
    overflow: hidden !important;
  }

  .panel-head {
    display: flex !important;
    justify-content: space-between !important;
    align-items: center !important;
    gap: 2mm !important;
    min-height: 7mm !important;
    padding: 1.2mm 2mm !important;
    border-bottom: 1px solid #46564e !important;
    background: #e8efeb !important;
  }

  .panel-head h2 {
    flex: 1;
    min-width: 0;
    margin: 0 !important;
    color: #111 !important;
    font-size: 9.3pt !important;
    font-weight: 950 !important;
    text-align: left !important;
  }

  .panel-head span,
  .panel-head strong {
    margin: 0 !important;
    color: #31463c !important;
    font-size: 7pt !important;
    font-weight: 850 !important;
    white-space: nowrap;
  }

  .analytics-kpi-grid,
  .analytics-product-summary,
  .analytics-action-summary,
  .analytics-detail-kpis {
    display: grid !important;
    gap: 1.3mm !important;
    margin: 0 !important;
    padding: 1.5mm !important;
  }

  .analytics-kpi-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr)) !important;
  }

  .analytics-kpi-grid-six {
    grid-template-columns: repeat(3, minmax(0, 1fr)) !important;
  }

  .analytics-product-summary {
    grid-template-columns: repeat(4, minmax(0, 1fr)) !important;
  }

  .analytics-detail-kpis {
    grid-template-columns: repeat(3, minmax(0, 1fr)) !important;
  }

  .analytics-kpi-card,
  .analytics-product-summary article,
  .analytics-action-summary article,
  .analytics-detail-kpis article {
    min-width: 0 !important;
    padding: 1.4mm 1mm !important;
    border: 1px solid #718078 !important;
    background: #fff !important;
    text-align: center !important;
  }

  .analytics-kpi-card span,
  .analytics-product-summary span,
  .analytics-detail-kpis span {
    display: block !important;
    color: #33473e !important;
    font-size: 6.6pt !important;
    font-weight: 850 !important;
    white-space: nowrap;
  }

  .analytics-kpi-card strong,
  .analytics-product-summary strong,
  .analytics-detail-kpis strong {
    display: block !important;
    margin: .5mm 0 !important;
    color: #000 !important;
    font-size: 10.9pt !important;
    line-height: 1 !important;
    font-weight: 950 !important;
  }

  .analytics-kpi-card em,
  .analytics-product-summary em,
  .analytics-detail-kpis em {
    display: block !important;
    color: #55665e !important;
    font-size: 6pt !important;
    line-height: 1.1 !important;
    font-style: normal !important;
  }

  .analytics-combined-category-grid {
    display: grid !important;
    grid-template-columns: repeat(3, minmax(0, 1fr)) !important;
    gap: 1.2mm !important;
    padding: 0 1.5mm 1.5mm !important;
  }

  .analytics-combined-category-grid article {
    min-width: 0 !important;
    padding: 1mm !important;
    border: 1px solid #718078 !important;
    background: #f8faf9 !important;
    text-align: center !important;
  }

  .analytics-combined-category-grid span {
    display: block !important;
    font-size: 6.2pt !important;
    font-weight: 800 !important;
  }

  .analytics-combined-category-grid strong {
    display: block !important;
    margin-top: .4mm !important;
    font-size: 9.5pt !important;
    font-weight: 950 !important;
  }

  .analytics-overview-grid,
  .analytics-product-grid,
  .analytics-manager-detail-grid {
    display: grid !important;
    grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
    gap: 2mm !important;
  }

  .analytics-strength-grid {
    display: grid !important;
    grid-template-columns: 1fr !important;
    gap: 1.2mm !important;
    padding: 0 1.5mm 1.5mm !important;
    margin: 0 !important;
  }

  .analytics-strength-grid article {
    min-width: 0 !important;
    padding: 1.4mm !important;
    border: 1px solid #718078 !important;
    background: #fff !important;
  }

  .analytics-strength-grid h3 {
    margin: 0 0 .7mm !important;
    color: #173c2d !important;
    font-size: 7.2pt !important;
    font-weight: 950 !important;
  }

  .analytics-strength-grid p {
    margin: 0 !important;
    font-size: 6.8pt !important;
    line-height: 1.25 !important;
  }

  .analytics-basis-note {
    display: flex !important;
    gap: 1.5mm !important;
    margin: 0 !important;
    padding: 1.5mm !important;
  }

  .analytics-diagnosis-list,
  .analytics-category-mix,
  .analytics-compare-bars {
    padding: 1.5mm !important;
  }

  .analytics-diagnosis-item {
    display: grid !important;
    grid-template-columns: 6mm minmax(0, 1fr) !important;
    gap: 1mm !important;
    align-items: start !important;
    padding: .8mm 0 !important;
    border-bottom: 1px solid #9aa69f !important;
  }

  .analytics-diagnosis-item p {
    margin: 0 !important;
    font-size: 6.8pt !important;
    line-height: 1.2 !important;
  }

  .table-wrap,
  .compact-model-table-wrap {
    width: 100% !important;
    min-width: 0 !important;
    overflow: hidden !important;
    max-height: none !important;
  }

  table {
    width: 100% !important;
    min-width: 0 !important;
    border-collapse: collapse !important;
    table-layout: fixed !important;
    color: #111 !important;
    background: #fff !important;
  }

  thead {
    display: table-header-group;
  }

  tfoot {
    display: table-footer-group;
  }

  tr {
    break-inside: avoid;
    page-break-inside: avoid;
  }

  th,
  td {
    min-width: 0 !important;
    border: .8px solid #56635d !important;
    padding: .75mm .45mm !important;
    color: #111 !important;
    background: #fff !important;
    font-size: 6.9pt !important;
    line-height: 1.08 !important;
    text-align: center !important;
    vertical-align: middle !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
    white-space: nowrap !important;
    font-variant-numeric: tabular-nums;
  }

  th {
    background: #dce6e1 !important;
    font-size: 6.6pt !important;
    font-weight: 950 !important;
  }

  td {
    font-weight: 750 !important;
  }

  td strong {
    color: #000 !important;
  }

  .analytics-combined-monthly-table th:first-child,
  .analytics-combined-monthly-table td:first-child {
    width: 15mm !important;
  }

  .analytics-combined-monthly-table th,
  .analytics-combined-monthly-table td {
    padding-left: .28mm !important;
    padding-right: .28mm !important;
    font-size: 6.25pt !important;
  }

  .analytics-report-number,
  .analytics-actual-number,
  .analytics-difference-number {
    font-size: 7.5pt !important;
    font-weight: 950 !important;
  }

  .analytics-matrix-number strong {
    display: block !important;
    color: #123f6f !important;
    font-size: 7.4pt !important;
    font-weight: 950 !important;
  }

  .analytics-matrix-number small {
    display: block !important;
    margin-top: .35mm !important;
    color: #075f3b !important;
    font-size: 6.7pt !important;
    font-weight: 950 !important;
  }

  .analytics-data-status,
  .analytics-type-badge {
    padding: .2mm .7mm !important;
    border: 1px solid #4c5852 !important;
    background: #fff !important;
    color: #111 !important;
    font-size: 5.8pt !important;
  }

  .analytics-print-chart {
    display: block;
    width: 100%;
    max-height: 66mm;
    object-fit: contain;
  }

  .analytics-chart-legend {
    display: flex !important;
    justify-content: center !important;
    gap: 3mm !important;
    padding: 1mm !important;
    font-size: 6.5pt !important;
  }

  .analytics-combined-product-grid {
    display: grid !important;
    grid-template-columns: 1.18fr .82fr !important;
    gap: 1.5mm !important;
    padding: 0 1.5mm 1.5mm !important;
  }

  .analytics-action-manager-group {
    margin: 0 !important;
    padding: 0 !important;
  }

  .analytics-action-manager-head {
    display: flex !important;
    justify-content: space-between !important;
    padding: 1.3mm 2mm !important;
    border-bottom: 1px solid #56635d !important;
    background: #dce6e1 !important;
  }

  .analytics-action-manager-head h3 {
    margin: 0 !important;
    font-size: 8pt !important;
  }

  .analytics-action-manager-items {
    display: grid !important;
    grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
    gap: 1.3mm !important;
    padding: 1.5mm !important;
  }

  .analytics-action-card {
    display: grid !important;
    grid-template-columns: 11mm minmax(0, 1fr) !important;
    gap: 1.5mm !important;
    min-width: 0 !important;
    padding: 1.5mm !important;
    border: 1px solid #718078 !important;
    background: #fff !important;
  }

  .analytics-action-card h3 {
    margin: 0 0 .5mm !important;
    font-size: 7pt !important;
  }

  .analytics-action-card p {
    margin: 0 !important;
    font-size: 6.4pt !important;
    line-height: 1.18 !important;
  }

  .analytics-action-priority {
    text-align: center !important;
    font-size: 6.5pt !important;
    font-weight: 950 !important;
  }

  .refund-text {
    color: #8f1f1a !important;
    font-weight: 950 !important;
  }

  .analytics-print-selected-value {
    display: inline-block;
    min-width: 15mm;
    padding: .5mm 1mm;
    border: 1px solid #555;
    text-align: center;
    font-size: 6.8pt;
    font-weight: 800;
  }

  .model-name-cell,
  .analytics-model-table td:nth-child(3) {
    max-width: 38mm !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
    white-space: nowrap !important;
  }

  .analytics-model-table .rank-col {
    width: 8mm;
  }

  .analytics-model-table .family-col {
    width: 20mm;
  }

  .analytics-model-table .model-col {
    width: 40mm;
  }

  .analytics-model-table .unit-col,
  .analytics-model-table .contract-col {
    width: 13mm;
  }

  small {
    color: #34483e !important;
    font-size: 6pt !important;
  }

  @media print {
    .analytics-report-page {
      break-inside: avoid;
      page-break-inside: avoid;
    }

    .panel,
    .analytics-action-manager-group,
    .analytics-kpi-card,
    .analytics-strength-grid article {
      break-inside: avoid;
      page-break-inside: avoid;
    }
  }
</style>
</head>
<body>
  ${printPages}
  <script>
    function fitAnalyticsReportPages() {
      document.querySelectorAll(".analytics-report-page-body").forEach((body) => {
        body.style.transform = "";
        body.style.transformOrigin = "";
        body.style.width = "";

        const availableHeight = body.clientHeight;
        const availableWidth = body.clientWidth;
        const contentHeight = body.scrollHeight;
        const contentWidth = body.scrollWidth;

        if (!availableHeight || !availableWidth) return;

        let scale = Math.min(
          1,
          availableHeight / Math.max(availableHeight, contentHeight),
          availableWidth / Math.max(availableWidth, contentWidth)
        );

        if (scale < .995) {
          scale = Math.max(.82, scale);
          body.style.transformOrigin = "top left";
          body.style.transform = "scale(" + scale + ")";
          body.style.width = (100 / scale) + "%";
        }
      });
    }

    window.addEventListener("load", () => {
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          fitAnalyticsReportPages();
          setTimeout(() => {
            window.focus();
            window.print();
          }, 180);
        });
      });
    });
  <\/script>
</body>
</html>`);
  doc.close();

  const cleanup = () => setTimeout(() => iframe.remove(), 800);
  iframe.contentWindow.onafterprint = cleanup;
}

function drawAnalyticsTrendChart() {
  const canvas = $("#analyticsTrendCanvas");
  if (!canvas || analyticsActiveTab !== "overview") return;
  const { months } = analyticsRangeValues();
  const entityName = $("#analyticsManagerFilter")?.value || "";
  const data = months.map((month) => analyticsMonthlyMetrics(month, entityName));
  const rect = canvas.getBoundingClientRect();
  const cssWidth = Math.max(560, Math.floor(rect.width || 760));
  const cssHeight = 280;
  const ratio = window.devicePixelRatio || 1;
  canvas.width = cssWidth * ratio;
  canvas.height = cssHeight * ratio;
  const ctx = canvas.getContext("2d");
  ctx.scale(ratio, ratio);
  ctx.clearRect(0, 0, cssWidth, cssHeight);
  const pad = { left: 46, right: 18, top: 20, bottom: 42 };
  const width = cssWidth - pad.left - pad.right;
  const height = cssHeight - pad.top - pad.bottom;
  const visible = data.filter((item) => !["미입력", "분석제외", "예정"].includes(item.status));
  const maxValue = Math.max(10, ...visible.flatMap((item) => [item.reportFinal, item.actualPure, item.goal])) * 1.15;
  ctx.font = "12px sans-serif"; ctx.strokeStyle = "#dfe8e3"; ctx.fillStyle = "#63756d";
  for (let i = 0; i <= 4; i += 1) {
    const y = pad.top + height * i / 4;
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + width, y); ctx.stroke();
    ctx.fillText(String(Math.round(maxValue * (1 - i / 4))), 8, y + 4);
  }
  const xFor = (index) => data.length <= 1 ? pad.left + width / 2 : pad.left + width * index / (data.length - 1);
  const yFor = (value) => pad.top + height - (toNumber(value) / maxValue * height);
  data.forEach((item, index) => { const x = xFor(index); ctx.fillStyle = "#62736b"; ctx.textAlign = "center"; ctx.fillText(item.month.slice(2).replace("-", "."), x, cssHeight - 14); });
  const drawLine = (key, color, dash = []) => {
    ctx.setLineDash(dash); ctx.strokeStyle = color; ctx.lineWidth = key === "goal" ? 2 : 3; ctx.beginPath();
    let started = false;
    data.forEach((item, index) => {
      if (["미입력", "분석제외", "예정"].includes(item.status)) return;
      const x = xFor(index), y = yFor(item[key]);
      if (started) ctx.lineTo(x, y); else { ctx.moveTo(x, y); started = true; }
    });
    ctx.stroke(); ctx.setLineDash([]);
  };
  drawLine("goal", "#d49a32", [6, 5]); drawLine("reportFinal", "#3b77b6"); drawLine("actualPure", "#226b51");
  data.forEach((item, index) => {
    if (["미입력", "분석제외", "예정"].includes(item.status)) return;
    const x = xFor(index);
    [[item.reportFinal, "#3b77b6"], [item.actualPure, "#226b51"]].forEach(([value, color], lineIndex) => {
      const y = yFor(value); ctx.fillStyle = color; ctx.beginPath(); ctx.arc(x, y, 3.7, 0, Math.PI * 2); ctx.fill();
      if (lineIndex === 1) { ctx.fillStyle = "#173d30"; ctx.textAlign = "center"; ctx.fillText(formatNumber(value), x, y - 9); }
    });
  });
}

const ANALYTICS_PRODUCT_OPTIONS = [
  { family: "정수기", terms: ["CP-"] },
  { family: "비데", terms: ["CBT-"] },
  { family: "인덕션", terms: ["CIR-", "CIHR-", "인덕션", "전기레인지"] },
  { family: "매트리스", terms: ["CRM-", "CFM-", "매트리스"] },
  { family: "안마의자", terms: ["CMS-"] },
  { family: "공기청정기", terms: ["AC-"] },
  { family: "제습기", terms: ["CDH-", "DH-", "제습기"] },
  { family: "음식물처리기", terms: ["CFD-", "음식물처리기"] }
];

function analyticsSellerCandidateNames() {
  const names = [];
  const add = (value) => {
    const name = String(value || "").trim();
    if (!name || names.some((item) => analyticsPersonKey(item) === analyticsPersonKey(name))) return;
    names.push(name);
  };
  teamManagerNames().forEach(add);
  (state.records || []).forEach((record) => { add(record?.seller); add(record?.manager); });
  analyticsSettings().sellerAliases.forEach((rule) => { add(rule.source); add(rule.target); });
  Object.keys(analyticsSettings().sellerStartMonths || {}).forEach(add);
  analyticsSettings().hiddenSellers.forEach(add);
  return names.sort((a, b) => a.localeCompare(b, "ko"));
}

function analyticsOptionMarkup(values, selected = "", emptyLabel = "선택") {
  const safe = Array.isArray(values) ? values : [];
  return [`<option value="">${escapeHtml(emptyLabel)}</option>`, ...safe.map((value) => `<option value="${escapeHtml(value)}" ${value === selected ? "selected" : ""}>${escapeHtml(value)}</option>`)].join("");
}

function analyticsMonthStatusDisplayMonths(settings) {
  const start = analyticsEffectiveStartMonth(settings);
  const end = monthIso();
  const months = analyticsMonthRange(start, end).filter((month) => month <= end);
  return months.slice(-18);
}

function syncAnalyticsGuidedControls() {
  const startMode = document.querySelector('input[name="analyticsStartMode"]:checked')?.value || "auto";
  const startWrap = $("#analyticsBranchStartMonthWrap");
  const startInput = $("#analyticsBranchStartMonthInput");
  if (startWrap) startWrap.classList.toggle("disabled", startMode !== "manual");
  if (startInput) startInput.disabled = startMode !== "manual";

  const statusMode = document.querySelector('input[name="analyticsMonthStatusMode"]:checked')?.value || "auto";
  const statusRows = $("#analyticsMonthStatusRows");
  if (statusRows) statusRows.hidden = statusMode !== "manual";
}

function renderAnalyticsMonthStatusRows(settings) {
  const wrap = $("#analyticsMonthStatusRows");
  if (!wrap) return;
  const months = analyticsMonthStatusDisplayMonths(settings);
  const end = monthIso();
  const years = [...new Set(months.map((month) => month.slice(0, 4)))];
  wrap.innerHTML = `
    <div class="analytics-month-status-toolbar">
      <span><strong>${months.length ? escapeHtml(years[0] + (years.length > 1 ? ` · ${years.at(-1)}` : "") ) : ""}</strong> · 최근 ${months.length}개월</span>
      <small>● 자료 있음 · ○ 자료 없음</small>
    </div>
    <div class="analytics-month-status-grid">
      ${months.map((month) => {
        const hasData = analyticsMonthHasData(month);
        const value = settings.monthDataStatus?.[month] || "auto";
        return `<div class="analytics-month-tile ${hasData ? "has-data" : ""}" data-analytics-month="${month}">
          <strong>${escapeHtml(formatMonthLabel(month).replace(/^\d{4}년\s*/, ""))}</strong>
          ${hasData
            ? `<span class="analytics-auto-badge">● 자료 있음</span>`
            : `<select class="analytics-month-status-select" data-month="${month}" aria-label="${escapeHtml(formatMonthLabel(month))} 상태">
                <option value="auto" ${value === "auto" ? "selected" : ""}>○ 자동</option>
                <option value="미입력" ${value === "미입력" ? "selected" : ""}>○ 미입력</option>
                <option value="입력완료" ${value === "입력완료" ? "selected" : ""}>● 완료</option>
              </select>`}
        </div>`;
      }).join("") || `<div class="analytics-empty-choice">선택할 월이 없습니다.</div>`}
    </div>`;
}

function renderAnalyticsAliasRows(settings) {
  const wrap = $("#analyticsSellerAliasRows");
  if (!wrap) return;
  const candidates = analyticsSellerCandidateNames();
  wrap.innerHTML = settings.sellerAliases.map((rule, index) => `<div class="analytics-setting-row analytics-alias-row" data-alias-index="${index}">
    <label>기존 판매자<select class="analytics-alias-source">${analyticsOptionMarkup(candidates, rule.source, "판매자 선택")}</select></label>
    <span class="analytics-row-arrow">→</span>
    <label>통합할 판매자<select class="analytics-alias-target">${analyticsOptionMarkup(candidates, rule.target, "판매자 선택")}</select></label>
    <label>적용월<input class="analytics-alias-month" type="month" value="${escapeHtml(rule.startMonth || "")}"></label>
    <button class="icon-button analytics-remove-alias" type="button" aria-label="삭제">×</button>
  </div>`).join("") || `<div class="analytics-empty-choice">통합할 판매자가 없으면 그대로 두면 됩니다.</div>`;
}

function renderAnalyticsHiddenSellerChoices(settings) {
  const wrap = $("#analyticsHiddenSellerChoices");
  if (!wrap) return;
  const candidates = analyticsSellerCandidateNames();
  const selected = new Set(settings.hiddenSellers.map(analyticsPersonKey));
  wrap.innerHTML = candidates.map((name) => `<label class="analytics-check-chip"><input type="checkbox" value="${escapeHtml(name)}" ${selected.has(analyticsPersonKey(name)) ? "checked" : ""}><span>${escapeHtml(name)}</span></label>`).join("") || `<div class="analytics-empty-choice">판매자 데이터가 등록되면 여기에서 선택할 수 있습니다.</div>`;
}

function renderAnalyticsSellerStartRows(settings) {
  const wrap = $("#analyticsSellerStartRows");
  if (!wrap) return;
  const candidates = analyticsSellerCandidateNames();
  wrap.innerHTML = candidates.map((name) => {
    const month = settings.sellerStartMonths?.[name] || "";
    return `<div class="analytics-setting-row analytics-seller-start-row" data-seller="${escapeHtml(name)}">
      <strong>${escapeHtml(name)}</strong>
      <select class="analytics-seller-start-mode">
        <option value="auto" ${month ? "" : "selected"}>첫 판매월부터 자동</option>
        <option value="manual" ${month ? "selected" : ""}>직접 선택</option>
      </select>
      <input class="analytics-seller-start-month" type="month" value="${escapeHtml(month)}" ${month ? "" : "disabled"}>
    </div>`;
  }).join("") || `<div class="analytics-empty-choice">판매자가 등록되면 시작월을 선택할 수 있습니다.</div>`;
}

function analyticsProductCatalogRule(family) {
  return ANALYTICS_PRODUCT_OPTIONS.find((item) => item.family === family) || null;
}

function analyticsProductRuleTerms(rule = {}) {
  const family = String(rule?.family || "").trim();
  const catalog = analyticsProductCatalogRule(family);
  const terms = [...(catalog?.terms || []), ...(Array.isArray(rule?.terms) ? rule.terms : [])]
    .map((term) => String(term || "").trim())
    .filter(Boolean);
  return [...new Set(terms)];
}

function analyticsProductRuleRowMarkup(rule = {}, index = 0) {
  const family = String(rule?.family || "").trim();
  const selectedTerms = new Set((Array.isArray(rule?.terms) ? rule.terms : []).map((term) => String(term || "").trim().toUpperCase()));
  const terms = analyticsProductRuleTerms(rule);
  const selectedLabels = terms.filter((term) => selectedTerms.has(term.toUpperCase()));
  return `<div class="analytics-product-rule-row" data-product-rule-index="${index}" data-family="${escapeHtml(family)}">
    <div class="analytics-product-rule-summary">
      <div class="analytics-product-rule-name"><strong>${escapeHtml(family)}</strong><span>${selectedLabels.length ? selectedLabels.map((term) => `<em>${escapeHtml(term)}</em>`).join("") : `<em class="empty">분류기준 미선택</em>`}</span></div>
      <div class="analytics-product-rule-actions">
        <button class="ghost-button small analytics-product-edit-btn" type="button">수정</button>
        <button class="ghost-button small danger analytics-product-delete-btn" type="button">삭제</button>
      </div>
    </div>
    <div class="analytics-product-rule-editor" hidden>
      <div class="analytics-product-editor-top">
        <label>제품군 이름<input class="analytics-product-family-input" value="${escapeHtml(family)}" maxlength="30"></label>
      </div>
      <div class="analytics-product-term-options">${terms.map((term) => `<label class="analytics-check-chip"><input type="checkbox" value="${escapeHtml(term)}" ${selectedTerms.has(term.toUpperCase()) ? "checked" : ""}><span>${escapeHtml(term)}</span></label>`).join("")}</div>
      <div class="analytics-product-custom-term-row">
        <input class="analytics-product-new-term" placeholder="새 모델코드/키워드">
        <button class="ghost-button small analytics-product-add-term-btn" type="button">분류기준 추가</button>
      </div>
      <div class="analytics-product-editor-actions">
        <button class="primary-button small analytics-product-edit-done-btn" type="button">수정 완료</button>
      </div>
    </div>
  </div>`;
}

function renderAnalyticsProductAddPanel() {
  const panel = $("#analyticsProductAddPanel");
  if (!panel) return;
  panel.innerHTML = `
    <div class="analytics-product-add-title">기본 제품군 선택</div>
    <div class="analytics-product-template-grid">
      ${ANALYTICS_PRODUCT_OPTIONS.map((group) => `<button type="button" class="analytics-product-template-btn" data-product-template="${escapeHtml(group.family)}"><strong>${escapeHtml(group.family)}</strong><span>${group.terms.map((term) => escapeHtml(term)).join(" · ")}</span></button>`).join("")}
    </div>
    <div class="analytics-product-custom-add">
      <label>새 제품군 이름<input id="analyticsCustomProductFamilyInput" maxlength="30" placeholder="예: 창문형에어컨"></label>
      <label>첫 분류기준<input id="analyticsCustomProductTermInput" maxlength="50" placeholder="예: CA-AW"></label>
      <button class="primary-button small" id="addAnalyticsCustomProductBtn" type="button">직접 추가</button>
    </div>`;
}

function renderAnalyticsProductChoices(settings) {
  const wrap = $("#analyticsProductRuleChoices");
  if (!wrap) return;
  const rules = Array.isArray(settings.productRules) ? settings.productRules : [];
  wrap.innerHTML = rules.length
    ? rules.map((rule, index) => analyticsProductRuleRowMarkup(rule, index)).join("")
    : `<div class="analytics-empty-choice analytics-product-empty">사용할 제품군을 위의 <b>제품군 추가</b>에서 선택해 주세요.</div>`;
  renderAnalyticsProductAddPanel();
}

function analyticsProductRowsCurrentRules() {
  return $$(".analytics-product-rule-row").map((row) => {
    const family = String(row.querySelector(".analytics-product-family-input")?.value || row.dataset.family || "").trim();
    const terms = [...row.querySelectorAll(".analytics-product-term-options input[type='checkbox']:checked")]
      .map((node) => String(node.value || "").trim()).filter(Boolean);
    return { family, terms: [...new Set(terms)] };
  }).filter((rule) => rule.family);
}

function appendAnalyticsProductRule(rule) {
  const wrap = $("#analyticsProductRuleChoices");
  if (!wrap) return;
  const current = analyticsProductRowsCurrentRules();
  const family = String(rule?.family || "").trim();
  if (!family) return;
  if (current.some((item) => item.family === family)) {
    showToast(`${family} 제품군은 이미 등록되어 있습니다.`);
    return;
  }
  const next = [...current, { family, terms: Array.isArray(rule.terms) ? rule.terms : [] }];
  wrap.innerHTML = next.map((item, index) => analyticsProductRuleRowMarkup(item, index)).join("");
}

function updateAnalyticsProductRuleSummary(row) {
  if (!row) return;
  const family = String(row.querySelector(".analytics-product-family-input")?.value || row.dataset.family || "").trim();
  const terms = [...row.querySelectorAll(".analytics-product-term-options input[type='checkbox']:checked")]
    .map((node) => String(node.value || "").trim()).filter(Boolean);
  row.dataset.family = family;
  const strong = row.querySelector(".analytics-product-rule-name > strong");
  const list = row.querySelector(".analytics-product-rule-name > span");
  if (strong) strong.textContent = family || "제품군";
  if (list) list.innerHTML = terms.length ? terms.map((term) => `<em>${escapeHtml(term)}</em>`).join("") : `<em class="empty">분류기준 미선택</em>`;
}

function renderAnalyticsSettings() {
  const settings = analyticsSettings();
  const startMode = settings.branchStartMode === "manual" ? "manual" : "auto";
  const startRadio = document.querySelector(`input[name="analyticsStartMode"][value="${startMode}"]`);
  if (startRadio) startRadio.checked = true;
  if ($("#analyticsBranchStartMonthInput")) $("#analyticsBranchStartMonthInput").value = settings.branchStartMonth || "";

  const statusMode = settings.monthStatusMode === "manual" ? "manual" : "auto";
  const statusRadio = document.querySelector(`input[name="analyticsMonthStatusMode"][value="${statusMode}"]`);
  if (statusRadio) statusRadio.checked = true;

  renderAnalyticsMonthStatusRows(settings);
  renderAnalyticsAliasRows(settings);
  renderAnalyticsHiddenSellerChoices(settings);
  renderAnalyticsSellerStartRows(settings);
  renderAnalyticsProductChoices(settings);
  syncAnalyticsGuidedControls();
}

function saveAnalyticsSettings() {
  const current = analyticsSettings();
  const branchStartMode = document.querySelector('input[name="analyticsStartMode"]:checked')?.value || "auto";
  const branchStartMonth = branchStartMode === "manual" ? ($("#analyticsBranchStartMonthInput")?.value || "") : "";
  const monthStatusMode = document.querySelector('input[name="analyticsMonthStatusMode"]:checked')?.value || "auto";

  const monthDataStatus = {};
  if (monthStatusMode === "manual") {
    $$(".analytics-month-status-select").forEach((select) => {
      const month = select.dataset.month || "";
      const status = select.value || "auto";
      if (/^\d{4}-\d{2}$/.test(month) && ["미입력", "입력완료"].includes(status)) monthDataStatus[month] = status;
    });
  }

  const sellerAliases = [];
  $$(".analytics-alias-row").forEach((row) => {
    const source = row.querySelector(".analytics-alias-source")?.value || "";
    const target = row.querySelector(".analytics-alias-target")?.value || "";
    const startMonth = row.querySelector(".analytics-alias-month")?.value || "";
    if (source && target && source !== target && /^\d{4}-\d{2}$/.test(startMonth)) sellerAliases.push({ source, target, startMonth });
  });

  const hiddenSellers = $$("#analyticsHiddenSellerChoices input[type='checkbox']:checked").map((node) => node.value).filter(Boolean);

  const sellerStartMonths = {};
  $$(".analytics-seller-start-row").forEach((row) => {
    const name = row.dataset.seller || "";
    const mode = row.querySelector(".analytics-seller-start-mode")?.value || "auto";
    const month = row.querySelector(".analytics-seller-start-month")?.value || "";
    if (name && mode === "manual" && /^\d{4}-\d{2}$/.test(month)) sellerStartMonths[name] = month;
  });

  const productRules = analyticsProductRowsCurrentRules()
    .map((rule) => ({ family: rule.family, terms: rule.terms }))
    .filter((rule) => rule.family && rule.terms.length);

  state.salesAnalyticsSettings = normalizeSalesAnalyticsSettings({
    ...current,
    branchStartMode,
    branchStartMonth,
    monthStatusMode,
    monthDataStatus,
    sellerAliases,
    hiddenSellers,
    sellerStartMonths,
    productRules
  });
  persistState();
  renderAnalyticsSettings();
  if (currentView === "analytics") renderAnalytics();
  showToast("영업분석 기준을 저장했습니다.");
}

function renderAnalytics() {
  const view = $("#analyticsView");
  if (!view) return;
  const settings = analyticsSettings();
  const endInput = $("#analyticsEndMonth");
  const startInput = $("#analyticsStartMonth");
  if (endInput && !endInput.value) endInput.value = $("#monthFilter")?.value || monthIso();
  if (startInput && !startInput.value) {
    const calculated = shiftMonth(endInput?.value || monthIso(), -(analyticsRangePreset - 1));
    const branchStart = analyticsEffectiveStartMonth(settings);
    startInput.value = calculated < branchStart ? branchStart : calculated;
  }
  if (startInput) startInput.min = settings.branchStartMonth || "";
  const entityNames = analyticsActualEntityNames();
  const managerFilter = $("#analyticsManagerFilter");
  const currentValue = managerFilter?.value || analyticsSelectedManager || "";
  if (managerFilter) setOptions(managerFilter, [{ value: "", label: "지국 전체" }, ...entityNames.map((name) => ({ value: name, label: name }))], currentValue);
  analyticsSelectedManager = managerFilter?.value || "";
  const detailSelect = $("#analyticsDetailManagerSelect");
  const detailValue = detailSelect?.value || analyticsSelectedManager || entityNames[0] || "";
  if (detailSelect) setOptions(detailSelect, entityNames.map((name) => ({ value: name, label: name })), detailValue);

  const { start, end, months } = analyticsRangeValues();
  if (!months.length) return;
  const selectedEntity = managerFilter?.value || "";
  const endMetrics = analyticsMonthlyMetrics(end, selectedEntity);
  const completedMetrics = months.filter(analyticsIsCompletedMonth).map((month) => analyticsMonthlyMetrics(month, selectedEntity));
  const period = analyticsPeriodProgress(end);
  const projected = period.progress > 0 && period.progress < 1 ? endMetrics.reportFinal / period.progress : endMetrics.reportFinal;
  const completedAverage = analyticsAverage(completedMetrics.map((item) => item.actualEvaluated));
  const shortage = Math.max(endMetrics.goal - endMetrics.reportFinal, 0);
  const dailyNeed = period.remainingDays > 0 ? shortage / period.remainingDays : shortage;

  if ($("#analyticsCurrentFinal")) $("#analyticsCurrentFinal").textContent = formatNumber(endMetrics.reportFinal);
  if ($("#analyticsCurrentPure")) $("#analyticsCurrentPure").textContent = formatNumber(endMetrics.actualPure);
  if ($("#analyticsCurrentMonthLabel")) $("#analyticsCurrentMonthLabel").textContent = `${formatMonthLabel(end)} · ${selectedEntity || "지국 전체"} · ${endMetrics.status}`;
  if ($("#analyticsPureGapLabel")) $("#analyticsPureGapLabel").textContent = `재약정 ${formatNumber(endMetrics.renewal)} · 환수 -${formatNumber(endMetrics.refund)} · 접수대비 ${endMetrics.difference >= 0 ? "+" : ""}${formatNumber(endMetrics.difference)}건`;
  if ($("#analyticsProjectedFinal")) $("#analyticsProjectedFinal").textContent = formatNumber(projected);
  if ($("#analyticsProgressLabel")) $("#analyticsProgressLabel").textContent = `기간 ${Math.round(period.progress * 100)}% 경과`;
  if ($("#analyticsCompletedAverage")) $("#analyticsCompletedAverage").textContent = formatNumber(completedAverage);
  if ($("#analyticsAverageMonthCount")) $("#analyticsAverageMonthCount").textContent = `${completedMetrics.length}개월 평균`;
  if ($("#analyticsCurrentGoal")) $("#analyticsCurrentGoal").textContent = formatNumber(endMetrics.goal);
  if ($("#analyticsGoalGap")) $("#analyticsGoalGap").textContent = shortage > 0 ? `부족 ${formatNumber(shortage)}건` : `초과 ${formatNumber(Math.abs(endMetrics.reportFinal - endMetrics.goal))}건`;
  if ($("#analyticsDailyNeed")) $("#analyticsDailyNeed").textContent = formatNumber(dailyNeed);
  if ($("#analyticsRemainingDays")) $("#analyticsRemainingDays").textContent = `${period.remainingDays}일 남음`;
  if ($("#analyticsDiagnosisPeriod")) $("#analyticsDiagnosisPeriod").textContent = `${formatMonthLabel(start)} ~ ${formatMonthLabel(end)}`;
  if ($("#analyticsTrendScope")) $("#analyticsTrendScope").textContent = selectedEntity || "지국 전체";
  if ($("#analyticsDataBasisLabel")) $("#analyticsDataBasisLabel").textContent = `${state.appMeta?.branchName || "지국"} ${formatMonthLabel(analyticsEffectiveStartMonth(settings))}부터`;

  const summaries = entityNames.map((name) => analyticsManagerSummary(name, months));
  const teamAverages = analyticsTeamCategoryAverages(summaries);
  renderAnalyticsDiagnosis(endMetrics, completedMetrics, period, summaries);
  renderAnalyticsCategoryMix(endMetrics);
  renderAnalyticsManagerSnapshot(summaries, teamAverages, months);
  renderAnalyticsMonthlyTable(months, selectedEntity);
  renderAnalyticsMatrix(months, entityNames);
  renderAnalyticsManagerAverage(summaries, teamAverages);
  const detailName = detailSelect?.value || analyticsSelectedManager || entityNames[0];
  const detailSummary = summaries.find((item) => item.managerName === detailName) || summaries[0];
  renderAnalyticsManagerDetail(detailSummary, teamAverages, months);
  renderAnalyticsProducts(months, selectedEntity, entityNames);
  const recommendations = analyticsBuildRecommendations(months, summaries, teamAverages);
  renderAnalyticsActions(recommendations);
  renderAnalyticsCombinedReport(selectedEntity, months, summaries, teamAverages, recommendations);
  setAnalyticsTab(analyticsActiveTab);
  window.requestAnimationFrame(drawAnalyticsTrendChart);
}



function managementEvaluationMonth() {
  return $("#evaluationMonthInput")?.value || $("#monthFilter")?.value || monthIso();
}

function managementEvaluationPolicy(month = managementEvaluationMonth()) {
  if (!state.managementEvaluationPolicies || typeof state.managementEvaluationPolicies !== "object" || Array.isArray(state.managementEvaluationPolicies)) {
    state.managementEvaluationPolicies = {};
  }
  if (!state.managementEvaluationPolicies[month]) {
    const previousMonth = Object.keys(state.managementEvaluationPolicies)
      .filter((item) => /^\d{4}-\d{2}$/.test(item) && item < month)
      .sort()
      .pop();
    const source = month === "2026-08"
      ? defaultManagementEvaluationPolicy(month)
      : previousMonth
        ? structuredClone(state.managementEvaluationPolicies[previousMonth])
        : defaultManagementEvaluationPolicy(month);
    state.managementEvaluationPolicies[month] = normalizeManagementEvaluationPolicy(source, month);
  }
  state.managementEvaluationPolicies[month] = normalizeManagementEvaluationPolicy(state.managementEvaluationPolicies[month], month);
  return state.managementEvaluationPolicies[month];
}

function managementEvaluationInput(month = managementEvaluationMonth()) {
  if (!state.managementEvaluationInputs || typeof state.managementEvaluationInputs !== "object" || Array.isArray(state.managementEvaluationInputs)) {
    state.managementEvaluationInputs = {};
  }
  if (!state.managementEvaluationInputs[month]) {
    const defaults = defaultManagementEvaluationInput();
    defaults.inspectionTotalAccount = toNumber(monthSetting(month).accountCount) || null;
    state.managementEvaluationInputs[month] = defaults;
  }
  state.managementEvaluationInputs[month] = normalizeManagementEvaluationInput(state.managementEvaluationInputs[month]);
  const policy = managementEvaluationPolicy(month);
  const manual = state.managementEvaluationInputs[month].policyManual;
  if (manual["policy-massage::otherTeam"] === undefined && state.managementEvaluationInputs[month].aTeamMassageUnits !== null && policy.policyItems.some((item) => item.id === "policy-massage")) {
    manual["policy-massage::otherTeam"] = state.managementEvaluationInputs[month].aTeamMassageUnits;
  }
  if (manual["policy-mattress::otherTeam"] === undefined && state.managementEvaluationInputs[month].aTeamMattressCareUnits !== null && policy.policyItems.some((item) => item.id === "policy-mattress")) {
    manual["policy-mattress::otherTeam"] = state.managementEvaluationInputs[month].aTeamMattressCareUnits;
  }
  return state.managementEvaluationInputs[month];
}

function managementEvaluationRecords(month = managementEvaluationMonth()) {
  const period = monthPeriod(month);
  return (state.records || []).filter((record) => recordBelongsToCurrentUserTeam(record, month) && inDateRange(record.receivedDate || "", period.start, period.end));
}

function managementEvaluationActiveRecords(month = managementEvaluationMonth()) {
  return managementEvaluationRecords(month).filter((record) => record && record.status !== "취소");
}

function managementEvaluationRecordText(record = {}) {
  return [
    record.product,
    record.productName,
    record.itemName,
    record.model,
    record.memo
  ].map((value) => String(value || "")).join(" ").normalize("NFKC");
}

function managementEvaluationIsSpecialSale(record) {
  return managementEvaluationRecordText(record).includes("특판");
}

function managementEvaluationProductItems(record) {
  const items = analyticsProductItems(record);
  if (items.length) return items;
  const raw = String(record?.product || record?.productName || record?.itemName || "").trim();
  return raw ? [raw] : [];
}

function managementEvaluationPhysicalCount(record) {
  return Math.max(1, Math.ceil(analyticsRecordCount(record)));
}

function managementEvaluationProductUnits(record) {
  const itemCount = Math.max(1, managementEvaluationProductItems(record).length);
  return managementEvaluationPhysicalCount(record) * itemCount;
}

function managementEvaluationNormalizedProduct(value) {
  return String(value || "").normalize("NFKC").toUpperCase().replace(/\s+/g, "");
}

function managementEvaluationModelName(value) {
  const normalized = managementEvaluationNormalizedProduct(value);
  const match = normalized.match(/[A-Z]{2,4}-[A-Z0-9\-]+/);
  return match ? match[0] : analyticsModelName(value);
}

function managementEvaluationContainsAny(text, terms = []) {
  return terms.some((term) => text.includes(String(term || "").toUpperCase()));
}

function managementEvaluationIsRerental(record) {
  const category = normalizeCategory(record?.category);
  const text = managementEvaluationNormalizedProduct(managementEvaluationRecordText(record));
  return category === "재렌탈" || text.includes("재렌탈") || text.includes("/R/");
}

function managementEvaluationIsBusinessRecord(record) {
  const category = normalizeCategory(record?.category);
  return ["신규", "패키지", "재렌탈", "일시불"].includes(category);
}

function managementEvaluationIsNewProductRecord(record) {
  const category = normalizeCategory(record?.category);
  if (!["신규", "패키지", "일시불"].includes(category)) return false;
  return !managementEvaluationIsRerental(record);
}


function isMattressCareProduct(productText) {
  const text = managementEvaluationNormalizedProduct(productText);
  if (!text.startsWith("CRM-")) return false;
  return ["6C", "12C", "4C", "케어B"].some(term => text.includes(String(term).toUpperCase()));
}

function isMattressCareRecord(record) {
  if (!record || record.status === "취소") return false;
  return managementEvaluationProductItems(record).some(item => isMattressCareProduct(item));
}

function dashboardMattressCareMatch(record) {
  return isMattressCareRecord(record);
}

function managementEvaluationRuleMatches(productText, rule = {}) {
  const text = managementEvaluationNormalizedProduct(productText);
  const keywords = evaluationKeywordList(rule.keywords);
  const required = evaluationKeywordList(rule.requiredKeywords);
  const excludes = evaluationKeywordList(rule.excludeKeywords);
  return (keywords.length === 0 || keywords.some((keyword) => text.includes(keyword)))
    && required.every((keyword) => text.includes(keyword))
    && !excludes.some((keyword) => text.includes(keyword));
}

function managementEvaluationPrimaryType(productText, month = managementEvaluationMonth()) {
  const rule = managementEvaluationPolicy(month).primaryProducts
    .find((item) => managementEvaluationRuleMatches(productText, item));
  return rule?.title || "";
}

function managementEvaluationHighValueType(productText, month = managementEvaluationMonth()) {
  const rule = managementEvaluationPolicy(month).highValueProducts
    .find((item) => managementEvaluationRuleMatches(productText, item));
  return rule?.title || "";
}

function managementEvaluationHighValueUnits(record, productText, month = managementEvaluationMonth()) {
  const type = managementEvaluationHighValueType(productText, month);
  if (!type) return 0;
  const text = managementEvaluationNormalizedProduct(productText);
  if (type === "비데" && (text.includes("2개세트") || text.includes("2개SET") || text.includes("2개세트"))) return 1;
  return managementEvaluationPhysicalCount(record);
}

function managementEvaluationPrimaryStats(records, month = managementEvaluationMonth()) {
  const modelMap = new Map();
  const typeMap = new Map();
  let total = 0;
  records.filter(managementEvaluationIsNewProductRecord).forEach((record) => {
    managementEvaluationProductItems(record).forEach((item) => {
      const type = managementEvaluationPrimaryType(item, month);
      if (!type) return;
      const units = managementEvaluationPhysicalCount(record);
      const model = managementEvaluationModelName(item);
      modelMap.set(model, (modelMap.get(model) || 0) + units);
      typeMap.set(type, (typeMap.get(type) || 0) + units);
      total += units;
    });
  });
  return {
    total,
    models: [...modelMap.entries()].map(([model, count]) => ({ model, count })).sort((a, b) => b.count - a.count || a.model.localeCompare(b.model, "ko")),
    types: [...typeMap.entries()].map(([type, count]) => ({ type, count })).sort((a, b) => b.count - a.count)
  };
}

function managementEvaluationHighValueStats(records, month = managementEvaluationMonth()) {
  const modelMap = new Map();
  const typeMap = new Map();
  let total = 0;

  records.forEach((record) => {
    managementEvaluationProductItems(record).forEach((item) => {
      const type = managementEvaluationHighValueType(item, month);
      if (!type) return;
      const units = managementEvaluationHighValueUnits(record, item, month);
      if (!(units > 0)) return;
      const model = managementEvaluationModelName(item);
      modelMap.set(model, (modelMap.get(model) || 0) + units);
      typeMap.set(type, (typeMap.get(type) || 0) + units);
      total += units;
    });
  });

  return {
    total,
    models: [...modelMap.entries()]
      .map(([model, count]) => ({ model, count }))
      .sort((a, b) => b.count - a.count || a.model.localeCompare(b.model, "ko")),
    types: [...typeMap.entries()]
      .map(([type, count]) => ({ type, count }))
      .sort((a, b) => b.count - a.count)
  };
}
function managementEvaluationScoreUp(rate, baseRate, stepRate, baseScore, stepScore, maxScore) {
  if (!Number.isFinite(rate) || rate < baseRate) return 0;
  const steps = Math.floor((rate - baseRate + 1e-9) / stepRate);
  return Math.min(maxScore, baseScore + steps * stepScore);
}

function managementEvaluationScoreDown(rate, baseRate, stepRate, baseScore, stepScore, maxScore) {
  if (!Number.isFinite(rate) || rate > baseRate) return 0;
  const steps = Math.floor((baseRate - rate + 1e-9) / stepRate);
  return Math.min(maxScore, baseScore + steps * stepScore);
}

function managementEvaluationPolicyScoreByUnits(units, thresholds, scoreMode = "at-least") {
  const value = Number(units);
  if (!Number.isFinite(value)) return null;
  const rows = Array.isArray(thresholds) ? thresholds.slice().sort((a, b) => toNumber(a[0]) - toNumber(b[0])) : [];
  if (scoreMode === "at-most") {
    const matched = rows.find(([maximum]) => value <= toNumber(maximum));
    return matched ? toNumber(matched[1]) : 0;
  }
  let score = 0;
  rows.forEach(([minimum, points]) => {
    if (value >= toNumber(minimum)) score = toNumber(points);
  });
  return score;
}

function managementEvaluationPolicyCriteriaText(item) {
  const suffix = item.kind === "count" ? "대" : "%";
  return item.scoreRules
    .map(([threshold, score]) => item.scoreMode === "at-most"
      ? `${formatNumber(threshold)}${suffix} 이하 ${formatNumber(score)}점`
      : `${formatNumber(threshold)}${suffix} 이상 ${formatNumber(score)}점`)
    .join(" · ");
}

function managementEvaluationGoalBaseLabel(goalBase) {
  return ({
    "new": "신규",
    "new-rental": "신규+재렌탈",
    "lump-sum": "일시불",
    "general": "전체"
  })[goalBase] || "신규+재렌탈";
}

function managementEvaluationPolicyCategoryLabel(filter) {
  return ({
    "all": "전체 접수",
    "business": "영업 전체",
    "rental": "렌탈(신규+패키지+재렌탈)",
    "new-rental": "신규+재렌탈",
    "new": "신규",
    "package": "패키지",
    "rerental": "재렌탈",
    "cash": "일시불",
    "membership": "멤버십"
  })[filter] || "영업 전체";
}

function managementEvaluationPolicyCategoryMatches(record, filter = "business") {
  const category = normalizeCategory(record?.category);
  switch (filter) {
    case "all": return true;
    case "rental": return ["신규", "패키지", "재렌탈"].includes(category);
    case "new-rental": return category === "신규" || category === "재렌탈";
    case "new": return category === "신규";
    case "package": return category === "패키지";
    case "rerental": return category === "재렌탈";
    case "cash": return category === "일시불";
    case "membership": return isMembershipRecord(record);
    case "business":
    default: return managementEvaluationIsBusinessRecord(record);
  }
}

function managementEvaluationGoalBaseMatches(record, goalBase) {
  return managementEvaluationPolicyCategoryMatches(record, ({
    "new": "new",
    "new-rental": "new-rental",
    "lump-sum": "cash",
    "general": "business"
  })[goalBase] || "business");
}

function managementEvaluationPolicyProductMatches(productText, item = {}) {
  const text = managementEvaluationNormalizedProduct(productText);
  const keywords = evaluationKeywordList(item.keywords);
  const required = evaluationKeywordList(item.requiredKeywords);
  const excludes = evaluationKeywordList(item.excludeKeywords);
  const anyMatch = !keywords.length || keywords.some((keyword) => text.includes(keyword));
  const requiredMatch = required.every((keyword) => text.includes(keyword));
  const excluded = excludes.some((keyword) => text.includes(keyword));
  return anyMatch && requiredMatch && !excluded;
}

function managementEvaluationPolicyMatchedUnits(records, item = {}) {
  const eligible = (records || []).filter((record) => managementEvaluationPolicyCategoryMatches(record, item.categoryFilter));
  if (item.countBasis === "product") {
    return eligible.reduce((sum, record) => sum + managementEvaluationProductItems(record)
      .filter((product) => managementEvaluationPolicyProductMatches(product, item))
      .reduce((itemSum) => itemSum + managementEvaluationPhysicalCount(record), 0), 0);
  }
  return eligible.reduce((sum, record) => {
    const matched = managementEvaluationProductItems(record).some((product) => managementEvaluationPolicyProductMatches(product, item));
    return sum + (matched ? managementEvaluationPhysicalCount(record) : 0);
  }, 0);
}

function managementEvaluationPolicyItemMetrics(records, goals, input, item, month = managementEvaluationMonth()) {
  const isLegacyWaterRateItem = item?.id === "policy-water" && item.kind === "rate";

  // 기존 정수기 목표 달성률 항목은 대시보드 정수기 KPI와 동일 기준 유지.
  if (isLegacyWaterRateItem) {
    const water = waterPurifierEvaluationMetrics(month);
    return {
      ...item,
      autoUnits: water.current,
      manualUnits: null,
      totalUnits: water.current,
      goal: water.goal,
      rate: water.achievementRate,
      score: managementEvaluationPolicyScoreByUnits(water.achievementRate, item.scoreRules, item.scoreMode)
    };
  }

  const matchedUnits = managementEvaluationPolicyMatchedUnits(records, item);
  const otherTeamKey = `${item.id}::otherTeam`;
  const otherTeamUnits = item.teamAggregation === "both"
    ? Math.max(0, toNumber(input.policyManual?.[otherTeamKey] ?? 0))
    : 0;

  if (item.kind === "percentile") {
    const manualValue = input.policyManual?.[item.id];
    const percentile = manualValue === null || manualValue === undefined || manualValue === "" ? null : Math.max(0, Math.min(100, toNumber(manualValue)));
    return {
      ...item,
      autoUnits: matchedUnits,
      manualUnits: percentile,
      totalUnits: matchedUnits,
      goal: null,
      rate: percentile,
      score: percentile === null ? null : managementEvaluationPolicyScoreByUnits(percentile, item.scoreRules, "at-most")
    };
  }

  if (item.kind === "rate") {
    const baseGoal = item.goalBase === "new"
      ? toNumber(goals.newGoal)
      : item.goalBase === "general"
        ? toNumber(goals.generalGoal)
        : item.goalBase === "lump-sum"
          ? toNumber(goals.cashGoal || 0)
          : toNumber(goals.newGoal) + toNumber(goals.rentalGoal);
    const goal = baseGoal * (toNumber(item.targetRate) / 100);
    const effectiveUnits = matchedUnits + otherTeamUnits;
    const rate = goal > 0 ? effectiveUnits / goal * 100 : 0;
    return {
      ...item,
      autoUnits: matchedUnits,
      otherTeamUnits,
      manualUnits: null,
      totalUnits: effectiveUnits,
      goal,
      rate,
      score: managementEvaluationPolicyScoreByUnits(rate, item.scoreRules, item.scoreMode)
    };
  }

  const manualValue = input.policyManual?.[item.id];
  const manualUnits = manualValue === null || manualValue === undefined || manualValue === ""
    ? 0
    : toNumber(manualValue);
  const totalUnits = matchedUnits + otherTeamUnits + manualUnits;
  return {
    ...item,
    autoUnits: matchedUnits,
    otherTeamUnits,
    manualUnits: manualValue === null || manualValue === undefined || manualValue === "" ? null : manualUnits,
    totalUnits,
    goal: null,
    rate: null,
    score: managementEvaluationPolicyScoreByUnits(totalUnits, item.scoreRules, item.scoreMode)
  };
}

function managementEvaluationMetrics(month = managementEvaluationMonth()) {
  const records = managementEvaluationActiveRecords(month);
  const businessRecords = records.filter(managementEvaluationIsBusinessRecord);
  const goals = calculatedGoals(month);
  const manualStats = applyManualStatsToTotals(actuals(businessRecords), "", month);
  const input = managementEvaluationInput(month);
  const policy = managementEvaluationPolicy(month);

  const overallActual = Math.max(0, toNumber(manualStats.coreActual) - toNumber(manualStats.refundActual));
  const overallGoal = toNumber(goals.generalGoal);
  const overallRate = overallGoal > 0 ? overallActual / overallGoal * 100 : 0;
  const newActual = toNumber(manualStats.newActual);
  const newGoal = toNumber(goals.newGoal);
  const newRate = newGoal > 0 ? newActual / newGoal * 100 : 0;

  const packageBaseRecords = businessRecords.filter((record) => {
    const category = normalizeCategory(record.category);
    return ["신규", "패키지", "재렌탈"].includes(category) && !managementEvaluationIsSpecialSale(record);
  });
  const packageDenominator = packageBaseRecords.reduce((sum, record) => sum + managementEvaluationProductUnits(record), 0);
  const packageUnits = packageBaseRecords
    .filter((record) => normalizeCategory(record.category) === "패키지")
    .reduce((sum, record) => sum + managementEvaluationProductUnits(record), 0);
  const packageRate = packageDenominator > 0 ? packageUnits / packageDenominator * 100 : 0;

  const primaryStats = managementEvaluationPrimaryStats(businessRecords, month);
  const primaryRelativeScore = input.primaryRelativeScore === null
    ? null
    : Math.max(0, Math.min(5, toNumber(input.primaryRelativeScore)));

  const highValueStats = managementEvaluationHighValueStats(businessRecords, month);
  const highValueUnits = highValueStats.total;
  const totalBusinessCount = businessRecords.reduce((sum, record) => sum + analyticsRecordCount(record), 0);
  const highValueRate = totalBusinessCount > 0 ? highValueUnits / totalBusinessCount * 100 : 0;

  const membershipCount = records
    .filter(isMembershipRecord)
    .reduce((sum, record) => sum + analyticsRecordCount(record), 0);

  const configuredMasterNameForEvaluation = compactValue((state.appMeta || sampleState.appMeta || {}).masterName, "");
  const configuredMasterKeyForEvaluation = analyticsPersonKey(configuredMasterNameForEvaluation);
  const activeManagers = teamManagers(month).filter((manager) => {
    const name = String(manager.name || "").trim();
    const key = analyticsPersonKey(name);
    if (name === "팀장") return false;
    if (configuredMasterNameForEvaluation && (name === configuredMasterNameForEvaluation || key === configuredMasterKeyForEvaluation)) return false;
    return true;
  });
  const accountCount = toNumber(monthSetting(month).accountCount);
  const requiredManagerCount = accountCount > 0 ? accountCount / 130 : 0;
  const managerFillRate = requiredManagerCount > 0 ? activeManagers.length / requiredManagerCount * 100 : 0;

  const retentionRate = input.retentionRate;
  const cancellationRate = input.cancellationRate;
  const managerChange = input.managerChange;

  const inspectionTotalAccount = input.inspectionTotalAccount;
  const inspectionCancel = toNumber(input.inspectionCancel);
  const inspectionExceptionHold = toNumber(input.inspectionExceptionHold);
  const inspectionHold = toNumber(input.inspectionHold);
  const inspectionCompleted = input.inspectionCompleted;
  const inspectionDenominator = inspectionTotalAccount === null
    ? null
    : Math.max(0, toNumber(inspectionTotalAccount) - inspectionCancel - inspectionExceptionHold + inspectionHold);
  const inspectionRate = inspectionCompleted === null || inspectionDenominator === null || inspectionDenominator <= 0
    ? null
    : toNumber(inspectionCompleted) / inspectionDenominator * 100;
  const happyTalkRate = input.happyTalkRate;

  // V10.70: 정책이행 각 항목의 판매종류/포함/필수/제외 조건은 항목 자체 설정으로 판단한다.
  const policyItems = policy.policyItems.map((item) =>
    managementEvaluationPolicyItemMetrics(records, goals, input, item, month)
  );

  const scores = {
    overall: managementEvaluationScoreUp(overallRate, 80, 10, 2, 1, 5),
    newGoal: managementEvaluationScoreUp(newRate, 70, 10, 2, 2, 10),
    package: managementEvaluationScoreUp(packageRate, 15, 3, 2, 1, 12),
    primary: primaryRelativeScore,
    highValue: managementEvaluationScoreUp(highValueRate, 35, 5, 2, 2, 8),
    retention: retentionRate === null ? null : managementEvaluationScoreUp(retentionRate, 40, 5, 2, 2, 12),
    cancellation: cancellationRate === null ? null : managementEvaluationScoreDown(cancellationRate, 0.40, 0.05, 2, 2, 12),
    membership: Math.min(6, membershipCount * 0.2),
    managerFill: managementEvaluationScoreUp(managerFillRate, 80, 5, 2, 1, 7),
    managerChange: managerChange === null
      ? null
      : managerFillRate >= 90
        ? 3
        : managerFillRate < 70
          ? 0
          : Math.max(0, Math.min(3, Math.floor(managerChange))),
    inspection: inspectionRate === null ? null : managementEvaluationScoreUp(inspectionRate, 97, 1, 1, 1, 3),
    happyTalk: happyTalkRate === null ? null : (happyTalkRate >= 15 ? 3 : happyTalkRate >= 12 ? 2 : happyTalkRate >= 10 ? 1 : 0),
    ...Object.fromEntries(policyItems.map((item) => [`policy:${item.id}`, item.score]))
  };

  const knownScores = Object.values(scores).filter((score) => score !== null);
  const currentScore = knownScores.reduce((sum, score) => sum + toNumber(score), 0);
  const pendingInputs = Object.values(scores).filter((score) => score === null).length;
  const maxScore = 86 + policyItems.reduce((sum, item) => sum + Math.max(0, ...item.scoreRules.map(([, score]) => toNumber(score))), 0);

  return {
    month,
    period: monthPeriod(month),
    records,
    businessRecords,
    goals,
    input,
    overallActual,
    overallGoal,
    overallRate,
    newActual,
    newGoal,
    newRate,
    packageUnits,
    packageDenominator,
    packageRate,
    primaryStats,
    primaryRelativeScore,
    highValueStats,
    highValueUnits,
    totalBusinessCount,
    highValueRate,
    policy,
    policyItems,
    retentionRate,
    cancellationRate,
    membershipCount,
    accountCount,
    activeManagerCount: activeManagers.length,
    requiredManagerCount,
    managerFillRate,
    managerChange,
    inspectionTotalAccount,
    inspectionCancel,
    inspectionExceptionHold,
    inspectionHold,
    inspectionCompleted,
    inspectionDenominator,
    inspectionRate,
    happyTalkRate,
    scores,
    currentScore,
    maxScore,
    pendingInputs
  };
}

function managementEvaluationFormatRate(value) {
  return value === null || !Number.isFinite(value) ? "-" : `${formatNumber(value)}%`;
}

function managementEvaluationScoreText(score, maximum) {
  return score === null ? `- / ${maximum}` : `${formatNumber(score)} / ${maximum}`;
}

function managementEvaluationRows(metrics) {
  const policyMax = metrics.policyItems.reduce((sum, item) => sum + Math.max(0, ...item.scoreRules.map(([, score]) => toNumber(score))), 0);
  const rows = [
    {
      part: "영업관리", partMax: 40, item: "전체목표 달성률",
      value: `${formatNumber(metrics.overallActual)} / ${formatNumber(metrics.overallGoal)} (${managementEvaluationFormatRate(metrics.overallRate)})`,
      criteria: "80% 2점 · 90% 3점 · 100% 4점 · 110% 5점",
      max: 5, score: metrics.scores.overall
    },
    {
      part: "영업관리", partMax: 40, item: "신규목표 달성률",
      value: `${formatNumber(metrics.newActual)} / ${formatNumber(metrics.newGoal)} (${managementEvaluationFormatRate(metrics.newRate)})`,
      criteria: "70% 2점 · 80% 4점 · 90% 6점 · 100% 8점 · 110% 10점",
      max: 10, score: metrics.scores.newGoal
    },
    {
      part: "영업관리", partMax: 40, item: "패키지 실적 비중",
      value: `${formatNumber(metrics.packageUnits)} / ${formatNumber(metrics.packageDenominator)} (${managementEvaluationFormatRate(metrics.packageRate)})`,
      criteria: "15% 2점 · 이후 3%당 +1점 · 45% 12점",
      max: 12, score: metrics.scores.package
    },
    {
      part: "영업관리", partMax: 40, item: "주력상품 신규건수(상대평가)",
      value: `${formatNumber(metrics.primaryStats.total)}건`,
      criteria: "상대평가 예상점수 수기입력",
      max: 5, score: metrics.scores.primary
    },
    {
      part: "영업관리", partMax: 40, item: "고가군 실적 비중",
      value: `${formatNumber(metrics.highValueUnits)} / ${formatNumber(metrics.totalBusinessCount)} (${managementEvaluationFormatRate(metrics.highValueRate)})`,
      criteria: "35% 2점 · 40% 4점 · 45% 6점 · 50% 8점",
      max: 8, score: metrics.scores.highValue
    },

    {
      part: "계정관리", partMax: 30, item: "유지율",
      value: managementEvaluationFormatRate(metrics.retentionRate),
      criteria: "40% 2점 · 이후 5%당 +2점 · 65% 12점",
      max: 12, score: metrics.scores.retention
    },
    {
      part: "계정관리", partMax: 30, item: "해지율",
      value: managementEvaluationFormatRate(metrics.cancellationRate),
      criteria: "0.40% 2점 · 0.35% 4점 · 0.30% 6점 · 0.25% 8점 · 0.20% 10점 · 0.15% 12점",
      max: 12, score: metrics.scores.cancellation
    },
    {
      part: "계정관리", partMax: 30, item: "멤버십 전환 건수",
      value: `${formatNumber(metrics.membershipCount)}건`,
      criteria: "1건당 0.2점 · 최대 30건",
      max: 6, score: metrics.scores.membership
    },

    {
      part: "조직관리", partMax: 10, item: "매니저 충원율",
      value: `${formatNumber(metrics.activeManagerCount)}명 / ${formatNumber(metrics.requiredManagerCount)}명 (${managementEvaluationFormatRate(metrics.managerFillRate)})`,
      criteria: "80% 2점 · 이후 5%당 +1점 · 105% 7점",
      max: 7, score: metrics.scores.managerFill
    },
    {
      part: "조직관리", partMax: 10, item: "매니저 전월대비 증감",
      value: metrics.managerChange === null ? "-" : `${formatNumber(metrics.managerChange)}명`,
      criteria: "70% 미만 0점 · +1명 1점 · +2명 2점 · +3명 3점 · 충원율 90% 이상 3점",
      max: 3, score: metrics.scores.managerChange
    },

    {
      part: "고객서비스관리", partMax: 6, item: "점검처리율",
      value: metrics.inspectionRate === null ? "-" : `${formatNumber(metrics.inspectionCompleted)} / ${formatNumber(metrics.inspectionDenominator)} (${managementEvaluationFormatRate(metrics.inspectionRate)})`,
      criteria: "97% 1점 · 98% 2점 · 99% 3점",
      max: 3, score: metrics.scores.inspection
    },
    {
      part: "고객서비스관리", partMax: 6, item: "해피톡 응답률",
      value: managementEvaluationFormatRate(metrics.happyTalkRate),
      criteria: "10% 1점 · 12% 2점 · 15% 3점",
      max: 3, score: metrics.scores.happyTalk
    },

    ...metrics.policyItems.map((item) => ({
      part: "정책이행",
      partMax: policyMax,
      item: item.title,
      value: item.kind === "rate"
        ? `${formatNumber(item.autoUnits)}${item.teamAggregation === "both" ? ` + 상대팀 ${formatNumber(item.otherTeamUnits)} = ${formatNumber(item.totalUnits)}` : ""} / ${formatNumber(item.goal)} (${managementEvaluationFormatRate(item.rate)})`
        : item.kind === "percentile"
          ? `정책영업 ${formatNumber(item.autoUnits)}건 · ${item.manualUnits === null ? "상위 백분위 미입력" : `상위 ${formatNumber(item.manualUnits)}%`}`
          : `${formatNumber(item.autoUnits)}${item.teamAggregation === "both" ? ` + 상대팀 ${formatNumber(item.otherTeamUnits)}` : ""}${item.manualLabel ? ` + ${item.manualLabel} ${item.manualUnits === null ? "-" : formatNumber(item.manualUnits)}` : ""} = ${formatNumber(item.totalUnits)}건`,
      criteria: managementEvaluationPolicyCriteriaText(item),
      max: Math.max(0, ...item.scoreRules.map(([, score]) => toNumber(score))),
      score: item.score
    }))
  ];

  const partSummaries = {};
  rows.forEach((row) => {
    if (!partSummaries[row.part]) {
      partSummaries[row.part] = { max: row.partMax, actual: 0, pending: 0, count: 0 };
    }
    partSummaries[row.part].count += 1;
    if (row.score === null) partSummaries[row.part].pending += 1;
    else partSummaries[row.part].actual += toNumber(row.score);
  });

  return { rows, partSummaries };
}

function managementEvaluationScoreRulesInputValue(item) {
  return item.scoreRules.map(([minimum, score]) => `${formatNumber(minimum)}:${formatNumber(score)}`).join(", ");
}

function parseManagementEvaluationScoreRules(value, kind) {
  const rows = String(value || "").split(/[,;\n]/)
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => item.split(/[:=]/).map((part) => evaluationNullableNumber(part.trim())))
    .filter((item) => item.length >= 2 && item[0] !== null && item[1] !== null)
    .map(([minimum, score]) => [Math.max(0, minimum), Math.max(0, score)]);
  return normalizeManagementEvaluationScoreRules(rows, kind);
}

function renderManagementEvaluationPolicyInputs(metrics) {
  const container = $("#evaluationPolicyManualInputs");
  if (!container) return;
  const countItems = metrics.policyItems.filter((item) => item.kind === "count");
  const rateItems = metrics.policyItems.filter((item) => item.kind === "rate");
  const percentileItems = metrics.policyItems.filter((item) => item.kind === "percentile");
  const manualFields = [
    ...countItems.filter((item) => item.manualLabel).map((item) => `
      <label>${escapeHtml(item.title)} · ${escapeHtml(item.manualLabel)}
        <input class="evaluation-policy-manual-input" data-policy-manual-id="${escapeHtml(item.id)}" type="number" min="0" step="1" value="${item.manualUnits === null ? "" : escapeHtml(item.manualUnits)}">
      </label>`),
    ...countItems.filter((item) => item.teamAggregation === "both").map((item) => `
      <label class="evaluation-policy-other-team-field"><span>${escapeHtml(item.title)} · 상대팀 실적 <small>수기</small></span>
        <input class="evaluation-policy-other-team-input" data-policy-other-team-id="${escapeHtml(item.id)}" type="number" min="0" step="1" value="${item.otherTeamUnits ? escapeHtml(item.otherTeamUnits) : (state.managementEvaluationInputs?.[managementEvaluationMonth()]?.policyManual?.[`${item.id}::otherTeam`] ?? "")}" placeholder="상대팀 건수">
      </label>`),
    ...percentileItems.map((item) => `
      <label>${escapeHtml(item.title)} · ${escapeHtml(item.manualLabel || "상위 백분위(%)")}
        <input class="evaluation-policy-manual-input" data-policy-manual-id="${escapeHtml(item.id)}" type="number" min="0" max="100" step="0.1" value="${item.manualUnits === null ? "" : escapeHtml(item.manualUnits)}" placeholder="예: 12.5">
      </label>`)
  ].join("");
  const countPreviews = countItems.map((item) => `<div><span>${escapeHtml(item.title)}</span><strong>자동 ${formatNumber(item.autoUnits)}${item.teamAggregation === "both" ? ` · 상대팀 ${formatNumber(item.otherTeamUnits)}` : ""} · 합계 ${formatNumber(item.totalUnits)}건</strong></div>`).join("");
  const ratePreviews = rateItems.map((item) => `<div><span>${escapeHtml(item.title)}</span><strong>${formatNumber(item.autoUnits)} / ${formatNumber(item.goal)} (${managementEvaluationFormatRate(item.rate)})</strong></div>`).join("");
  const percentilePreviews = percentileItems.map((item) => `<div><span>${escapeHtml(item.title)}</span><strong>정책영업 ${formatNumber(item.autoUnits)}건 · ${item.manualUnits === null ? "상위 백분위 입력 필요" : `상위 ${formatNumber(item.manualUnits)}% · ${formatNumber(item.score)}점`}</strong></div>`).join("");
  container.innerHTML = `
    <div class="evaluation-policy-manual">${manualFields || '<span class="muted">수기 입력 항목 없음</span>'}</div>
    <div class="evaluation-policy-previews">${countPreviews}${ratePreviews}${percentilePreviews}</div>`;
}

function evaluationPolicyProductRowsMarkup(type, rules) {
  return rules.map((rule) => `
    <div class="evaluation-product-rule-editor" data-evaluation-product-kind="${type}" data-evaluation-product-id="${escapeHtml(rule.id)}">
      <input class="evaluation-product-rule-title" value="${escapeHtml(rule.title)}" aria-label="제품군명">
      <input class="evaluation-product-rule-keywords" value="${escapeHtml(rule.keywords.join(', '))}" aria-label="모델 또는 포함문구">
      <button class="ghost-button small remove-evaluation-product-rule" type="button">삭제</button>
    </div>`).join("");
}

function renderManagementEvaluationPolicySettings(month = managementEvaluationMonth()) {
  const container = $("#evaluationPolicySettings");
  if (!container) return;
  const policy = managementEvaluationPolicy(month);
  const policyRows = policy.policyItems.map((item, index) => `
    <article class="evaluation-policy-card" data-evaluation-policy-id="${escapeHtml(item.id)}">
      <div class="evaluation-policy-card-head">
        <div class="evaluation-policy-card-title">
          <span class="evaluation-policy-index">${index + 1}</span>
          <strong>${escapeHtml(item.title || "항목명을 입력하세요")}</strong>
        </div>
        <button class="ghost-button small remove-evaluation-policy-item" type="button">삭제</button>
      </div>

      <div class="evaluation-policy-core-grid">
        <label class="policy-title-field">항목명
          <input class="evaluation-policy-title" value="${escapeHtml(item.title)}" placeholder="예: 매트리스 케어">
        </label>
        <label>평가방식
          <select class="evaluation-policy-kind">
            <option value="count"${item.kind === "count" ? " selected" : ""}>수량 자동집계</option>
            <option value="rate"${item.kind === "rate" ? " selected" : ""}>목표 달성률</option>
            <option value="percentile"${item.kind === "percentile" ? " selected" : ""}>상위 백분위(수기)</option>
          </select>
        </label>
        <label>실적 집계 대상
          <select class="evaluation-policy-category-filter">
            ${[
              ["all","전체 접수"],["business","영업 전체"],["rental","렌탈(신규+패키지+재렌탈)"],["new-rental","신규+재렌탈"],
              ["new","신규"],["package","패키지"],["rerental","재렌탈"],["cash","일시불"],["membership","멤버십"]
            ].map(([value,label]) => `<option value="${value}"${item.categoryFilter === value ? " selected" : ""}>${label}</option>`).join("")}
          </select>
        </label>
        <label>팀 집계 기준
          <select class="evaluation-policy-team-aggregation">
            <option value="team"${item.teamAggregation !== "both" ? " selected" : ""}>담당팀 기준</option>
            <option value="both"${item.teamAggregation === "both" ? " selected" : ""}>2팀 전체 합산</option>
          </select>
        </label>
        <label>집계단위
          <select class="evaluation-policy-count-basis">
            <option value="record"${item.countBasis !== "product" ? " selected" : ""}>접수행 1건</option>
            <option value="product"${item.countBasis === "product" ? " selected" : ""}>제품수량</option>
          </select>
        </label>
      </div>

      <div class="evaluation-policy-section">
        <span class="evaluation-policy-section-label">상품 조건 <small>필요할 때만 입력</small></span>
        <div class="evaluation-policy-condition-grid">
          <label>모델·포함문구<input class="evaluation-policy-keywords" value="${escapeHtml(item.keywords.join(", "))}" placeholder="예: CRM-, CMS-"></label>
          <label>필수 포함문구<input class="evaluation-policy-required-keywords" value="${escapeHtml(item.requiredKeywords.join(", "))}" placeholder="예: 필수키워드"></label>
          <label>제외문구<input class="evaluation-policy-exclude-keywords" value="${escapeHtml(item.excludeKeywords.join(", "))}" placeholder="예: 프레임"></label>
        </div>
      </div>

      <div class="evaluation-policy-section evaluation-policy-score-grid">
        <label>달성률 목표 기준
          <select class="evaluation-policy-goal-base-select">
            <option value="new"${item.goalBase === "new" ? " selected" : ""}>신규만</option>
            <option value="new-rental"${item.goalBase === "new-rental" ? " selected" : ""}>신규+재렌탈</option>
            <option value="lump-sum"${item.goalBase === "lump-sum" ? " selected" : ""}>일시불</option>
            <option value="general"${item.goalBase === "general" ? " selected" : ""}>전체</option>
          </select>
        </label>
        <label>목표비율(%)
          <input class="evaluation-policy-target-rate-input" type="number" min="0" step="0.1" value="${escapeHtml(item.targetRate)}">
        </label>
        <label>점수방향
          <select class="evaluation-policy-score-mode">
            <option value="at-least"${item.scoreMode !== "at-most" ? " selected" : ""}>기준 이상이면 점수</option>
            <option value="at-most"${item.scoreMode === "at-most" ? " selected" : ""}>기준 이하이면 점수</option>
          </select>
        </label>
        <label class="policy-score-rules-field">점수기준
          <input class="evaluation-policy-score-rules" value="${escapeHtml(managementEvaluationScoreRulesInputValue(item))}" placeholder="예: 2:2, 3:3, 4:4, 5:7">
        </label>
      </div>

      <div class="evaluation-policy-extra-grid">
        <label>수기 추가수량명
          <input class="evaluation-policy-manual-label-input" value="${escapeHtml(item.manualLabel)}" placeholder="일반 수기 추가가 필요할 때만">
        </label>
        <label class="evaluation-policy-manual-switch">
          <input class="evaluation-policy-manual-required" type="checkbox"${item.manualRequired ? " checked" : ""}>
          자동수량에 일반 수기 추가
        </label>
        ${item.teamAggregation === "both" ? '<div class="evaluation-policy-team-note">※ 2팀 전체 합산을 선택하면 평가 화면에서 상대팀 실적을 별도로 수기 입력합니다.</div>' : ""}
      </div>
    </article>`).join("");

  container.innerHTML = `
    <div class="evaluation-policy-settings-block">
      <div class="evaluation-policy-settings-head">
        <div><h3>정책이행 조건 설정</h3><p>항목별로 실적 범위와 팀 집계 방식을 간단하게 설정합니다. 정책마다 담당팀 평가 또는 2팀 합산 평가를 선택할 수 있습니다.</p></div>
        <button class="ghost-button small" id="addEvaluationPolicyItemBtn" type="button">항목 추가</button>
      </div>
      <div class="evaluation-policy-editor-list">${policyRows || '<div class="empty">등록된 정책이행 항목이 없습니다.</div>'}</div>
    </div>
    <div class="evaluation-product-settings-grid">
      <section class="evaluation-policy-settings-block">
        <div class="evaluation-policy-settings-head"><h3>주력상품 모델</h3><button class="ghost-button small" data-add-evaluation-product-rule="primary" type="button">모델 추가</button></div>
        <div class="evaluation-product-rule-list">${evaluationPolicyProductRowsMarkup("primary", policy.primaryProducts)}</div>
      </section>
      <section class="evaluation-policy-settings-block">
        <div class="evaluation-policy-settings-head"><h3>고가상품 모델</h3><button class="ghost-button small" data-add-evaluation-product-rule="high" type="button">모델 추가</button></div>
        <div class="evaluation-product-rule-list">${evaluationPolicyProductRowsMarkup("high", policy.highValueProducts)}</div>
      </section>
    </div>`;
}

function collectManagementEvaluationPolicySettings() {
  const month = managementEvaluationMonth();
  const current = managementEvaluationPolicy(month);
  const policyItems = $$("#evaluationPolicySettings .evaluation-policy-card").map((row) => {
    const rawKind = row.querySelector(".evaluation-policy-kind")?.value || "count";
    const kind = ["count", "rate", "percentile"].includes(rawKind) ? rawKind : "count";
    return {
      id: row.dataset.evaluationPolicyId || uid("evaluation-policy"),
      title: row.querySelector(".evaluation-policy-title")?.value || "",
      description: "",
      kind,
      keywords: evaluationKeywordList(row.querySelector(".evaluation-policy-keywords")?.value),
      requiredKeywords: evaluationKeywordList(row.querySelector(".evaluation-policy-required-keywords")?.value),
      excludeKeywords: evaluationKeywordList(row.querySelector(".evaluation-policy-exclude-keywords")?.value),
      categoryFilter: row.querySelector(".evaluation-policy-category-filter")?.value || "business",
      countBasis: row.querySelector(".evaluation-policy-count-basis")?.value === "product" ? "product" : "record",
      teamAggregation: row.querySelector(".evaluation-policy-team-aggregation")?.value === "both" ? "both" : "team",
      manualLabel: row.querySelector(".evaluation-policy-manual-label-input")?.value || "",
      manualRequired: Boolean(row.querySelector(".evaluation-policy-manual-required")?.checked),
      goalBase: row.querySelector(".evaluation-policy-goal-base-select")?.value || "new-rental",
      targetRate: evaluationNullableNumber(row.querySelector(".evaluation-policy-target-rate-input")?.value),
      scoreMode: row.querySelector(".evaluation-policy-score-mode")?.value === "at-most" ? "at-most" : "at-least",
      scoreRules: parseManagementEvaluationScoreRules(row.querySelector(".evaluation-policy-score-rules")?.value, kind)
    };
  });
  const readProductRules = (type) => $$("#evaluationPolicySettings .evaluation-product-rule-editor")
    .filter((row) => row.dataset.evaluationProductKind === type)
    .map((row) => {
      const id = row.dataset.evaluationProductId || uid(`evaluation-${type}`);
      const previous = (type === "primary" ? current.primaryProducts : current.highValueProducts).find((item) => item.id === id);
      return {
        id,
        title: row.querySelector(".evaluation-product-rule-title")?.value || "",
        keywords: evaluationKeywordList(row.querySelector(".evaluation-product-rule-keywords")?.value),
        excludeKeywords: previous?.excludeKeywords || []
      };
    })
    .filter((rule) => rule.title || rule.keywords.length);
  state.managementEvaluationPolicies[month] = normalizeManagementEvaluationPolicy({
    ...current,
    policyItems,
    primaryProducts: readProductRules("primary"),
    highValueProducts: readProductRules("high")
  }, month);
  return state.managementEvaluationPolicies[month];
}


function evaluationPartLabelClass(part) {
  return ({"영업관리":"business","계정관리":"account","조직관리":"organization","고객서비스관리":"service","정책이행":"policy"})[part] || "general";
}

function renderEvaluationPartScoreCards(partSummaries) {
  const container = $("#evaluationPartScoreCards");
  if (!container) return;
  const order = ["영업관리","계정관리","조직관리","고객서비스관리","정책이행"];
  const cards = order.filter((part) => partSummaries[part]).map((part) => {
    const item = partSummaries[part];
    const pending = item.pending ? `<span class="evaluation-part-card-pending">수기 ${formatNumber(item.pending)}개 대기</span>` : '<span class="evaluation-part-card-pending ok">자동 계산 완료</span>';
    return `<article class="evaluation-part-card ${evaluationPartLabelClass(part)}">\n      <div class="evaluation-part-card-head"><span>${escapeHtml(part)}</span><strong>${formatNumber(item.actual)} / ${formatNumber(item.max)}</strong></div>\n      <div class="evaluation-part-card-meta"><b>${item.max ? Math.round((toNumber(item.actual) / toNumber(item.max)) * 100) : 0}%</b>${pending}</div>\n    </article>`;
  }).join("");
  container.innerHTML = cards || '<div class="empty">점수 요약이 없습니다.</div>';
}

function renderEvaluationScoreMobileList(rows, partSummaries) {
  const container = $("#evaluationScoreMobileList");
  if (!container) return;
  const grouped = rows.reduce((acc, row) => {
    (acc[row.part] ||= []).push(row);
    return acc;
  }, {});
  const order = ["영업관리","계정관리","조직관리","고객서비스관리","정책이행"];
  container.innerHTML = order.filter((part) => grouped[part]?.length).map((part) => {
    const summary = partSummaries[part] || { actual: 0, max: 0, pending: 0 };
    const items = grouped[part].map((row) => `\n      <div class="evaluation-score-mobile-item">\n        <div class="evaluation-score-mobile-item-head">\n          <strong>${escapeHtml(row.item)}</strong>\n          <b>${row.score === null ? '-' : formatNumber(row.score)} / ${formatNumber(row.max)}</b>\n        </div>\n        <div class="evaluation-score-mobile-line"><span>현재수치</span><strong>${escapeHtml(row.value)}</strong></div>\n        <div class="evaluation-score-mobile-line"><span>평가기준</span><strong>${escapeHtml(row.criteria)}</strong></div>\n      </div>`).join("");
    return `<section class="evaluation-score-mobile-group ${evaluationPartLabelClass(part)}">\n      <div class="evaluation-score-mobile-group-head">\n        <div><span>${escapeHtml(part)}</span><strong>${formatNumber(summary.actual)} / ${formatNumber(summary.max)}</strong></div>\n        <em>${summary.pending ? `수기 ${formatNumber(summary.pending)}개 대기` : '확인 완료'}</em>\n      </div>\n      <div class="evaluation-score-mobile-items">${items}</div>\n    </section>`;
  }).join("");
}

function syncEvaluationPolicySettingsVisibility() {
  const panel = $("#evaluationPolicySettingsPanel");
  const body = $("#evaluationPolicySettingsBody");
  const button = $("#evaluationPolicySettingsToggle");
  if (!panel || !body || !button) return;
  if (window.innerWidth <= 850 && !panel.dataset.collapseInitialized) {
    panel.classList.add("collapsed");
    panel.dataset.collapseInitialized = "1";
  }
  const collapsed = panel.classList.contains("collapsed");
  body.hidden = collapsed;
  button.textContent = collapsed ? "기준 편집 열기" : "기준 편집 숨기기";
  button.setAttribute("aria-expanded", collapsed ? "false" : "true");
}

function setupEvaluationViewUx() {
  if (!document.body || document.body.dataset.evaluationUxBound === "1") return;
  document.body.dataset.evaluationUxBound = "1";
  document.addEventListener("click", (event) => {
    const jumpBtn = event.target.closest("[data-evaluation-jump]");
    if (jumpBtn) {
      const target = document.getElementById(jumpBtn.dataset.evaluationJump || "");
      if (target) target.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    const toggle = event.target.closest("#evaluationPolicySettingsToggle");
    if (toggle) {
      $("#evaluationPolicySettingsPanel")?.classList.toggle("collapsed");
      syncEvaluationPolicySettingsVisibility();
      return;
    }
  });
  syncEvaluationPolicySettingsVisibility();
}

function renderManagementEvaluation() {
  const monthInput = $("#evaluationMonthInput");
  if (!monthInput) return;
  if (!monthInput.value) monthInput.value = $("#monthFilter")?.value || monthIso();
  const month = monthInput.value;
  const metrics = managementEvaluationMetrics(month);
  const input = metrics.input;

  const valueForInput = (value) => value === null ? "" : value;
  const inputMap = {
    evaluationPrimaryRelativeScoreInput: input.primaryRelativeScore,
    evaluationRetentionRateInput: input.retentionRate,
    evaluationCancellationRateInput: input.cancellationRate,
    evaluationManagerChangeInput: input.managerChange,
    evaluationInspectionTotalInput: input.inspectionTotalAccount,
    evaluationInspectionCancelInput: input.inspectionCancel,
    evaluationInspectionExceptionHoldInput: input.inspectionExceptionHold,
    evaluationInspectionHoldInput: input.inspectionHold,
    evaluationInspectionCompletedInput: input.inspectionCompleted,
    evaluationHappyTalkRateInput: input.happyTalkRate
  };
  Object.entries(inputMap).forEach(([id, value]) => {
    const node = $("#" + id);
    if (node) node.value = valueForInput(value);
  });

  if ($("#evaluationPeriodLabel")) $("#evaluationPeriodLabel").textContent = `${metrics.period.start} ~ ${metrics.period.end}`;
  if ($("#evaluationScoreTotal")) $("#evaluationScoreTotal").textContent = formatNumber(metrics.currentScore);
  if ($("#evaluationScoreMax")) $("#evaluationScoreMax").textContent = formatNumber(metrics.maxScore);
  if ($("#evaluationPendingCount")) $("#evaluationPendingCount").textContent = `${metrics.pendingInputs}개`;
  if ($("#evaluationPrimaryTotal")) $("#evaluationPrimaryTotal").textContent = `${formatNumber(metrics.primaryStats.total)}건`;
  if ($("#evaluationSalesTotal")) $("#evaluationSalesTotal").textContent = `${formatNumber(metrics.overallActual)}건`;

  const body = $("#evaluationScoreBody");
  const { rows, partSummaries } = managementEvaluationRows(metrics);
  if (body) {
    const renderedParts = new Set();
    const partClassNames = {
      "영업관리": "business",
      "계정관리": "account",
      "조직관리": "organization",
      "고객서비스관리": "service",
      "정책이행": "policy"
    };

    body.innerHTML = rows.map((row) => {
      const part = partSummaries[row.part];
      const partClassName = partClassNames[row.part] || "general";
      const isPartStart = !renderedParts.has(row.part);
      let partCells = "";
      if (isPartStart) {
        renderedParts.add(row.part);
        const pendingText = part.pending ? `<small>${part.pending}개 대기</small>` : "";
        partCells = `
          <td class="evaluation-part-name" rowspan="${part.count}"><strong>${escapeHtml(row.part)}</strong></td>
          <td class="evaluation-part-max" rowspan="${part.count}">${formatNumber(part.max)}</td>
          <td class="evaluation-part-score" rowspan="${part.count}"><strong>${formatNumber(part.actual)}</strong><span>/ ${formatNumber(part.max)}</span>${pendingText}</td>`;
      }

      return `<tr class="evaluation-score-row evaluation-part-${partClassName}${isPartStart ? " evaluation-part-start" : ""}">
        ${partCells}
        <td class="evaluation-item-name">${escapeHtml(row.item)}</td>
        <td class="evaluation-current-value">${escapeHtml(row.value)}</td>
        <td class="evaluation-criteria">${escapeHtml(row.criteria)}</td>
        <td class="evaluation-item-max">${formatNumber(row.max)}</td>
        <td class="evaluation-score-cell">${row.score === null ? "-" : formatNumber(row.score)}</td>
      </tr>`;
    }).join("");
  }

  const primaryBody = $("#evaluationPrimaryProductBody");
  if (primaryBody) {
    primaryBody.innerHTML = metrics.primaryStats.models.length
      ? metrics.primaryStats.models.map((item, index) => `<tr><td>${index + 1}</td><td>${escapeHtml(item.model)}</td><td class="evaluation-product-count-cell">${formatNumber(item.count)}개</td></tr>`).join("")
      : `<tr><td colspan="3" class="empty">해당월 주력상품 신규 건수가 없습니다.</td></tr>`;
  }

  const primaryTotal = formatNumber(metrics.primaryStats.total);
  if ($("#evaluationPrimaryProductTotal")) $("#evaluationPrimaryProductTotal").textContent = `총 ${primaryTotal}개`;
  if ($("#evaluationPrimaryProductTotalCell")) $("#evaluationPrimaryProductTotalCell").textContent = `${primaryTotal}개`;

  const highValueBody = $("#evaluationHighValueProductBody");
  if (highValueBody) {
    highValueBody.innerHTML = metrics.highValueStats.models.length
      ? metrics.highValueStats.models.map((item, index) => `<tr><td>${index + 1}</td><td>${escapeHtml(item.model)}</td><td class="evaluation-product-count-cell">${formatNumber(item.count)}개</td></tr>`).join("")
      : `<tr><td colspan="3" class="empty">해당월 고가상품 건수가 없습니다.</td></tr>`;
  }

  const highValueTotal = formatNumber(metrics.highValueStats.total);
  if ($("#evaluationHighValueTotal")) $("#evaluationHighValueTotal").textContent = `총 ${highValueTotal}개`;
  if ($("#evaluationHighValueTotalCell")) $("#evaluationHighValueTotalCell").textContent = `${highValueTotal}개`;

  if ($("#evaluationInspectionRatePreview")) $("#evaluationInspectionRatePreview").textContent = managementEvaluationFormatRate(metrics.inspectionRate);
  if ($("#evaluationManagerCountPreview")) $("#evaluationManagerCountPreview").textContent = `${formatNumber(metrics.activeManagerCount)}명`;
  if ($("#evaluationManagerFillPreview")) $("#evaluationManagerFillPreview").textContent = managementEvaluationFormatRate(metrics.managerFillRate);
  if ($("#evaluationMembershipPreview")) $("#evaluationMembershipPreview").textContent = `${formatNumber(metrics.membershipCount)}건`;
  renderManagementEvaluationPolicyInputs(metrics);
  renderManagementEvaluationPolicySettings(month);
  renderEvaluationPartScoreCards(partSummaries);
  renderEvaluationScoreMobileList(rows, partSummaries);
  setupEvaluationViewUx();
  syncEvaluationPolicySettingsVisibility();
}

function collectManagementEvaluationInput() {
  const month = managementEvaluationMonth();
  const read = (id) => evaluationNullableNumber($("#" + id)?.value);
  const current = managementEvaluationInput(month);
  const policyManual = { ...(current.policyManual || {}) };
  $$("#evaluationPolicyManualInputs [data-policy-manual-id]").forEach((node) => {
    policyManual[node.dataset.policyManualId] = evaluationNullableNumber(node.value);
  });
  $$("#evaluationPolicyManualInputs [data-policy-other-team-id]").forEach((node) => {
    policyManual[`${node.dataset.policyOtherTeamId}::otherTeam`] = evaluationNullableNumber(node.value);
  });
  state.managementEvaluationInputs[month] = normalizeManagementEvaluationInput({
    ...current,
    primaryRelativeScore: read("evaluationPrimaryRelativeScoreInput"),
    retentionRate: read("evaluationRetentionRateInput"),
    cancellationRate: read("evaluationCancellationRateInput"),
    managerChange: read("evaluationManagerChangeInput"),
    inspectionTotalAccount: read("evaluationInspectionTotalInput"),
    inspectionCancel: read("evaluationInspectionCancelInput"),
    inspectionExceptionHold: read("evaluationInspectionExceptionHoldInput"),
    inspectionHold: read("evaluationInspectionHoldInput"),
    inspectionCompleted: read("evaluationInspectionCompletedInput"),
    happyTalkRate: read("evaluationHappyTalkRateInput"),
    policyManual
  });
}

function saveManagementEvaluationInput() {
  collectManagementEvaluationInput();
  persistState();
  renderManagementEvaluation();
  showToast("경영평가 입력값과 예상점수를 저장했습니다.");
}

function printManagementEvaluation() {
  const source = $("#evaluationPrintArea");
  if (!source) return;

  const clone = source.cloneNode(true);
  clone.querySelectorAll("button").forEach((node) => node.remove());
  clone.querySelectorAll("input, select, textarea").forEach((input) => {
    const span = document.createElement("span");
    span.className = "evaluation-print-value";
    if (input.tagName === "SELECT") {
      span.textContent = input.options?.[input.selectedIndex]?.textContent || input.value || "-";
    } else {
      span.textContent = input.value || "-";
    }
    input.replaceWith(span);
  });

  const meta = state.appMeta || sampleState.appMeta;
  const month = managementEvaluationMonth();
  const period = monthPeriod(month);
  const printedAt = new Date().toLocaleString("ko-KR");
  const branchName = meta.branchName || "지국명 미설정";
  const masterLine = [meta.masterName, meta.masterRole].filter(Boolean).join(" · ");

  const summary = clone.querySelector(".evaluation-summary-panel");
  const inputPanel = clone.querySelector(".evaluation-input-panel");
  const scorePanel = clone.querySelector(".evaluation-score-panel");
  const scoreMobileList = clone.querySelector("#evaluationScoreMobileList");
  const productGrid = clone.querySelector(".evaluation-product-tables-grid");
  const policy = managementEvaluationPolicy(month);

  const esc = (value) => escapeHtml(value === null || value === undefined || value === "" ? "-" : String(value));
  const joinKeywords = (arr) => Array.isArray(arr) && arr.length ? arr.join(", ") : "-";
  const rulesText = (rules) => Array.isArray(rules) && rules.length
    ? rules.map(([minimum, score]) => `${formatNumber(minimum)} → ${formatNumber(score)}점`).join(" · ")
    : "-";

  // 보고서용 수기 입력은 화면의 입력 폼을 그대로 복사하지 않고, 제출 문서에 맞는 간결한 표 형태로 재구성한다.
  const manualItems = [
    ["영업관리", "주력상품 상대평가 예상점수", $("#evaluationPrimaryRelativeScoreInput")?.value ? `${$("#evaluationPrimaryRelativeScoreInput").value}점` : "-"],
    ["계정관리", "유지율", $("#evaluationRetentionRateInput")?.value ? `${$("#evaluationRetentionRateInput").value}%` : "-"],
    ["계정관리", "해지율", $("#evaluationCancellationRateInput")?.value ? `${$("#evaluationCancellationRateInput").value}%` : "-"],
    ["계정관리", "멤버십 건수", $("#evaluationMembershipPreview")?.textContent || "-"],
    ["조직관리", "매니저 전월대비 증감", $("#evaluationManagerChangeInput")?.value ? `${$("#evaluationManagerChangeInput").value}명` : "-"],
    ["조직관리", "현재 매니저수", $("#evaluationManagerCountPreview")?.textContent || "-"],
    ["조직관리", "매니저 충원율", $("#evaluationManagerFillPreview")?.textContent || "-"],
    ["고객서비스관리", "점검 전체계정", $("#evaluationInspectionTotalInput")?.value || "-"],
    ["고객서비스관리", "점검 취소", $("#evaluationInspectionCancelInput")?.value || "-"],
    ["고객서비스관리", "점검 예외보류", $("#evaluationInspectionExceptionHoldInput")?.value || "-"],
    ["고객서비스관리", "점검 보류", $("#evaluationInspectionHoldInput")?.value || "-"],
    ["고객서비스관리", "점검 완료", $("#evaluationInspectionCompletedInput")?.value || "-"],
    ["고객서비스관리", "해피톡 응답률", $("#evaluationHappyTalkRateInput")?.value ? `${$("#evaluationHappyTalkRateInput").value}%` : "-"],
    ["고객서비스관리", "점검처리율", $("#evaluationInspectionRatePreview")?.textContent || "-"],
  ];
  const manualPolicyItems = policy.policyItems.filter((item) => item.manualLabel);
  manualPolicyItems.forEach((item) => {
    const suffix = item.kind === "percentile" ? "%" : "건";
    const value = item.manualUnits === null || item.manualUnits === undefined ? "-" : `${formatNumber(item.manualUnits)}${suffix}`;
    manualItems.push(["정책이행", `${item.title} · ${item.manualLabel}`, value]);
  });

  const manualMarkup = `
    <section class="evaluation-manual-report">
      <div class="report-subheading">수기 입력항목</div>
      <p class="report-intro">평가월에 직접 입력하거나 확인해야 하는 항목입니다.</p>
      <table class="evaluation-manual-table">
        <thead><tr><th>구분</th><th>입력항목</th><th>현재 입력값</th></tr></thead>
        <tbody>${manualItems.map(([part, item, value]) => `<tr><td class="manual-part">${esc(part)}</td><td>${esc(item)}</td><td class="manual-value">${esc(value)}</td></tr>`).join("")}</tbody>
      </table>
    </section>`;

  const policyRows = policy.policyItems.map((item) => `
    <tr>
      <td class="policy-item-title">${esc(item.title)}</td>
      <td>${esc(item.kind === "rate" ? "달성률" : "수량")}</td>
      <td>${esc(joinKeywords(item.keywords))}</td>
      <td>${esc(item.manualLabel || "-")}</td>
      <td>${esc(item.manualRequired ? "합산" : "-")}</td>
      <td>${esc(managementEvaluationGoalBaseLabel(item.goalBase))}</td>
      <td>${item.targetRate === null || item.targetRate === undefined ? "-" : esc(`${formatNumber(item.targetRate)}%`)}</td>
      <td>${esc(rulesText(item.scoreRules))}</td>
    </tr>`).join("");

  const policyTable = `
    <section class="evaluation-policy-report">
      <div class="report-subheading">월별 경영평가 기준</div>
      <p class="report-intro">${esc(formatMonthLabel(month))} 평가기간 <strong>${esc(period.start)} ~ ${esc(period.end)}</strong>에 적용되는 기준입니다.</p>
      <table class="evaluation-policy-table">
        <thead><tr><th>평가항목</th><th>계산</th><th>모델·포함문구</th><th>추가입력명</th><th>수기합산</th><th>목표기준</th><th>목표비율</th><th>점수기준</th></tr></thead>
        <tbody>${policyRows || '<tr><td colspan="8">등록된 정책이행 기준이 없습니다.</td></tr>'}</tbody>
      </table>
    </section>`;

  const productRulesMarkup = `
    <section class="evaluation-policy-product-report">
      <div class="report-subheading">상품별 평가 기준</div>
      <div class="evaluation-policy-product-grid">
        <div><h3>주력상품 모델</h3><table><thead><tr><th>제품군</th><th>모델·포함문구</th></tr></thead><tbody>${(policy.primaryProducts || []).map((r) => `<tr><td>${esc(r.title)}</td><td>${esc(joinKeywords(r.keywords))}</td></tr>`).join("") || '<tr><td colspan="2">등록된 기준 없음</td></tr>'}</tbody></table></div>
        <div><h3>고가상품 모델</h3><table><thead><tr><th>제품군</th><th>모델·포함문구</th></tr></thead><tbody>${(policy.highValueProducts || []).map((r) => `<tr><td>${esc(r.title)}</td><td>${esc(joinKeywords(r.keywords))}</td></tr>`).join("") || '<tr><td colspan="2">등록된 기준 없음</td></tr>'}</tbody></table></div>
      </div>
    </section>`;

  const page = (title, subtitle, bodyMarkup, extraClass = "") => `
    <section class="evaluation-report-page ${extraClass}">
      <header class="evaluation-report-header">
        <div>
          <div class="evaluation-report-kicker">MANAGEMENT EVALUATION REPORT</div>
          <h1>${escapeHtml(title)}</h1>
          <p>${escapeHtml(branchName)}${masterLine ? ` · ${escapeHtml(masterLine)}` : ""}</p>
        </div>
        <div class="evaluation-report-meta"><strong>${escapeHtml(formatMonthLabel(month))}</strong><span>${escapeHtml(period.start)} ~ ${escapeHtml(period.end)}</span></div>
      </header>
      <div class="evaluation-report-body">${subtitle ? `<div class="evaluation-report-section-note">${escapeHtml(subtitle)}</div>` : ""}${bodyMarkup}</div>
      <footer class="evaluation-report-footer"><span>경영평가 예상점수</span><span>출력 ${escapeHtml(printedAt)}</span><strong class="evaluation-page-number"></strong></footer>
    </section>`;

  const pages = [];
  // 1p: summary immediately followed by itemized expected scores.
  let reportScoreMarkup = "";
  if (scoreMobileList && scoreMobileList.innerHTML.trim()) {
    reportScoreMarkup = `
      <section class="evaluation-detail-report">
        <div class="report-subheading">항목별 예상점수</div>
        <table class="evaluation-detail-table">
          <thead><tr><th>평가항목</th><th>현재점수</th><th>현재수치</th><th>평가기준</th></tr></thead>
          <tbody>${Array.from(scoreMobileList.querySelectorAll('.evaluation-score-mobile-item')).map((node)=>{
            const head=node.querySelector('.evaluation-score-mobile-item-head');
            const lines=node.querySelectorAll('.evaluation-score-mobile-line');
            return `<tr><td>${head?.querySelector('strong')?.textContent || '-'}</td><td>${head?.querySelector('b')?.textContent || '-'}</td><td>${lines[0]?.querySelector('strong')?.textContent || '-'}</td><td>${lines[1]?.querySelector('strong')?.textContent || '-'}</td></tr>`;
          }).join('')}</tbody>
        </table>
      </section>`;
  } else {
    reportScoreMarkup = scorePanel?.outerHTML || "";
  }
  pages.push(page(
    "경영평가 예상점수",
    "선택 평가월의 전체 예상점수와 항목별 예상점수",
    `${summary?.outerHTML || ""}${reportScoreMarkup}`,
    "evaluation-report-first"
  ));
  // 2p: product status only.
  pages.push(page(
    "주력·고가상품 현황",
    "평가에 반영되는 주력상품 및 고가상품 현황",
    productGrid?.outerHTML || '<div class="report-empty">상품 현황이 없습니다.</div>',
    "evaluation-report-products"
  ));
  // 3p: manual inputs.
  pages.push(page(
    "수기 입력항목",
    "평가월에 직접 입력하거나 확인해야 하는 항목",
    manualMarkup,
    "evaluation-report-manual"
  ));
  // 4p+: policy criteria in readable tables, split to avoid one long raw-text page.
  const chunkSize = 4;
  const chunks = [];
  for (let i = 0; i < policy.policyItems.length; i += chunkSize) chunks.push(policy.policyItems.slice(i, i + chunkSize));
  if (!chunks.length) chunks.push([]);
  chunks.forEach((chunk, idx) => {
    const rows = chunk.map((item) => `
      <tr><td class="policy-item-title">${esc(item.title)}</td><td>${esc(item.kind === "rate" ? "달성률" : item.kind === "percentile" ? "상위백분위" : "수량")}</td><td>${esc(managementEvaluationPolicyCategoryLabel(item.categoryFilter))}</td><td>${esc(joinKeywords(item.keywords))}</td><td>${esc(joinKeywords(item.requiredKeywords))}</td><td>${esc(joinKeywords(item.excludeKeywords))}</td><td>${esc(item.description || "-")}</td><td>${esc(managementEvaluationPolicyCriteriaText(item))}</td></tr>`).join("");
    const criteria = `<section class="evaluation-policy-report"><div class="report-subheading">월별 경영평가 기준${chunks.length > 1 ? ` · ${idx + 1}` : ""}</div><p class="report-intro">${esc(formatMonthLabel(month))} · ${esc(period.start)} ~ ${esc(period.end)}</p><table class="evaluation-policy-table"><thead><tr><th>평가항목</th><th>계산</th><th>판매종류</th><th>포함문구</th><th>필수문구</th><th>제외문구</th><th>정책설명</th><th>점수기준</th></tr></thead><tbody>${rows || '<tr><td colspan="8">등록된 정책이행 기준이 없습니다.</td></tr>'}</tbody></table></section>`;
    const isLast = idx === chunks.length - 1;
    pages.push(page("월별 경영평가 기준", "정책이행 및 상품별 평가 기준", criteria + (isLast ? productRulesMarkup : ""), "evaluation-report-policy"));
  });

  const frame = document.createElement("iframe");
  frame.id = "evaluation-print-frame";
  frame.style.position = "fixed"; frame.style.width = "0"; frame.style.height = "0"; frame.style.border = "0";
  document.body.appendChild(frame);
  const doc = frame.contentWindow.document;
  doc.open();
  doc.write(`<!doctype html><html lang="ko"><head><meta charset="utf-8"><title>SK 영업관리 경영평가 보고서</title>
<style>
@page{size:A4 portrait;margin:0}*{box-sizing:border-box;-webkit-print-color-adjust:exact;print-color-adjust:exact}html,body{margin:0;padding:0;background:#fff;color:#17231e;font-family:"Malgun Gothic",Arial,sans-serif}body{font-size:9pt;line-height:1.35}.evaluation-report-page{position:relative;width:210mm;height:297mm;padding:13mm 13mm 12mm;overflow:hidden;background:#fff;break-after:page;page-break-after:always}.evaluation-report-page:last-child{break-after:auto;page-break-after:auto}.evaluation-report-header{height:24mm;display:flex;justify-content:space-between;align-items:flex-end;gap:10mm;padding-bottom:4mm;border-bottom:2px solid #214b3b;margin-bottom:5mm}.evaluation-report-kicker{color:#527b69;font-size:7pt;font-weight:900;letter-spacing:.16em;margin-bottom:1.2mm}.evaluation-report-header h1{margin:0;font-size:20pt;line-height:1.1;color:#173a2e;letter-spacing:-.04em}.evaluation-report-header p{margin:2mm 0 0;color:#5b6c64;font-size:8pt;font-weight:700}.evaluation-report-meta{min-width:42mm;text-align:right}.evaluation-report-meta strong{display:block;font-size:11pt;color:#173a2e}.evaluation-report-meta span{display:block;margin-top:1mm;color:#5b6c64;font-size:7.5pt;font-weight:700}.evaluation-report-section-note{margin:0 0 3mm;padding:2mm 3mm;border-left:3px solid #4b8069;background:#f1f6f3;color:#3d5148;font-size:8pt;font-weight:750}.evaluation-report-body{height:243mm;overflow:hidden}.evaluation-report-footer{position:absolute;left:13mm;right:13mm;bottom:5mm;padding-top:2mm;border-top:1px solid #c5d0cb;display:grid;grid-template-columns:1fr 1fr 12mm;gap:3mm;color:#708078;font-size:6.8pt}.evaluation-report-footer span:nth-child(2){text-align:center}.evaluation-report-footer strong{text-align:right;color:#214b3b}.panel{border:1px solid #b9c7c0;border-radius:3px;background:#fff;box-shadow:none;margin:0 0 4mm;overflow:hidden}.panel-head{display:flex;justify-content:space-between;align-items:center;padding:2.2mm 3mm;border-bottom:1px solid #c8d2cd;background:#f0f5f2}.panel-head h2{margin:0;font-size:10pt;color:#1c4032;font-weight:900}.panel-head strong,.panel-head span{color:#53655d;font-size:7.5pt;font-weight:800}.evaluation-summary-grid{display:grid;grid-template-columns:1.35fr repeat(3,1fr);gap:2.2mm;padding:2.5mm}.evaluation-summary-card{min-height:21mm;padding:2.6mm;border:1px solid #c2cec8;border-radius:3px;background:#fbfcfb;text-align:center}.evaluation-summary-card.main{background:#eef6f1;border-color:#7ca18e}.evaluation-summary-card span{display:block;color:#5b6b63;font-size:7.2pt;font-weight:800}.evaluation-summary-card strong{display:block;margin-top:1.8mm;color:#173a2e;font-size:14pt;line-height:1;font-weight:950}.evaluation-summary-card.main strong{font-size:18pt}.evaluation-score-panel{margin-top:3mm}.evaluation-score-table{width:100%;border-collapse:collapse;table-layout:fixed}.evaluation-score-table th,.evaluation-score-table td{border:1px solid #bcc7c2;padding:1.25mm .8mm;text-align:center;vertical-align:middle;overflow:hidden}.evaluation-score-table th{background:#edf3f0;color:#234536;font-size:6.7pt;font-weight:900}.evaluation-score-table td{font-size:6.5pt;font-weight:700;color:#25342e}.evaluation-score-table th:nth-child(1){width:14mm}.evaluation-score-table th:nth-child(2){width:14mm}.evaluation-score-table th:nth-child(3){width:16mm}.evaluation-score-table th:nth-child(4){width:31mm}.evaluation-score-table th:nth-child(5){width:27mm}.evaluation-score-table th:nth-child(6){width:48mm}.evaluation-score-table th:nth-child(7){width:16mm}.evaluation-score-table th:nth-child(8){width:16mm}.evaluation-part-name{background:#f5f8f6;font-weight:900;color:#214b3b}.evaluation-part-max,.evaluation-part-score{background:#f9fbfa}.evaluation-part-score strong{display:block;font-size:8.5pt}.evaluation-part-score span,.evaluation-part-score small{display:block;color:#66766e;font-size:5.8pt}.evaluation-score-cell{font-size:8.5pt;font-weight:950;color:#173a2e}.evaluation-detail-table{width:100%;border-collapse:collapse;table-layout:fixed;margin-bottom:4mm}.evaluation-detail-table th,.evaluation-detail-table td{border:1px solid #bcc7c2;padding:1.8mm 1.2mm;font-size:7.2pt;vertical-align:middle}.evaluation-detail-table th{background:#edf3f0;color:#234536;font-weight:900;text-align:center}.evaluation-detail-table td{text-align:center}.evaluation-detail-table td:first-child{text-align:left;font-weight:900;color:#214b3b}.evaluation-detail-report{margin-top:3mm}.evaluation-detail-report .report-subheading{margin-bottom:2mm}..evaluation-product-tables-grid{display:grid;grid-template-columns:1fr 1fr;gap:4mm}.evaluation-product-tables-grid table,.evaluation-policy-product-report table{width:100%;border-collapse:collapse;table-layout:fixed}.evaluation-product-tables-grid th,.evaluation-product-tables-grid td,.evaluation-policy-product-report th,.evaluation-policy-product-report td{border:1px solid #bcc7c2;padding:1.5mm 1mm;text-align:center;vertical-align:middle;font-size:7pt}.evaluation-product-tables-grid th,.evaluation-policy-product-report th{background:#edf3f0;color:#234536;font-weight:900}.evaluation-product-total-row th,.evaluation-product-total-row td{background:#f0f5f2;font-weight:950}.evaluation-product-count-cell{font-weight:950;color:#173a2e}.evaluation-manual-report,.evaluation-policy-report,.evaluation-policy-product-report{margin:0}.report-subheading{font-size:12pt;font-weight:950;color:#173a2e;padding:2mm 0 2.5mm;border-bottom:2px solid #214b3b;margin-bottom:2.5mm}.report-intro{margin:0 0 3mm;color:#5b6c64;font-size:7.8pt;font-weight:700}.evaluation-manual-table,.evaluation-policy-table{width:100%;border-collapse:collapse;table-layout:fixed}.evaluation-manual-table th,.evaluation-manual-table td,.evaluation-policy-table th,.evaluation-policy-table td{border:1px solid #bcc7c2;padding:1.7mm 1.2mm;vertical-align:middle}.evaluation-manual-table th,.evaluation-policy-table th{background:#edf3f0;color:#234536;font-size:7pt;font-weight:900;text-align:center}.evaluation-manual-table td{font-size:7.4pt}.evaluation-manual-table th:nth-child(1){width:32mm}.evaluation-manual-table th:nth-child(2){width:auto}.evaluation-manual-table th:nth-child(3){width:38mm}.manual-part{background:#f7faf8;font-weight:900;color:#214b3b}.manual-value{text-align:center;font-weight:950;color:#173a2e}.evaluation-policy-table{font-size:6.6pt}.evaluation-policy-table th,.evaluation-policy-table td{padding:1.5mm .9mm;text-align:center;overflow-wrap:anywhere}.evaluation-policy-table th:nth-child(1){width:27mm}.evaluation-policy-table th:nth-child(2){width:15mm}.evaluation-policy-table th:nth-child(3){width:40mm}.evaluation-policy-table th:nth-child(4){width:27mm}.evaluation-policy-table th:nth-child(5){width:17mm}.evaluation-policy-table th:nth-child(6){width:24mm}.evaluation-policy-table th:nth-child(7){width:18mm}.evaluation-policy-table th:nth-child(8){width:auto}.policy-item-title{font-weight:900;color:#214b3b;background:#f7faf8}.evaluation-policy-product-report{margin-top:5mm}.evaluation-policy-product-report h3{margin:0 0 1.5mm;font-size:8.5pt;color:#214b3b}.evaluation-policy-product-grid{display:grid;grid-template-columns:1fr 1fr;gap:4mm}.evaluation-print-value{font-weight:900}.report-empty{padding:12mm;text-align:center;color:#718078;border:1px dashed #b9c7c0}.evaluation-report-first .evaluation-score-panel{margin-bottom:0}.evaluation-report-policy .evaluation-policy-report{margin-bottom:0}@media print{.evaluation-report-page{break-inside:avoid;page-break-inside:avoid}}

/* V10.70 Evaluation Report Design Upgrade */
.evaluation-report-first .evaluation-summary-grid{grid-template-columns:1.6fr repeat(3,1fr);gap:3mm;}
.evaluation-report-first .evaluation-summary-card{border-radius:8px;padding:4mm;min-height:25mm;background:#fff;}
.evaluation-report-first .evaluation-summary-card.main{background:linear-gradient(135deg,#e8f3ff,#f7fbff);border:2px solid #2f6fb5;}
.evaluation-report-first .evaluation-summary-card.main strong{font-size:24pt;color:#1559a8;}
.evaluation-report-first .evaluation-score-panel{border-radius:8px;overflow:hidden;}
.evaluation-report-first .evaluation-score-table th{background:#245d96;color:white;font-size:8pt;padding:2mm;}
.evaluation-report-first .evaluation-score-table td{font-size:7.5pt;padding:2mm 1mm;}
.evaluation-report-first .evaluation-score-table .evaluation-part-score strong{font-size:11pt;color:#d62828;}
.evaluation-report-first .evaluation-score-table tr:nth-child(even){background:#f8fbff;}
.evaluation-report-first .evaluation-score-cell{font-size:11pt;color:#1d5fa7;font-weight:950;}
.evaluation-report-first .evaluation-score-table td:nth-child(7){font-weight:900;}

</style></head><body><div class="evaluation-report-pages">${pages.join("\n")}</div><script>(function(){function fit(){document.querySelectorAll('.evaluation-report-page').forEach(function(page){var body=page.querySelector('.evaluation-report-body');if(!body)return;body.style.transform='';body.style.transformOrigin='';body.style.width='';var h=body.scrollHeight,ah=body.clientHeight;if(h>ah&&ah>0){var scale=Math.max(.68,ah/h);body.style.transform='scale('+scale+')';body.style.transformOrigin='top left';body.style.width=(100/scale)+'%';}});var pages=document.querySelectorAll('.evaluation-report-page');pages.forEach(function(p,i){var n=p.querySelector('.evaluation-page-number');if(n)n.textContent=(i+1)+' / '+pages.length;});setTimeout(function(){window.focus();window.print();},250)}window.addEventListener('load',function(){setTimeout(fit,120)})})();<\/script></body></html>`);
  doc.close();
  frame.contentWindow.onafterprint = () => setTimeout(() => frame.remove(), 500);
}

function normalizePayrollRecord(raw = {}) {
  return {
    seller: String(raw.seller || "").trim(),
    customerNo: String(raw.customerNo || "").trim(),
    customerName: String(raw.customerName || "").trim(),
    product: String(raw.product || "").trim(),
    baseFee: toNumber(raw.baseFee),
    salesActivation: toNumber(raw.salesActivation),
    additionalFee: toNumber(raw.additionalFee),
    category: String(raw.category || "").trim(),
    quantity: toNumber(raw.quantity),
    receivedDate: String(raw.receivedDate || "").trim(),
    installCompleteDate: String(raw.installCompleteDate || "").trim(),
    previousCustomerNo: String(raw.previousCustomerNo || "").trim(),
    isRefund: Boolean(raw.isRefund),
    matched: Boolean(raw.matched),
    matchedManager: String(raw.matchedManager || "").trim(),
    matchedSeller: String(raw.matchedSeller || "").trim(),
    numberMismatch: Boolean(raw.numberMismatch),
    matchedCustomerNo: String(raw.matchedCustomerNo || "").trim()
  };
}

function normalizePayrollArchive(raw = {}) {
  const records = Array.isArray(raw.records) ? raw.records.map(normalizePayrollRecord) : [];
  const unmatchedRecords = Array.isArray(raw.unmatchedRecords) ? raw.unmatchedRecords.map(normalizePayrollRecord) : [];
  return {
    id: String(raw.id || uid("payroll")).trim(),
    month: String(raw.month || "").trim(),
    manager: String(raw.manager || "").trim(),
    savedAt: String(raw.savedAt || "").trim(),
    records,
    unmatchedRecords
  };
}

function payrollArchiveLabel(archive) {
  const month = String(archive.month || "").trim();
  const manager = String(archive.manager || "").trim() || "미지정";
  return `${month ? month.replace("-", "년 ") + "월" : "급여월 미지정"} · ${manager}`;
}

function payrollHeaderValue(row, header, labels = []) {
  const wanted = labels.map((label) => normalizeImportHeader(label));
  const entry = Object.entries(header || {}).find(([, value]) => wanted.includes(normalizeImportHeader(value)));
  return entry ? row?.[entry[0]] ?? "" : "";
}

function payrollNumber(value) {
  if (value === null || value === undefined || value === "") return 0;
  const text = String(value).replace(/,/g, "").replace(/원/g, "").trim();
  const n = Number(text);
  return Number.isFinite(n) ? n : 0;
}

function payrollCustomerKey(value) {
  return String(value || "").replace(/[^0-9]/g, "");
}

function payrollNameKey(value) {
  return String(value || "").normalize("NFKC").replace(/\s+/g, "").trim();
}

function payrollProductKey(value) {
  return String(value || "").normalize("NFKC").replace(/\s+/g, "").toUpperCase().trim();
}

function payrollMaskedNameMatch(sourceName, targetName) {
  const source = payrollNameKey(sourceName);
  const target = payrollNameKey(targetName);
  if (!source || !target) return false;
  if (source === target) return true;
  if (!source.includes("*")) return false;
  if (source.length !== target.length) return false;
  for (let i = 0; i < source.length; i += 1) {
    if (source[i] !== "*" && source[i] !== target[i]) return false;
  }
  return true;
}

function resolvePayrollSeller(customerNo, customerName, product, defaultManager) {
  const key = payrollCustomerKey(customerNo);
  const records = Array.isArray(state.records) ? state.records : [];
  const sortedByDate = (items) => [...items].sort((a, b) =>
    String(b.receivedDate || "").localeCompare(String(a.receivedDate || ""))
  );

  // 1) 급여 엑셀의 고객번호가 최우선 기준
  const exactMatches = key
    ? sortedByDate(records.filter((record) => payrollCustomerKey(record.customerNo) === key))
    : [];

  if (exactMatches.length) {
    const base = exactMatches[0];
    const sellerRecord = exactMatches.find((record) => String(record.seller || "").trim());
    const seller = String(sellerRecord?.seller || "").trim() || String(defaultManager || "").trim();
    return {
      seller: seller || "미지정",
      matched: true,
      manager: String(base.manager || defaultManager || "").trim(),
      matchedSeller: String(sellerRecord?.seller || "").trim(),
      customerName: String(base.customerName || customerName || "").trim(),
      matchedCustomerNo: String(base.customerNo || "").trim(),
      numberMismatch: false
    };
  }

  // 2) 고객번호가 다르면 고객명 + 상품명으로 보조 매칭
  const fallbackMatches = sortedByDate(records.filter((record) =>
    payrollMaskedNameMatch(customerName, record.customerName) &&
    payrollProductKey(product) === payrollProductKey(record.product)
  ));

  if (fallbackMatches.length) {
    const base = fallbackMatches[0];
    const sellerRecord = fallbackMatches.find((record) => String(record.seller || "").trim());
    const seller = String(sellerRecord?.seller || "").trim() || String(defaultManager || "").trim();
    return {
      seller: seller || "미지정",
      matched: true,
      manager: String(base.manager || defaultManager || "").trim(),
      matchedSeller: String(sellerRecord?.seller || "").trim(),
      customerName: String(base.customerName || customerName || "").trim(),
      matchedCustomerNo: String(base.customerNo || "").trim(),
      numberMismatch: true
    };
  }

  // 3) 완전 미매칭이면 현재 불러오는 주매니저에게 귀속
  return {
    seller: String(defaultManager || "").trim() || "미지정",
    matched: false,
    manager: String(defaultManager || "").trim(),
    matchedSeller: "",
    customerName: String(customerName || "").trim(),
    matchedCustomerNo: "",
    numberMismatch: false
  };
}

function payrollNetQuantity(record) {
  const qty = Math.abs(toNumber(record.quantity));
  return record.isRefund ? -qty : qty;
}

async function parsePayrollFile(file) {
  const workbook = await readXlsx(file);
  const matchedRows = [];
  const unmatchedRows = [];
  const required = ["고객번호", "고객명", "상품명", "접수일", "설치완료일", "수량", "기본수수료", "판매할성화", "추가수수료", "구분"];
  workbook.sheets.forEach((sheet) => {
    const rows = sheet.rows || [];
    let header = null;
    let headerIndex = -1;
    for (let i = 0; i < rows.length; i += 1) {
      const labels = Object.values(rows[i] || {}).map(normalizeImportHeader);
      const score = required.filter((label) => labels.includes(normalizeImportHeader(label))).length;
      if (score >= 8 && labels.includes(normalizeImportHeader("고객번호")) && labels.includes(normalizeImportHeader("상품명"))) {
        header = rows[i]; headerIndex = i; break;
      }
    }
    if (!header) return;
    for (let i = headerIndex + 1; i < rows.length; i += 1) {
      const row = rows[i] || {};
      const customerNo = String(payrollHeaderValue(row, header, ["고객번호"]) || "").trim();
      const product = String(payrollHeaderValue(row, header, ["상품명"]) || "").trim();
      const customerName = String(payrollHeaderValue(row, header, ["고객명"]) || "").trim();
      if (!customerNo && !product && !customerName) continue;
      const category = String(payrollHeaderValue(row, header, ["구분"]) || "").trim();
      const refund = category.includes("환수");
      const match = resolvePayrollSeller(customerNo, customerName, product, state.payrollManager || "");
      (match.matched ? matchedRows : unmatchedRows).push(normalizePayrollRecord({
        seller: match.seller,
        customerNo,
        customerName,
        product,
        baseFee: payrollNumber(payrollHeaderValue(row, header, ["기본수수료"])),
        salesActivation: payrollNumber(payrollHeaderValue(row, header, ["판매할성화", "판매활성화"])),
        additionalFee: payrollNumber(payrollHeaderValue(row, header, ["추가수수료"])),
        category,
        quantity: payrollNumber(payrollHeaderValue(row, header, ["수량"])),
        receivedDate: excelDate(payrollHeaderValue(row, header, ["접수일"])),
        installCompleteDate: excelDate(payrollHeaderValue(row, header, ["설치완료일"])),
        previousCustomerNo: String(payrollHeaderValue(row, header, ["재렌탈이전번호"]) || "").trim(),
        isRefund: refund,
        matched: match.matched,
        matchedManager: match.manager,
        matchedSeller: match.matchedSeller,
        numberMismatch: match.numberMismatch,
        matchedCustomerNo: match.matchedCustomerNo
      }));
    }
  });
  return {
    matchedRows,
    unmatchedRows,
    totalRows: matchedRows.length + unmatchedRows.length
  };
}

function payrollFeeTotal(record) {
  return toNumber(record.baseFee) + toNumber(record.salesActivation) + toNumber(record.additionalFee);
}

function payrollSellerGroups(rows, mainManager = "") {
  const groups = new Map();
  rows.forEach((row) => {
    const seller = String(row.seller || mainManager || "미지정").trim() || String(mainManager || "미지정");
    if (!groups.has(seller)) groups.set(seller, []);
    groups.get(seller).push(row);
  });
  return [...groups.entries()].sort((a, b) => {
    const main = String(mainManager || "").trim();
    if (main && a[0] === main && b[0] !== main) return -1;
    if (main && b[0] === main && a[0] !== main) return 1;
    return a[0].localeCompare(b[0], "ko");
  });
}

function payrollGroupTotals(rows) {
  return rows.reduce((acc, row) => {
    acc.baseFee += toNumber(row.baseFee);
    acc.salesActivation += toNumber(row.salesActivation);
    acc.additionalFee += toNumber(row.additionalFee);
    acc.quantity += payrollNetQuantity(row);
    acc.fee += payrollFeeTotal(row);
    return acc;
  }, { baseFee: 0, salesActivation: 0, additionalFee: 0, quantity: 0, fee: 0 });
}

function payrollFeeDisplay(value) {
  return toNumber(value) === 0 ? "" : formatWon(value);
}

function payrollDateStack(receivedDate, installDate) {
  return `<div class="payroll-date-stack"><span>${escapeHtml(receivedDate || "-")}</span><span>${escapeHtml(installDate || "-")}</span></div>`;
}

function renderPayroll() {
  const payrollMonth = String(state.payrollMonth || $("#payrollMonthInput")?.value || currentDashboardMonth()).trim();
  const allRows = (Array.isArray(state.payrollRecords) ? state.payrollRecords : []).filter((row) => {
    if (teamOperationMode(payrollMonth) === "1") return true;
    const seller = String(row?.seller || "").trim();
    const manager = managerByName(seller);
    return Boolean(manager && managerTeamForMonth(manager, payrollMonth) === currentUserTeamName(payrollMonth));
  });
  const body = $("#payrollTableBody");
  const rowCount = $("#payrollRowCount");
  const summary = $("#payrollMatchSummary");
  const exportBtn = $("#payrollExportBtn");
  const saveBtn = $("#payrollSaveBtn");
  const monthInput = $("#payrollMonthInput");
  const filter = $("#payrollSellerFilter");
  const managerInput = $("#payrollManagerInput");
  const selectedSeller = filter?.value || "ALL";
  const managers = teamManagerNames(payrollMonth);

  if (managerInput) {
    const current = state.payrollManager || managerInput.value || state.appMeta?.masterName || managers[0] || "";
    setOptions(managerInput, managers.map((name) => ({ value: name, label: name })), current);
    state.payrollManager = managerInput.value || current;
  }
  if (monthInput) monthInput.value = state.payrollMonth || monthInput.value || "";

  if (filter) {
    const sellers = [...new Set(allRows.map((row) => String(row.seller || state.payrollManager || "미지정").trim() || state.payrollManager || "미지정"))];
    const previous = filter.value || "ALL";
    const ordered = sellers.sort((a, b) => {
      const main = String(state.payrollManager || "").trim();
      if (main && a === main && b !== main) return -1;
      if (main && b === main && a !== main) return 1;
      return a.localeCompare(b, "ko");
    });
    filter.innerHTML = `<option value="ALL">전체 판매자</option>${ordered.map((seller) => `<option value="${escapeHtml(seller)}">${escapeHtml(seller)}</option>`).join("")}`;
    filter.value = ordered.includes(previous) ? previous : "ALL";
  }

  const rows = selectedSeller === "ALL" ? allRows : allRows.filter((row) => (row.seller || state.payrollManager || "미지정") === selectedSeller);
  if (rowCount) rowCount.textContent = `${rows.length}건`;
  if (exportBtn) exportBtn.disabled = !rows.length;
  if (saveBtn) saveBtn.disabled = !rows.length || !String(state.payrollManager || "").trim() || !String(state.payrollMonth || "").trim();
  const unmatchedRows = Array.isArray(state.payrollUnmatchedRecords) ? state.payrollUnmatchedRecords : [];
  if (summary) {
    const mismatch = allRows.filter((row) => row.numberMismatch).length;
    summary.textContent = `매칭 ${allRows.length}건 · 고객번호 불일치 ${mismatch}건 · 접수리스트 미매칭 ${unmatchedRows.length}건`;
  }

  const summaryGrid = $("#payrollSummaryGrid");
  if (summaryGrid) {
    const groups = payrollSellerGroups(rows, state.payrollManager || "");
    const cards = groups.map(([seller, groupRows]) => {
      const t = payrollGroupTotals(groupRows);
      return `<div class="payroll-summary-card payroll-manager-summary-card">
        <span class="payroll-summary-manager">${escapeHtml(seller)}</span>
        <div class="payroll-fee-grand-total payroll-fee-grand-total-only"><span>총 수수료 합계</span><strong>${formatWon(t.fee)}</strong></div>
        <small class="payroll-summary-meta">건수 ${formatNumber(t.quantity)} · ${groupRows.length}개 리스트</small>
      </div>`;
    });
    summaryGrid.innerHTML = cards.join("") || `<div class="payroll-summary-card"><span>급여 데이터</span><strong>0원</strong><small>파일을 불러오세요.</small></div>`;
  }

  const unmatchedBody = $("#payrollUnmatchedTableBody");
  const unmatchedCount = $("#payrollUnmatchedCount");
  if (unmatchedCount) unmatchedCount.textContent = `${unmatchedRows.length}건`;
  if (unmatchedBody) {
    if (!unmatchedRows.length) {
      unmatchedBody.innerHTML = `<tr><td colspan="7" class="empty-state">접수리스트에 없는 급여 엑셀 목록은 없습니다.</td></tr>`;
    } else {
      unmatchedBody.innerHTML = unmatchedRows.map((row) => `
        <tr>
          <td>${escapeHtml(row.customerNo)}</td>
          <td title="${escapeHtml(row.customerName)}">${escapeHtml(row.customerName)}</td>
          <td title="${escapeHtml(row.product)}">${escapeHtml(row.product)}</td>
          <td class="money payroll-total-fee-cell"><strong>${formatWon(payrollFeeTotal(row))}</strong></td>
          <td>${escapeHtml(row.category)}</td>
          <td class="qty">${formatNumber(row.quantity)}</td>
          <td>${payrollDateStack(row.receivedDate, row.installCompleteDate)}</td>
        </tr>`).join("");
    }
  }

  if (!body) return;
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="12" class="empty-state">급여 엑셀 파일을 불러오면 변환 결과가 표시됩니다.</td></tr>`;
    return;
  }

  const html = [];
  payrollSellerGroups(rows, state.payrollManager || "").forEach(([seller, groupRows]) => {
    groupRows.sort((a,b) => String(a.customerNo).localeCompare(String(b.customerNo), "ko") || String(a.product).localeCompare(String(b.product), "ko"));
    groupRows.forEach((row) => {
      const customerClass = row.numberMismatch ? "payroll-customer-mismatch" : "";
      html.push(`<tr>
        <td>${escapeHtml(row.seller)}</td>
        <td class="${customerClass}" title="${row.numberMismatch ? `주문리스트 고객번호: ${escapeHtml(row.matchedCustomerNo || "-")}` : ""}">${escapeHtml(row.customerNo)}</td>
        <td title="${escapeHtml(row.customerName)}">${escapeHtml(row.customerName)}</td>
        <td title="${escapeHtml(row.product)}">${escapeHtml(row.product)}</td>
        <td class="money">${payrollFeeDisplay(row.baseFee)}</td>
        <td class="money">${payrollFeeDisplay(row.salesActivation)}</td>
        <td class="money">${payrollFeeDisplay(row.additionalFee)}</td>
        <td class="money payroll-total-fee-cell"><strong>${formatWon(payrollFeeTotal(row))}</strong></td>
        <td>${escapeHtml(row.category)}</td>
        <td class="qty">${formatNumber(row.quantity)}</td>
        <td>${payrollDateStack(row.receivedDate, row.installCompleteDate)}</td>
        <td>${escapeHtml(row.previousCustomerNo)}</td>
      </tr>`);
    });
    const t = payrollGroupTotals(groupRows);
    html.push(`<tr class="payroll-subtotal-row">
      <td colspan="4"><strong>${escapeHtml(seller)} 합계</strong></td>
      <td class="money"><strong>${payrollFeeDisplay(t.baseFee)}</strong></td>
      <td class="money"><strong>${payrollFeeDisplay(t.salesActivation)}</strong></td>
      <td class="money"><strong>${payrollFeeDisplay(t.additionalFee)}</strong></td>
      <td class="money payroll-total-fee-cell"><strong>${formatWon(t.fee)}</strong></td>
      <td><strong>합계</strong></td>
      <td class="qty"><strong>${formatNumber(t.quantity)}</strong></td>
      <td></td><td></td>
    </tr>`);
  });
  body.innerHTML = html.join("");
}

function renderPayrollArchives() {
  const container = $("#payrollArchivesList");
  if (!container) return;
  const archives = Array.isArray(state.payrollArchives) ? [...state.payrollArchives] : [];
  archives.sort((a, b) => String(b.month || "").localeCompare(String(a.month || "")) || String(a.manager || "").localeCompare(String(b.manager || ""), "ko"));
  if (!archives.length) {
    container.innerHTML = `<div class="payroll-archive-empty">저장된 급여내역이 없습니다. 최종 리스트를 확인한 후 저장하세요.</div>`;
    return;
  }
  container.innerHTML = archives.map((archive) => {
    const total = payrollGroupTotals(archive.records || []);
    const saved = archive.savedAt ? new Date(archive.savedAt).toLocaleString("ko-KR", { hour12:false }) : "";
    return `<div class="payroll-archive-row">
      <div class="payroll-archive-main">
        <strong>${escapeHtml(payrollArchiveLabel(archive))}</strong>
        <span>매칭 ${formatNumber((archive.records || []).length)}건 · 미매칭 ${formatNumber((archive.unmatchedRecords || []).length)}건 · ${formatWon(total.fee)} · 최종건수 ${formatNumber(total.quantity)}</span>
        ${saved ? `<small>저장일 ${escapeHtml(saved)}</small>` : ""}
      </div>
      <div class="payroll-archive-actions">
        <button type="button" class="ghost-button payroll-archive-load" data-payroll-archive-id="${escapeHtml(archive.id)}">불러오기</button>
        <button type="button" class="ghost-button payroll-archive-delete" data-payroll-archive-id="${escapeHtml(archive.id)}">삭제</button>
      </div>
    </div>`;
  }).join("");
}

function savePayrollArchive() {
  const manager = String(state.payrollManager || $("#payrollManagerInput")?.value || "").trim();
  const month = String(state.payrollMonth || $("#payrollMonthInput")?.value || "").trim();
  const records = Array.isArray(state.payrollRecords) ? state.payrollRecords : [];
  if (!manager) { showToast("먼저 주매니저를 선택해 주세요."); return; }
  if (!month) { showToast("급여월을 선택해 주세요."); $("#payrollMonthInput")?.focus(); return; }
  if (!records.length) { showToast("먼저 급여 엑셀을 불러와 최종 리스트를 확인해 주세요."); return; }
  const archives = Array.isArray(state.payrollArchives) ? state.payrollArchives : [];
  const existing = archives.find((item) => item.month === month && item.manager === manager);
  if (existing && !window.confirm(`${month.replace("-", "년 ")}월 ${manager} 급여가 이미 저장되어 있습니다.\n기존 저장본을 새 결과로 덮어쓸까요?`)) return;
  const snapshot = {
    id: existing?.id || uid("payroll"),
    month,
    manager,
    savedAt: new Date().toISOString(),
    records: structuredClone(records),
    unmatchedRecords: structuredClone(Array.isArray(state.payrollUnmatchedRecords) ? state.payrollUnmatchedRecords : [])
  };
  state.payrollArchives = existing ? archives.map((item) => item.id === existing.id ? snapshot : item) : [...archives, snapshot];
  state.payrollManager = manager; state.payrollMonth = month;
  persistState({ immediateServer: true });
  renderPayroll(); renderPayrollArchives();
  showToast(`${month.replace("-", "년 ")}월 ${manager} 급여를 저장했습니다.`);
}

function loadPayrollArchive(id) {
  const archive = (state.payrollArchives || []).find((item) => item.id === id);
  if (!archive) return;
  state.payrollManager = archive.manager; state.payrollMonth = archive.month; state.payrollRecords = structuredClone(archive.records || []); state.payrollUnmatchedRecords = structuredClone(archive.unmatchedRecords || []);
  const filter = $("#payrollSellerFilter"); if (filter) filter.value = "ALL";
  persistState({ immediateServer: true }); renderPayroll(); renderPayrollArchives();
  showToast(`${payrollArchiveLabel(archive)}를 불러왔습니다.`);
}

function deletePayrollArchive(id) {
  const archive = (state.payrollArchives || []).find((item) => item.id === id);
  if (!archive) return;
  if (!window.confirm(`${payrollArchiveLabel(archive)} 저장본을 삭제할까요?`)) return;
  state.payrollArchives = (state.payrollArchives || []).filter((item) => item.id !== id);
  persistState({ immediateServer: true }); renderPayrollArchives();
  showToast("저장된 급여내역을 삭제했습니다.");
}

async function importPayrollFile(file) {
  if (!file) return;
  const manager = state.payrollManager || $("#payrollManagerInput")?.value || "";
  if (!manager) {
    showToast("먼저 주매니저를 선택해 주세요.");
    return;
  }
  state.payrollManager = manager;
  state.payrollMonth = String($("#payrollMonthInput")?.value || state.payrollMonth || "").trim();
  try {
    const parsed = await parsePayrollFile(file);
    const targetMonth = state.payrollMonth || currentDashboardMonth();
    state.payrollRecords = parsed.matchedRows.filter((row) => {
      if (teamOperationMode(targetMonth) === "1") return true;
      const seller = String(row?.seller || "").trim();
      const manager = managerByName(seller);
      return Boolean(manager && managerTeamForMonth(manager, targetMonth) === currentUserTeamName(targetMonth));
    });
    state.payrollUnmatchedRecords = parsed.unmatchedRows;
    persistState({ immediateServer: true });
    renderPayroll();
    renderPayrollArchives();
    showToast(`급여 엑셀 ${parsed.totalRows}건 중 매칭 ${parsed.matchedRows.length}건, 미매칭 ${parsed.unmatchedRows.length}건을 반영했습니다.`);
  } catch (error) {
    console.error(error);
    showToast(`급여 엑셀을 읽지 못했습니다: ${error.message || error}`);
  }
}

function createPayrollXlsxBlob(rows) {
  const header = ["판매자", "고객번호", "고객명", "상품명", "기본수수료", "판매활성화", "추가수수료", "합계금액", "구분", "수량", "접수일\n설치완료일", "재렌탈이전번호"];
  const data = [header];
  payrollSellerGroups(rows, state.payrollManager || "").forEach(([seller, groupRows]) => {
    groupRows.sort((a,b) => String(a.customerNo).localeCompare(String(b.customerNo), "ko") || String(a.product).localeCompare(String(b.product), "ko"));
    groupRows.forEach((row) => data.push([
      row.seller, row.customerNo, row.customerName, row.product,
      toNumber(row.baseFee) || "", toNumber(row.salesActivation) || "", toNumber(row.additionalFee) || "", payrollFeeTotal(row) || "",
      row.category, row.quantity, `${row.receivedDate || ""}\n${row.installCompleteDate || ""}`, row.previousCustomerNo
    ]));
    const t = payrollGroupTotals(groupRows);
    data.push([`${seller} 합계`, "", "", "", t.baseFee || "", t.salesActivation || "", t.additionalFee || "", t.fee || "", "합계", t.quantity, "", ""]);
  });

  const files = {
    "[Content_Types].xml": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/></Types>`,
    "_rels/.rels": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`,
    "xl/workbook.xml": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="급여계산" sheetId="1" r:id="rId1"/></sheets></workbook>`,
    "xl/_rels/workbook.xml.rels": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>`,
    "xl/styles.xml": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="3"><font><sz val="10"/><name val="맑은 고딕"/></font><font><b/><sz val="10"/><name val="맑은 고딕"/></font><font><b/><sz val="11"/><name val="맑은 고딕"/></font></fonts><fills count="3"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="solid"><fgColor rgb="E8F1ED"/><bgColor indexed="64"/></patternFill></fill><fill><patternFill patternType="solid"><fgColor rgb="F2F4F7"/><bgColor indexed="64"/></patternFill></fill></fills><borders count="2"><border><left/><right/><top/><bottom/></border><border><left style="thin"/><right style="thin"/><top style="thin"/><bottom style="thin"/></border></borders><cellXfs count="5"><xf numFmtId="0" fontId="0" fillId="0" borderId="1" applyAlignment="1"><alignment vertical="center"/></xf><xf numFmtId="0" fontId="1" fillId="1" borderId="1" applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf><xf numFmtId="0" fontId="1" fillId="2" borderId="1" applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf><xf numFmtId="4" fontId="0" fillId="0" borderId="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf><xf numFmtId="4" fontId="1" fillId="2" borderId="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf></cellXfs></styleSheet>`,
    "xl/worksheets/sheet1.xml": payrollSheetXml(data)
  };
  return new Blob([createZipStore(files)], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });
}

function payrollSheetXml(rows) {
  const widths = [16, 20, 14, 42, 14, 14, 14, 14, 12, 9, 20, 22];
  const sheetRows = rows.map((row, rowIndex) => {
    const rowNumber = rowIndex + 1;
    const isHeader = rowIndex === 0;
    const isSubtotal = rowIndex > 0 && String(row[0] || "").endsWith(" 합계");
    const cells = row.map((value, colIndex) => {
      const ref = `${columnName(colIndex + 1)}${rowNumber}`;
      const text = String(value ?? "");
      const moneyColumn = colIndex >= 4 && colIndex <= 7;
      const style = isHeader ? 1 : (isSubtotal ? (moneyColumn ? 4 : 2) : (moneyColumn ? 3 : 0));
      return `<c r="${ref}" s="${style}" t="inlineStr"><is><t xml:space="preserve">${escapeXmlText(text).replace(/\n/g, "&#10;")}</t></is></c>`;
    }).join("");
    return `<row r="${rowNumber}" ht="${isHeader ? 30 : 22}" customHeight="1">${cells}</row>`;
  }).join("");
  const cols = widths.map((w, i) => `<col min="${i+1}" max="${i+1}" width="${w}" customWidth="1"/>`).join("");
  const lastRow = Math.max(1, rows.length);
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><cols>${cols}</cols><sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews><sheetData>${sheetRows}</sheetData><autoFilter ref="A1:L${lastRow}"/></worksheet>`;
}

function exportPayrollExcel() {
  const allRows = Array.isArray(state.payrollRecords) ? state.payrollRecords : [];
  const selectedSeller = $("#payrollSellerFilter")?.value || "ALL";
  const rows = selectedSeller === "ALL" ? allRows : allRows.filter((row) => (row.seller || state.payrollManager || "미지정") === selectedSeller);
  if (!rows.length) { showToast("먼저 급여 엑셀 파일을 불러와 주세요."); return; }
  const blob = createPayrollXlsxBlob(rows);
  downloadBlob(`급여계산-${todayIso()}.xlsx`, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", blob);
  showToast("판매자별 합계가 포함된 Excel 파일을 다운로드했습니다.");
}

function clearPayrollData() {
  if (!window.confirm("현재 급여계산 데이터를 초기화할까요?")) return;
  state.payrollRecords = [];
  state.payrollUnmatchedRecords = [];
  persistState({ immediateServer: true });
  renderPayroll();
  showToast("급여계산 데이터를 초기화했습니다.");
}

function renderView(view = currentView) {
  switch (view) {
    case "dashboard":
      renderDashboard();
      break;
    case "analytics":
      renderAnalytics();
      break;
    case "evaluation":
      renderManagementEvaluation();
      break;
    case "renewalguide":
      renderRenewalGuide();
      break;
    case "records":
      renderRecords();
      renderMembershipRecords();
      break;
    case "promotions":
      renderPromotions();
      break;
    case "checklist":
      renderChecklist();
      break;
    case "contactnote":
      renderContactNotes();
      break;
    case "contactrequest":
      renderContactRequests();
      break;
    case "payroll":
      renderPayroll();
      renderPayrollArchives();
      break;
    case "settings":
      renderSettings();
      break;
    case "db":
      renderDbManagement();
      break;
    case "license":
      renderLicenseManagement();
      break;
    default:
      renderDashboard();
  }
}

function renderNow() {
  if (renderFrameId) {
    window.cancelAnimationFrame(renderFrameId);
    renderFrameId = 0;
  }
  applyOptionalMenuVisibility();
  renderCommonControls();
  renderTopbar();
  renderView(currentView);
  window.requestAnimationFrame(enhanceMobileFullAppUi);
}

function render() {
  if (renderFrameId) return;
  renderFrameId = window.requestAnimationFrame(() => {
    renderFrameId = 0;
    renderNow();
  });
}

function renderTopbar() {
  const meta = state.appMeta || sampleState.appMeta;
  const branchName = String(meta.branchName || "").trim();
  const masterName = String(meta.masterName || "").trim();
  const masterRole = String(meta.masterRole || "담당자").trim() || "마스터";
  const managerLine = masterName ? `${masterName} ${masterRole}` : "사용자 정보 미등록";

  $("#topDateLabel").textContent = formatKoreanLongDate();
  $("#masterLine").textContent = [branchName, managerLine].filter(Boolean).join(" ");

  const sidebarManagerLine = $("#sidebarManagerLine");
  if (sidebarManagerLine) sidebarManagerLine.textContent = managerLine;

  const sidebarAppTitle = $("#sidebarAppTitle");
  if (sidebarAppTitle) sidebarAppTitle.innerHTML = `<span class="sidebar-app-name">영업관리 시스템</span><span class="sidebar-version-badge">${versionLabelForDisplay(APP_VERSION)}</span>`;
  applyProgramVersionToStaticLabels();

  const titles = {
    dashboard: "영업현황",
    analytics: "영업분석 및 통계",
    evaluation: "경영평가",
    renewalguide: "월별도표",
    payroll: "급여계산",
    records: "접수리스트",
    checklist: "업무체크리스트",
    contactnote: "만기컨텍리스트",
    contactrequest: "컨텍노트",
    promotions: "프로모션",
    settings: "사용자설정",
    db: "DB관리",
    license: "라이선스 관리"
  };
  $("#viewTitle").textContent = titles[currentView] || "영업현황";
}

function availableRecordManagerNamesForFilter() {
  const names = new Set();
  const records = recordsByRecordPeriod();
  const selectedGoalMonth = normalizeManagerMonth($("#recordMonthFilter")?.value) || currentDashboardMonth();
  records.forEach((record) => {
    if (!record || isMembershipRecord(record)) return;
    if (!recordBelongsToCurrentUserTeam(record, selectedGoalMonth)) return;
    const name = String(record.managerNameAtRecord || record.manager || "").trim();
    if (name) names.add(name);
  });
  return sortManagerNamesByDisplayOrder([...names]);
}

function renderCommonControls() {
  const managers = allManagerNames();
  const salesManagers = teamManagerNames(currentDashboardMonth());
  const managerFilter = $("#managerFilter");
  if (managerFilter) setOptions(managerFilter, [{ value: "", label: "전체" }, ...salesManagers.map((name) => ({ value: name, label: name }))], managerFilter.value);

  const managerInput = $("#managerInput");
  if (managerInput) {
    const selectedManager = managerInput.value;
    refreshRecordManagerOptions(recordEntryMonth(), selectedManager);
  }

  setOptions($("#categoryInput"), categories, $("#categoryInput").value);
  const activityTypeInput = $("#activityTypeInput");
  if (activityTypeInput) setOptions(activityTypeInput, activityTypes.map((value) => ({ value, label: value || "-" })), activityTypeInput.value);
  setOptions($("#statusInput"), statuses, $("#statusInput").value);
  updateSellerInputOptions($("#sellerInput")?.value || "");

  const recordStatusFilter = $("#recordStatusFilter");
  const recordManagerFilter = $("#recordManagerFilter");
  const recordCategoryFilter = $("#recordCategoryFilter");
  const recordSellerFilter = $("#recordSellerFilter");
  if (recordStatusFilter) setOptions(recordStatusFilter, [{ value: "", label: "상태" }, ...statuses.map((status) => ({ value: status, label: status }))], recordStatusFilter.value);
  const recordManagerNames = availableRecordManagerNamesForFilter();
  if (recordManagerFilter) setOptions(recordManagerFilter, [{ value: "", label: "매니저" }, ...recordManagerNames.map((name) => ({ value: name, label: name }))], recordManagerFilter.value);
  if (recordCategoryFilter) setOptions(recordCategoryFilter, [{ value: "", label: "판매종류" }, ...mainCategories.map((category) => ({ value: category, label: category }))], recordCategoryFilter.value);
  if (recordSellerFilter) setOptions(recordSellerFilter, [{ value: "", label: "실판매자" }, ...Array.from(new Set([...sellerRoles.filter(Boolean), ...managers])).map((role) => ({ value: role, label: role }))], recordSellerFilter.value);

  const membershipStatusFilter = $("#membershipStatusFilter");
  const membershipManagerFilter = $("#membershipManagerFilter");
  const membershipContactFilter = $("#membershipContactFilter");
  if (membershipStatusFilter) setOptions(membershipStatusFilter, [{ value: "", label: "전체 상태" }, ...statuses.map((status) => ({ value: status, label: status }))], membershipStatusFilter.value);
  if (membershipManagerFilter) setOptions(membershipManagerFilter, [{ value: "", label: "전체 매니저" }, ...managers.map((name) => ({ value: name, label: name }))], membershipManagerFilter.value);
  if (membershipContactFilter) setOptions(membershipContactFilter, [{ value: "", label: "전체 컨텍자" }, ...sellerRoles.filter(Boolean).map((role) => ({ value: role, label: role }))], membershipContactFilter.value);
}

function setProgress(selector, rate) {
  const node = $(selector);
  if (!node) return;
  node.style.width = `${Math.max(0, Math.min(rate, 120))}%`;
  const track = node.parentElement;
  if (track) track.dataset.rate = `${rate}%`;
}


function actualPerformanceCreditManagerName(record, managerNames = null) {
  const names = managerNames instanceof Set
    ? managerNames
    : new Set(teamManagerNames(currentDashboardMonth()));
  const seller = compactValue(record?.seller, "");
  const manager = compactValue(record?.manager, "");

  // 1) 지국장은 실제 실적현황에서 별도 관리자 행으로 집계한다.
  if (seller === "지국장") return "지국장";

  // 2) 팀장은 사용자설정의 등록 사용자(마스터)에게 귀속한다.
  //    예: 매니저=김강령 / 실판매자=팀장 / 사용자=사용자 -> 사용자 실제 실적.
  if (seller === "팀장") {
    const masterName = compactValue((state.appMeta || sampleState.appMeta || {}).masterName, "");
    if (masterName && names.has(masterName)) return masterName;
  }

  // 3) 실판매자에 등록 매니저 이름이 직접 들어 있으면 그 매니저에게 귀속.
  if (seller && names.has(seller)) return seller;

  // 4) 실판매자 미입력 또는 개인 매니저로 특정할 수 없는 값은 주매니저에게 귀속.
  return manager;
}

function managerPerformanceDisplayManagers(salesManagers, actualMode = false) {
  const source = Array.isArray(salesManagers) ? salesManagers.slice() : [];
  const masterName = compactValue((state.appMeta || sampleState.appMeta || {}).masterName, "");
  const seen = new Set();
  const unique = source.filter((manager) => {
    const name = compactValue(manager?.name, "");
    if (!name || seen.has(name)) return false;
    seen.add(name);
    return true;
  });

  // 일반 매니저가 먼저, 등록 사용자(마스터)는 항상 마지막 관리자 위치.
  const normalManagers = unique.filter((manager) => manager.name !== masterName && manager.name !== "지국장");
  const masterManager = unique.find((manager) => manager.name === masterName);

  const ordered = normalManagers.slice();
  if (masterManager) ordered.push(masterManager);

  // 실제 실적현황에서는 실판매자 "지국장"을 맨 마지막 관리자 행으로 별도 표시.
  if (actualMode) {
    ordered.push({
      id: "__actual_branch_manager__",
      name: "지국장",
      team: "관리자",
      goal: 0,
      virtual: true
    });
  }
  return ordered;
}

function actualManagerSalesMetrics(records, managerName = "") {
  // 실제 실적현황에서도 컨스는 별도 관리 건수로만 집계합니다.
  // 재약정/환수는 해당 매니저의 기존 수기값을 그대로 반영합니다.
  const base = actuals(records);
  const manual = manualStatFor(managerName);
  const newCount = toNumber(base.newCount);
  const packageCount = toNumber(base.packageCount);
  const rentalCount = toNumber(base.rentalActual);
  const cashCount = toNumber(base.cashActual);
  const renewal = toNumber(manual.renewal);
  const refund = toNumber(manual.refund);
  const consCount = toNumber(base.orderConsActual);
  const supportCount = toNumber(base.supportActual);
  const business = newCount + packageCount + rentalCount + cashCount + supportCount;
  const final = business + renewal - refund;
  return {
    newCount, packageCount, rentalCount, cashCount, consCount, supportCount,
    business,
    renewal,
    refund,
    final
  };
}


function renderManagerPerformanceMobileCards(rowMetrics, actualMode = false) {
  const totalMetrics = rowMetrics.reduce((acc, row) => {
    const m = row.exactMetrics;
    acc.newCount += toNumber(m.newCount);
    acc.packageCount += toNumber(m.packageCount);
    acc.rentalCount += toNumber(m.rentalCount);
    acc.consCount += toNumber(m.consCount);
    acc.cashCount += toNumber(m.cashCount);
    acc.supportCount += toNumber(m.supportCount);
    acc.business += toNumber(m.business);
    acc.renewal += toNumber(m.renewal);
    acc.refund += toNumber(m.refund);
    acc.final += toNumber(m.final);
    acc.goal += toNumber(row.managerGoal);
    return acc;
  }, { newCount: 0, packageCount: 0, rentalCount: 0, cashCount: 0, consCount: 0, supportCount: 0, business: 0, renewal: 0, refund: 0, final: 0, goal: 0 });

  const metric = (label, value, cls = "") => `<div class="manager-zone-metric ${cls}"><span>${label}</span><strong>${formatNumber(toNumber(value))}</strong></div>`;
  const diffInfo = (actual, goal, isVirtual = false) => {
    if (isVirtual || goal <= 0) return { text: "목표 없음", cls: "neutral" };
    const delta = toNumber(actual) - toNumber(goal);
    if (delta > 0) return { text: `${formatNumber(delta)}건 초과`, cls: "good" };
    if (delta < 0) return { text: `${formatNumber(Math.abs(delta))}건 부족`, cls: "short" };
    return { text: "목표 달성", cls: "good" };
  };

  const renderCard = (title, metrics, managerGoal, rate, options = {}) => {
    const isTotal = Boolean(options.total);
    const isVirtual = Boolean(options.isVirtualBranchManager);
    const managerName = options.managerName || title;
    const consDue = Math.max(0, toNumber(options.consDue));
    const consActual = Math.max(0, toNumber(metrics.consCount));
    const consDisplay = consActual > 0 ? formatNumber(consActual) : (consDue > 0 ? `${formatNumber(consDue)} 예정` : "");
    const validRate = (!isVirtual && managerGoal > 0) ? Math.max(0, Math.round(toNumber(rate))) : null;
    const diff = diffInfo(metrics.final, managerGoal, isVirtual);
    const shareButton = (!isTotal && !actualMode)
      ? `<button class="manager-share-icon mobile" type="button" data-manager-share="${escapeHtml(managerName)}" title="${escapeHtml(managerName)} 매니저 카톡 이미지 공유" aria-label="${escapeHtml(managerName)} 매니저 이미지공유">↗</button>`
      : "";
    const rateClass = validRate === null ? "neutral" : validRate >= 100 ? "good" : validRate >= 85 ? "watch" : "danger";
    return `
      <article class="manager-zone-card ${isTotal ? "total" : ""}">
        <div class="manager-zone-head">
          <div class="manager-zone-name"><span>${isTotal ? "지국 전체" : "매니저"}</span><strong>${escapeHtml(title)}</strong></div>
          <div class="manager-zone-head-actions">${shareButton}<span class="manager-zone-rate ${rateClass}">${validRate === null ? "-" : `${validRate}%`}</span></div>
        </div>
        <div class="manager-zone-summary">
          ${metric("영업", metrics.business, "business")}
          ${metric("최종", metrics.final, "final")}
          <div class="manager-zone-metric goal"><span>목표</span><strong>${managerGoal > 0 ? formatNumber(managerGoal) : "-"}</strong></div>
        </div>
        <div class="manager-zone-sales">
          ${metric("신규", metrics.newCount)}
          ${metric("패키지", metrics.packageCount)}
          ${metric("재렌탈", metrics.rentalCount)}
          ${metric("일시불", metrics.cashCount)}
        </div>
        <div class="manager-zone-subline">
          ${actualMode ? `<span>지원 <b>${formatNumber(toNumber(metrics.supportCount))}</b></span>` : `<button type="button" class="cons-manage-button" data-cons-manage="${escapeHtml(managerName)}"><span>컨스</span> <b>${consDisplay}</b></button><span>지원 <b>${formatNumber(toNumber(metrics.supportCount))}</b></span>`}
          <span>재약정 <b>${formatNumber(toNumber(metrics.renewal))}</b></span>
          <span>환수 <b class="refund">${formatNumber(toNumber(metrics.refund))}</b></span>
        </div>
        <div class="manager-zone-progress">
          <div class="manager-zone-progress-head"><strong>달성률</strong><span class="${diff.cls}">${diff.text}</span></div>
          <div class="manager-zone-track"><span style="width:${validRate === null ? 0 : Math.max(0, Math.min(validRate, 100))}%"></span></div>
        </div>
        ${actualMode || isTotal ? "" : `<details class="manager-zone-manual"><summary>실적 보완 · 재약정/환수</summary><div class="manager-zone-manual-grid">
          <label><span>재약정</span><input class="manager-inline-input" data-manager="${escapeHtml(managerName)}" data-field="renewal" type="number" min="0" step="0.5" value="${manualStatFor(managerName).renewal || ""}" inputmode="decimal"></label>
          <label><span>환수</span><input class="manager-inline-input refund-input" data-manager="${escapeHtml(managerName)}" data-field="refund" type="number" min="0" step="0.5" value="${manualStatFor(managerName).refund || ""}" inputmode="decimal"></label>
        </div></details>`}
      </article>`;
  };

  const totalRate = totalMetrics.goal > 0 ? (totalMetrics.final / totalMetrics.goal) * 100 : 0;
  const cards = [renderCard("합계", totalMetrics, totalMetrics.goal, totalRate, { total: true })];
  rowMetrics.forEach(({ manager, exactMetrics, consDue, managerGoal, managerRate, isVirtualBranchManager }) => {
    cards.push(renderCard(manager.name, exactMetrics, managerGoal, managerRate, { managerName: manager.name, consDue, isVirtualBranchManager }));
  });
  return cards.join("");
}


function consRegisteredRecordsForManager(managerName, sourceRecords = null) {
  const records = Array.isArray(sourceRecords)
    ? sourceRecords
    : (typeof getCurrentDashboardRecords === "function" ? getCurrentDashboardRecords() : filteredRecords());
  return (records || [])
    .filter((record) => record && record.status !== "취소")
    .filter((record) => String(record.manager || "").trim() === String(managerName || "").trim())
    .filter((record) => recordActivityType(record) === "컨스")
    .sort((a, b) => String(a.receivedDate || "").localeCompare(String(b.receivedDate || "")) || String(a.customerName || "").localeCompare(String(b.customerName || ""), "ko"));
}

function renderConsPaymentRecordList(managerName, sourceRecords = null) {
  const host = $("#consPaymentRecordList");
  const count = $("#consPaymentRecordCount");
  if (!host) return;
  const rows = consRegisteredRecordsForManager(managerName, sourceRecords);
  if (count) count.textContent = `${formatNumber(rows.length)}건`;
  if (!rows.length) {
    host.innerHTML = `<div class="cons-payment-record-empty">등록된 컨스 접수건이 없습니다.</div>`;
    return;
  }
  host.innerHTML = `<div class="cons-payment-record-table-wrap"><table class="cons-payment-record-table">
    <thead><tr><th>접수일</th><th>고객명</th><th>상품명</th><th>판매유형</th><th>상태</th></tr></thead>
    <tbody>${rows.map((record) => `<tr>
      <td>${escapeHtml(record.receivedDate || "-")}</td>
      <td>${escapeHtml(record.customerName || "-")}</td>
      <td title="${escapeHtml(record.product || "")}">${escapeHtml(record.product || "-")}</td>
      <td>${escapeHtml(normalizeCategory(record.category) || "-")}</td>
      <td>${escapeHtml(record.status || "-")}</td>
    </tr>`).join("")}</tbody>
  </table></div>`;
}

function openConsPaymentManager(managerName) {
  const month = manualStatsMonthKey();
  const stat = manualStatFor(managerName, month);
  const records = typeof getCurrentDashboardRecords === "function" ? getCurrentDashboardRecords() : filteredRecords();
  const metrics = exactManagerSalesMetrics((records || []).filter((record) => record.manager === managerName), managerName);
  const modal = $("#consPaymentModal");
  if (!modal) return;
  const actual = Math.max(0, toNumber(metrics.consCount));
  const due = Math.max(0, toNumber(stat.consDue));
  const paid = Math.min(actual, due);
  const pending = Math.max(0, due - paid);
  modal.dataset.manager = managerName;
  modal.dataset.month = month;
  $("#consPaymentManagerName").textContent = managerName;
  $("#consPaymentMonth").textContent = `${month.slice(0,4)}년 ${Number(month.slice(5,7))}월`;
  $("#consPaymentActual").textContent = formatNumber(actual);
  $("#consPaymentTarget").value = due || "";
  $("#consPaymentPaid").textContent = formatNumber(paid);
  $("#consPaymentPending").textContent = formatNumber(pending);
  $("#consPaymentMemo").value = stat.consMemo || "";
  renderConsPaymentRecordList(managerName, records);
  modal.hidden = false;
}
function closeConsPaymentManager() { const modal = $("#consPaymentModal"); if (modal) modal.hidden = true; }
$("#printManagerStats")?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-cons-manager]");
  if (button) { event.preventDefault(); openConsPaymentManager(button.dataset.consManager || ""); return; }
});
// 컨스 지급관리 모달은 index.html에서 app.js보다 뒤에 렌더링될 수 있으므로
// 직접 요소에 이벤트를 즉시 바인딩하지 않고 document 위임으로 처리합니다.
// 이렇게 하면 웹/PC 어느 환경에서도 취소·저장 버튼이 정상 작동하고, 재렌더링에도 이벤트가 유지됩니다.
function refreshConsPaymentCalculatedFields() {
  const modal = $("#consPaymentModal");
  if (!modal || modal.hidden) return;
  const managerName = modal.dataset.manager || "";
  const records = typeof getCurrentDashboardRecords === "function" ? getCurrentDashboardRecords() : filteredRecords();
  const actual = Math.max(0, toNumber(exactManagerSalesMetrics((records || []).filter((record) => record.manager === managerName), managerName).consCount));
  const due = Math.max(0, Number($("#consPaymentTarget")?.value || 0));
  const paid = Math.min(actual, due);
  const pending = Math.max(0, due - paid);
  if ($("#consPaymentActual")) $("#consPaymentActual").textContent = formatNumber(actual);
  if ($("#consPaymentPaid")) $("#consPaymentPaid").textContent = formatNumber(paid);
  if ($("#consPaymentPending")) $("#consPaymentPending").textContent = formatNumber(pending);
}

document.addEventListener("input", (event) => {
  if (event.target?.id === "consPaymentTarget") refreshConsPaymentCalculatedFields();
});

document.addEventListener("click", (event) => {
  const closeButton = event.target.closest?.("#consPaymentCloseBtn, #consPaymentCancelBtn, #consPaymentCancelBtn2");
  if (closeButton) {
    event.preventDefault();
    closeConsPaymentManager();
    return;
  }

  const saveButton = event.target.closest?.("#consPaymentSaveBtn");
  if (!saveButton) return;
  event.preventDefault();

  const modal = $("#consPaymentModal");
  const managerName = modal?.dataset.manager || "";
  const month = modal?.dataset.month || manualStatsMonthKey();
  if (!managerName) {
    showToast("컨스 지급관리 대상을 확인해 주세요.");
    return;
  }

  const stat = manualStatFor(managerName, month);
  const rawTarget = $("#consPaymentTarget")?.value || "0";
  const target = Math.max(0, Number(rawTarget));
  if (!Number.isFinite(target)) {
    showToast("지급예정 건수를 확인해 주세요.");
    return;
  }

  const records = typeof getCurrentDashboardRecords === "function" ? getCurrentDashboardRecords() : filteredRecords();
  const actual = Math.max(0, toNumber(exactManagerSalesMetrics((records || []).filter((record) => record.manager === managerName), managerName).consCount));
  stat.consDue = target;
  // 지급 완료/대기는 컨스 등록건과 지급예정 건수에서 자동 계산합니다.
  stat.consPaid = Math.min(actual, target);
  stat.consMemo = String($("#consPaymentMemo")?.value || "");

  persistState();
  renderDashboard();
  closeConsPaymentManager();
  showToast(`${managerName} ${month}월 컨스 지급관리를 저장했습니다.`);
});

function renderManagerPerformanceTable(records, salesManagers) {
  const actualMode = managerPerformanceMode === "actual";
  const managerNames = new Set((salesManagers || []).map((manager) => manager.name));
  const table = $("#managerStatsTable");
  const head = $("#managerStatsHead");
  const body = $("#managerStatsBody");
  const guide = $("#managerPerformanceGuide");

  $$("#printManagerStats [data-manager-performance-tab]").forEach((button) => {
    const active = button.dataset.managerPerformanceTab === managerPerformanceMode;
    button.classList.toggle("active", active);
    button.setAttribute("aria-selected", active ? "true" : "false");
  });

  if (table) table.classList.toggle("actual-performance-table", actualMode);
  const printButton = $("#managerPerformancePrintBtn");
  if (printButton) {
    printButton.textContent = actualMode ? "실제 실적현황 출력" : "매니저별 실적현황 출력";
    printButton.setAttribute("aria-label", printButton.textContent);
  }
  if (guide) {
    guide.textContent = actualMode
      ? "실판매자 기준의 실제 판매실적입니다. 실판매자=팀장은 등록 사용자(마스터)에게, 실판매자=지국장은 별도 지국장 행에 집계합니다. 실판매자가 없으면 주매니저에게 귀속합니다. 컨스는 별도 관리 건수로 확인하며 재약정·환수는 기존 수기값을 반영합니다."
      : "신규·패키지·재렌탈·일시불·지원은 접수리스트에서 자동 집계됩니다. 컨스는 접수리스트 등록건을 자동 집계해 지급관리에서 확인할 수 있으며, 재약정·환수는 수기로 입력합니다.";
  }

  const assignedHeaders = ["매니저","신규","패키지","재렌탈","일시불","컨스","지원","영업실적","재약정","환수","최종실적","상시목표","상시부족","달성률"];
  const actualHeaders = ["매니저","신규","패키지","재렌탈","일시불","지원","영업실적","재약정","환수","최종실적","상시목표","상시부족","달성률"];
  const headers = actualMode ? actualHeaders : assignedHeaders;
  if (head) head.innerHTML = headers.map((label) => `<th>${label}</th>`).join("");

  const displayManagers = managerPerformanceDisplayManagers(salesManagers, actualMode);
  const rowMetrics = displayManagers.map((manager) => {
    const managerRecords = actualMode
      ? (records || []).filter((record) => actualPerformanceCreditManagerName(record, managerNames) === manager.name)
      : (records || []).filter((record) => record.manager === manager.name);
    const exactMetrics = actualMode
      ? actualManagerSalesMetrics(managerRecords, manager.name)
      : exactManagerSalesMetrics(managerRecords, manager.name);
    const manual = manualStatFor(manager.name);
    const consDue = Math.max(0, toNumber(manual.consDue));
    const consPaid = Math.max(0, toNumber(manual.consPaid));
    const consPending = Math.max(0, consDue - consPaid);
    const isVirtualBranchManager = Boolean(manager.virtual && manager.name === "지국장");
    const managerGoal = isVirtualBranchManager ? 0 : toNumber(managerGoalFor(manager.name));
    const shortage = isVirtualBranchManager ? null : exactMetrics.final - managerGoal;
    const managerRate = isVirtualBranchManager
      ? null
      : (managerGoal > 0 ? Math.round((exactMetrics.final / managerGoal) * 100) : 0);
    return { manager, exactMetrics, manual, consDue, consPaid, consPending, managerGoal, shortage, managerRate, isVirtualBranchManager };
  });

  const rows = rowMetrics.map(({ manager, exactMetrics, manual, consDue, consPaid, consPending, managerGoal, shortage, managerRate, isVirtualBranchManager }) => {
    const nameCell = actualMode
      ? `<td class="manager-name-cell"><strong>${escapeHtml(manager.name)}</strong></td>`
      : `<td class="manager-name-cell">
          <strong>${escapeHtml(manager.name)}</strong>
          <button class="manager-share-icon" type="button" data-manager-share="${escapeHtml(manager.name)}" onclick="window.shareManagerKakaoImage?.(this.dataset.managerShare)" title="${escapeHtml(manager.name)} 매니저 카톡 이미지 공유" aria-label="${escapeHtml(manager.name)} 매니저 이미지공유">↗</button>
        </td>`;

    if (actualMode) {
      return `
        <tr>
          ${nameCell}
          <td class="primary-metric">${blankZeroNumber(exactMetrics.newCount)}</td>
          <td class="primary-metric">${blankZeroNumber(exactMetrics.packageCount)}</td>
          <td class="primary-metric">${blankZeroNumber(exactMetrics.rentalCount)}</td>
          <td class="primary-metric">${blankZeroNumber(exactMetrics.cashCount)}</td>
          <td class="activity-value-cell support-auto-cell">${blankZeroNumber(exactMetrics.supportCount)}</td>
          <td class="business-cell metric-emphasis"><strong>${blankZeroNumber(exactMetrics.business)}</strong></td>
          <td class="manual-light">${blankZeroNumber(exactMetrics.renewal)}</td>
          <td class="manual-light refund-text">${exactMetrics.refund ? `-${formatNumber(exactMetrics.refund)}` : ""}</td>
          <td class="final-cell metric-final"><strong>${blankZeroNumber(exactMetrics.final)}</strong></td>
          <td>${isVirtualBranchManager ? "" : blankZeroNumber(managerGoal)}</td>
          <td class="shortage-text">${isVirtualBranchManager || shortage === 0 ? "" : formatNumber(shortage)}</td>
          <td class="rate-cell">
            ${isVirtualBranchManager ? "" : `
            <div class="mini-rate-wrap">
              <div class="mini-rate-track" data-rate="${managerRate}%">
                <span style="width:${Math.max(0, Math.min(managerRate, 120))}%"></span>
              </div>
              <strong>${managerRate}%</strong>
            </div>`}
          </td>
        </tr>
      `;
    }

    return `
      <tr>
        ${nameCell}
        <td class="primary-metric">${blankZeroNumber(exactMetrics.newCount)}</td>
        <td class="primary-metric">${blankZeroNumber(exactMetrics.packageCount)}</td>
        <td class="primary-metric">${blankZeroNumber(exactMetrics.rentalCount)}</td>
        <td class="primary-metric">${blankZeroNumber(exactMetrics.cashCount)}</td>
        <td class="activity-value-cell cons-auto-cell">${(() => {
          const actual = toNumber(exactMetrics.consCount);
          const due = toNumber(consDue);
          const value = actual > 0 ? actual : due;
          const pending = actual <= 0 && due > 0;
          const title = actual > 0 ? `실제 컨스 ${formatNumber(actual)}건 · 지급관리` : (due > 0 ? `컨스 지급 예정 ${formatNumber(due)}건 · 실제 접수 전` : "컨스 지급관리");
          return `<button type="button" class="cons-table-button ${pending ? "cons-pending-button" : ""}" data-cons-manager="${escapeHtml(manager.name)}" title="${escapeHtml(title)}"><span>${blankZeroNumber(value)}</span>${pending ? `<small>예정</small>` : ""}</button>`;
        })()}</td>
        <td class="activity-value-cell support-auto-cell">${blankZeroNumber(exactMetrics.supportCount)}</td>
        <td class="business-cell metric-emphasis"><strong>${blankZeroNumber(exactMetrics.business)}</strong></td>
        <td class="manual-stat-cell manual-light"><input class="manager-inline-input" data-manager="${escapeHtml(manager.name)}" data-field="renewal" type="number" min="0" step="0.5" value="${manual.renewal ? manual.renewal : ""}" inputmode="decimal" aria-label="재약정 수기입력"></td>
        <td class="manual-stat-cell manual-light refund-text"><input class="manager-inline-input refund-input" data-manager="${escapeHtml(manager.name)}" data-field="refund" type="number" min="0" step="0.5" value="${manual.refund ? manual.refund : ""}" inputmode="decimal" aria-label="환수 수기입력"></td>
        <td class="final-cell metric-final"><strong>${blankZeroNumber(exactMetrics.final)}</strong></td>
        <td>${blankZeroNumber(managerGoal)}</td>
        <td class="shortage-text">${shortage === 0 ? "" : formatNumber(shortage)}</td>
        <td class="rate-cell">
          <div class="mini-rate-wrap">
            <div class="mini-rate-track" data-rate="${managerRate}%">
              <span style="width:${Math.max(0, Math.min(managerRate, 120))}%"></span>
            </div>
            <strong>${managerRate}%</strong>
          </div>
        </td>
      </tr>
    `;
  }).join("");

  const totals = rowMetrics.reduce((acc, row) => {
    const m = row.exactMetrics;
    acc.newCount += toNumber(m.newCount);
    acc.packageCount += toNumber(m.packageCount);
    acc.rentalCount += toNumber(m.rentalCount);
    acc.consCount += toNumber(m.consCount);
    acc.cashCount += toNumber(m.cashCount);
    acc.supportCount += toNumber(m.supportCount);
    acc.business += toNumber(m.business);
    acc.renewal += toNumber(m.renewal);
    acc.refund += toNumber(m.refund);
    acc.final += toNumber(m.final);
    acc.goal += toNumber(row.managerGoal);
    return acc;
  }, { newCount:0, packageCount:0, rentalCount:0, cashCount:0, consCount:0, supportCount:0, business:0, renewal:0, refund:0, final:0, goal:0 });

  const totalShortage = totals.final - totals.goal;
  const totalRate = totals.goal > 0 ? Math.round((totals.final / totals.goal) * 100) : 0;

  let totalRow = "";
  if ((salesManagers || []).length) {
    totalRow = actualMode ? `
      <tr class="manager-total-row">
        <td class="manager-name-cell"><strong>합계</strong></td>
        <td class="primary-metric">${blankZeroNumber(totals.newCount)}</td>
        <td class="primary-metric">${blankZeroNumber(totals.packageCount)}</td>
        <td class="primary-metric">${blankZeroNumber(totals.rentalCount)}</td>
        <td class="primary-metric">${blankZeroNumber(totals.cashCount)}</td>
        <td class="activity-value-cell support-auto-cell">${blankZeroNumber(totals.supportCount)}</td>
        <td class="business-cell metric-emphasis"><strong>${blankZeroNumber(totals.business)}</strong></td>
        <td>${blankZeroNumber(totals.renewal)}</td>
        <td class="refund-text">${totals.refund ? `-${formatNumber(totals.refund)}` : ""}</td>
        <td class="final-cell metric-final"><strong>${blankZeroNumber(totals.final)}</strong></td>
        <td>${blankZeroNumber(totals.goal)}</td>
        <td class="shortage-text">${totalShortage === 0 ? "" : formatNumber(totalShortage)}</td>
        <td class="rate-cell">
          <div class="mini-rate-wrap">
            <div class="mini-rate-track" data-rate="${totalRate}%">
              <span style="width:${Math.max(0, Math.min(totalRate, 120))}%"></span>
            </div>
            <strong>${totalRate}%</strong>
          </div>
        </td>
      </tr>
    ` : `
      <tr class="manager-total-row">
        <td class="manager-name-cell"><strong>합계</strong></td>
        <td class="primary-metric">${blankZeroNumber(totals.newCount)}</td>
        <td class="primary-metric">${blankZeroNumber(totals.packageCount)}</td>
        <td class="primary-metric">${blankZeroNumber(totals.rentalCount)}</td>
        <td class="primary-metric">${blankZeroNumber(totals.cashCount)}</td>
        <td class="activity-value-cell cons-auto-cell">${blankZeroNumber(totals.consCount)}</td>
        <td class="activity-value-cell support-auto-cell">${blankZeroNumber(totals.supportCount)}</td>
        <td class="business-cell metric-emphasis"><strong>${blankZeroNumber(totals.business)}</strong></td>
        <td>${blankZeroNumber(totals.renewal)}</td>
        <td class="refund-text">${totals.refund ? `-${formatNumber(totals.refund)}` : ""}</td>
        <td class="final-cell metric-final"><strong>${blankZeroNumber(totals.final)}</strong></td>
        <td>${blankZeroNumber(totals.goal)}</td>
        <td class="shortage-text">${totalShortage === 0 ? "" : formatNumber(totalShortage)}</td>
        <td class="rate-cell">
          <div class="mini-rate-wrap">
            <div class="mini-rate-track" data-rate="${totalRate}%">
              <span style="width:${Math.max(0, Math.min(totalRate, 120))}%"></span>
            </div>
            <strong>${totalRate}%</strong>
          </div>
        </td>
      </tr>
    `;
  }

  if (body) {
    body.innerHTML = rows + totalRow || `<tr><td colspan="${headers.length}" class="empty">등록된 매니저가 없습니다.</td></tr>`;
  }

  const mobileList = $("#managerPerformanceMobileList");
  if (mobileList) {
    mobileList.innerHTML = displayManagers.length
      ? renderManagerPerformanceMobileCards(rowMetrics, actualMode)
      : `<div class="manager-mobile-empty">등록된 매니저가 없습니다.</div>`;
  }
}



function syncDashboardSummaryMode() {
  const legacy = $("#dashboardView .dashboard-legacy-summary");
  const mobileOverview = $("#dashboardMobileOverview");
  if (!legacy || !mobileOverview) return;

  const mobile = mobileOnlyViewport();
  if (mobile) {
    legacy.style.setProperty("display", "none", "important");
    legacy.setAttribute("aria-hidden", "true");
    mobileOverview.style.setProperty("display", "grid", "important");
    mobileOverview.removeAttribute("aria-hidden");
  } else {
    legacy.style.removeProperty("display");
    legacy.removeAttribute("aria-hidden");
    mobileOverview.style.setProperty("display", "none", "important");
    mobileOverview.setAttribute("aria-hidden", "true");
  }
}

function renderDashboardMobileOverview(records, goals, totals) {
  const host = $("#dashboardMobileOverview");
  if (!host) return;

  const rateFor = (actual, goal) => goal > 0 ? Math.round((toNumber(actual) / toNumber(goal)) * 100) : 0;
  const shortageFor = (actual, goal) => Math.max(toNumber(goal) - toNumber(actual), 0);
  const progressRow = (label, actual, goal, tone = "") => {
    const rate = rateFor(actual, goal);
    const shortage = shortageFor(actual, goal);
    const delta = toNumber(actual) - toNumber(goal);
    const status = goal <= 0 ? "목표 없음" : shortage > 0 ? `${formatNumber(shortage)}건 부족` : delta > 0 ? `${formatNumber(delta)}건 초과` : "목표 달성";
    return `
      <div class="mobile-goal-row ${tone}">
        <div class="mobile-goal-row-top">
          <strong>${escapeHtml(label)}</strong>
          <span><b>${formatNumber(actual)}</b> / ${formatNumber(goal)}</span>
          <em>${goal > 0 ? `${rate}%` : "-"}</em>
        </div>
        <div class="mobile-goal-row-bottom">
          <div class="mobile-goal-track"><span style="width:${Math.max(0, Math.min(rate, 100))}%"></span></div>
          <small class="${shortage > 0 ? "short" : "done"}">${escapeHtml(status)}</small>
        </div>
      </div>`;
  };

  const goalsHtml = [
    progressRow("종합달성", totals.overallActual, goals.overallGoal, "overall"),
    progressRow("신규", totals.newActual, goals.newGoal),
    progressRow("패키지", totals.packageCount, goals.packageGoal),
    progressRow("재렌탈", totals.rentalActual, goals.rentalGoal),
    progressRow("재약정", totals.renewalActual, goals.renewalGoal)
  ].join("");

  const activeCards = dashboardCustomCards().filter(card =>
    card.enabled && card.conditions?.some(c => String(c.value || "").trim())
  );
  const focusCards = activeCards.map((card) => {
    let value = 0;
    let sub = "";
    if (String(card.title || "").trim() === "정수기") {
      const water = waterPurifierEvaluationMetrics(currentDashboardMonth());
      value = water.current;
      sub = `${formatNumber(Math.round(water.achievementRate * 10) / 10)}%`;
    } else {
      value = customCardCount(records, card);
    }
    return `<div class="mobile-focus-tile"><span>${escapeHtml(card.title || "조건")}</span><strong>${formatNumber(value)}</strong>${sub ? `<em>${sub}</em>` : ""}</div>`;
  }).join("");

  const detailItems = [
    ["순수 신규", totals.newCount],
    ["일시불", totals.cashActual],
    ["컨스", totals.orderConsActual],
    ["환수", totals.refundActual]
  ].map(([label,value]) => `<div class="mobile-detail-tile ${label === "환수" ? "refund" : ""}"><span>${label}</span><strong>${formatNumber(value)}</strong></div>`).join("");

  host.innerHTML = `
    <section class="mobile-overview-zone mobile-overview-goals">
      <div class="mobile-zone-head"><div><span>SALES SNAPSHOT</span><strong>핵심 목표 진행</strong></div><em>실적 / 목표 / 달성률</em></div>
      <div class="mobile-goal-list">${goalsHtml}</div>
    </section>
    ${focusCards ? `<section class="mobile-overview-zone"><div class="mobile-zone-head"><div><span>FOCUS</span><strong>집중관리</strong></div></div><div class="mobile-focus-grid">${focusCards}</div></section>` : ""}
    <section class="mobile-overview-zone"><div class="mobile-zone-head"><div><span>DETAIL</span><strong>세부 실적</strong></div></div><div class="mobile-detail-grid">${detailItems}</div></section>
  `;
}

function renderDashboard() {
  const records = filteredRecords();
  const goals = calculatedGoals($("#monthFilter").value);
  const totals = applyManualStatsToTotals(actuals(records));
  const newRate = goals.newGoal > 0 ? Math.round((totals.newActual / goals.newGoal) * 100) : 0;
  const packageCardRate = goals.packageGoal > 0 ? Math.round((totals.packageCount / goals.packageGoal) * 100) : 0;
  const rentalRate = goals.rentalGoal > 0 ? Math.round((totals.rentalActual / goals.rentalGoal) * 100) : 0;
  const coreRate = goals.generalGoal > 0 ? Math.round((totals.coreActual / goals.generalGoal) * 100) : 0;
  const renewalRate = goals.renewalGoal > 0 ? Math.round((totals.renewalActual / goals.renewalGoal) * 100) : 0;
  const overallRate = goals.overallGoal > 0 ? Math.round((totals.overallActual / goals.overallGoal) * 100) : 0;
  const coreShortage = Math.max(goals.generalGoal - totals.coreActual, 0);
  const newShortage = Math.max(goals.newGoal - totals.newActual, 0);
  const packageShortage = Math.max(goals.packageGoal - totals.packageCount, 0);
  const rentalShortage = Math.max(goals.rentalGoal - totals.rentalActual, 0);
  const renewalShortage = Math.max(goals.renewalGoal - totals.renewalActual, 0);
  const overallShortage = Math.max(goals.overallGoal - totals.overallActual, 0);
  const shortageRate = goals.generalGoal > 0 ? Math.round((coreShortage / goals.generalGoal) * 100) : 0;

  const targetPeriod = monthPeriod($("#monthFilter").value);
  $("#summaryAccount").textContent = formatNumber(monthSetting($("#monthFilter").value).accountCount);
  $("#summaryPeriod").textContent = `${targetPeriod.start} ~ ${targetPeriod.end}`;
  $("#summaryNewTitle").textContent = `신규 ${formatNumber(goals.newGoal)}`;
  const summaryPackageTitle = $("#summaryPackageTitle");
  if (summaryPackageTitle) summaryPackageTitle.textContent = `패키지 ${formatNumber(goals.packageGoal)}`;
  $("#summaryRentalTitle").textContent = `재렌탈 ${formatNumber(goals.rentalGoal)}`;
  $("#summaryCoreTitle").textContent = `합계 ${formatNumber(goals.generalGoal)}`;
  $("#summaryRenewalTitle").textContent = `재약정 ${formatNumber(goals.renewalGoal)}`;
  $("#summaryOverallTitle").textContent = `종합달성 ${formatNumber(goals.overallGoal)}`;
  $("#summaryNewActual").textContent = formatNumber(totals.newActual);
  renderDashboardCustomCards(records);
  renderDashboardMobileOverview(records, goals, totals);
  syncDashboardSummaryMode();
  $("#summaryNewOnly").textContent = formatNumber(totals.newCount);
  $("#summaryPackageActual").textContent = formatNumber(totals.packageCount);
  const summaryPackageSub = $("#summaryPackageSub");
  if (summaryPackageSub) summaryPackageSub.innerHTML = `부족 <b>${formatNumber(packageShortage)}</b>`;
  setProgress("#summaryPackageBar", packageCardRate);
  $("#summaryCashActual").textContent = formatNumber(totals.cashActual);
  $("#summaryRentalActual").textContent = formatNumber(totals.rentalActual);
  $("#summaryCoreActual").textContent = formatNumber(totals.coreActual);
  const summaryShortageActual = $("#summaryShortageActual");
  if (summaryShortageActual) summaryShortageActual.textContent = formatNumber(coreShortage);
  $("#summaryRefundActual").textContent = totals.refundActual ? `-${formatNumber(totals.refundActual)}` : "0";
  $("#summaryRenewalActual").textContent = formatNumber(totals.renewalActual);
  $("#summaryOrderConsActual").textContent = formatNumber(totals.orderConsActual);
  $("#summaryOverallActual").textContent = formatNumber(totals.overallActual);
  $("#summaryNewSub").innerHTML = `부족 <b>${formatNumber(newShortage)}</b>`;
  $("#summaryRentalSub").innerHTML = `부족 <b>${formatNumber(rentalShortage)}</b>`;
  $("#summaryCoreSub").innerHTML = `부족 <b>${formatNumber(coreShortage)}</b>`;
  $("#summaryRenewalSub").innerHTML = `부족 <b>${formatNumber(renewalShortage)}</b>`;
  $("#summaryOverallSub").innerHTML = `부족 <b>${formatNumber(overallShortage)}</b>`;
  setProgress("#summaryNewBar", newRate);
  setProgress("#summaryRentalBar", rentalRate);
  setProgress("#summaryCoreBar", coreRate);
  setProgress("#summaryRenewalBar", renewalRate);
  setProgress("#summaryOverallBar", overallRate);
  renderOperatingGoalPanel(totals, $("#monthFilter").value);
  renderDashboardCalendar();
  $("#targetPeriodLabel").textContent = `${targetPeriod.start} ~ ${targetPeriod.end}`;
  $("#periodLabel").textContent = `${$("#startDateFilter").value} ~ ${$("#endDateFilter").value}`;

  const salesManagers = teamManagers($("#monthFilter")?.value || monthIso());
  renderManagerPerformanceTable(records, salesManagers);
  renderDashboardManagerConditionSummary(records, salesManagers);
}

function renderDashboardManagerConditionSummary(records, managers) {
  const host = $("#managerConditionSummary");
  if (!host) return;

  const cards = dashboardCustomCards().filter((card) => card.enabled && card.value);
  const promo = hundredPointPromotion(currentDashboardMonth());
  const promoRules = promo ? promoKeywordRules(promo) : [];

  const managerList = Array.isArray(managers) ? managers : [];
  const rows = managerList.map((manager) => {
    const managerRecords = (records || []).filter((record) => record.manager === manager.name);
    const conditionValues = cards.map((card) => dashboardConditionPhysicalCount(managerRecords, card));
    const promoScores = promo ? hundredPointPromotionScoresForManager(manager.name, promo, records) : { values: [], total: 0 };
    return {
      manager: manager.name,
      conditionValues,
      promoValues: promoScores.values || [],
      promoTotal: toNumber(promoScores.total)
    };
  });

  const totalConditions = cards.map((_, index) =>
    rows.reduce((sum, row) => sum + toNumber(row.conditionValues[index]), 0)
  );
  const totalPromo = promoRules.map((_, index) =>
    rows.reduce((sum, row) => sum + toNumber(row.promoValues[index]), 0)
  );
  const totalPromoScore = totalPromo.reduce((sum, value) => sum + toNumber(value), 0);

  if (!cards.length && !promoRules.length) {
    host.innerHTML = `
      <div class="manager-condition-summary-head">
        <div class="dashboard-section-heading"><p class="eyebrow">CONDITION &amp; PROMOTION</p><h3>매니저별 조건·프로모션 현황</h3></div>
      </div>
      <div class="manager-condition-summary-empty">대시보드 선택 조건 또는 100점을 잡아라 프로모션이 등록되어 있지 않습니다.</div>
    `;
    return;
  }

  const conditionHead = cards.length
    ? `<th colspan="${cards.length}" class="condition-group-head">집중관리 제품 <small>건수</small></th>`
    : "";
  const promoHead = promoRules.length
    ? `<th colspan="${promoRules.length + 1}" class="promo-group-head">100점을 잡아라 <small>점수</small></th>`
    : "";

  const useGroupedDesktopHead = Boolean(cards.length && promoRules.length);
  const desktopHead = useGroupedDesktopHead
    ? `<tr class="manager-condition-group-row">
        <th rowspan="2" class="manager-head-cell">매니저</th>
        ${conditionHead}
        ${promoHead}
      </tr>
      <tr class="manager-condition-label-row">
        ${cards.map((card) => `<th class="condition-col">${escapeHtml(card.title || "조건")}</th>`).join("")}
        ${promoRules.map((rule) => `<th class="promo-col">${escapeHtml(rule.title || rule.keyword || "항목")}</th>`).join("")}
        ${promoRules.length ? '<th class="promo-col promo-total-col">총점</th>' : ""}
      </tr>`
    : `<tr class="manager-condition-single-row">
        <th class="manager-head-cell">매니저</th>
        ${cards.map((card) => `<th class="condition-col">${escapeHtml(card.title || "조건")}</th>`).join("")}
        ${promoRules.map((rule) => `<th class="promo-col">${escapeHtml(rule.title || rule.keyword || "항목")}</th>`).join("")}
        ${promoRules.length ? '<th class="promo-col promo-total-col">총점</th>' : ""}
      </tr>`;

  const valuePill = (value, kind = "condition", suffix = "") => {
    const numeric = toNumber(value);
    if (numeric <= 0) return `<span class="manager-condition-zero">—</span>`;
    return `<span class="manager-condition-value-pill ${kind}">${formatNumber(numeric)}${suffix}</span>`;
  };

  const bodyRows = rows.map((row) => `
    <tr>
      <td class="manager-condition-name"><span>${escapeHtml(row.manager)}</span></td>
      ${row.conditionValues.map((value) => `<td class="condition-value">${valuePill(value, "condition")}</td>`).join("")}
      ${row.promoValues.map((value) => `<td class="promo-value">${valuePill(value, "promo", "점")}</td>`).join("")}
      ${promoRules.length ? `<td class="promo-value promo-total-value">${valuePill(row.promoTotal, "total", "점")}</td>` : ""}
    </tr>
  `).join("");



  const mobileRows = [
    {
      manager: "합계",
      conditionValues: totalConditions,
      promoValues: totalPromo,
      promoTotal: totalPromoScore,
      total: true
    },
    ...rows
  ];
  const mobileHtml = mobileRows.map((row) => {
    const conditionTiles = cards.map((card, index) => `<div class="condition-mobile-tile"><span>${escapeHtml(card.title || "조건")}</span><strong>${formatNumber(toNumber(row.conditionValues[index]))}</strong></div>`).join("");
    const promoTiles = promoRules.map((rule, index) => `<div class="condition-mobile-promo-item"><span>${escapeHtml(rule.title || rule.keyword || "항목")}</span><strong>${formatNumber(toNumber(row.promoValues[index]))}점</strong></div>`).join("");
    return `
      <article class="condition-mobile-card ${row.total ? "total" : ""}">
        <div class="condition-mobile-card-head"><strong>${escapeHtml(row.manager)}</strong>${promoRules.length ? `<span>프로모션 <b>${formatNumber(toNumber(row.promoTotal))}점</b></span>` : `<span>프로모션 없음</span>`}</div>
        ${conditionTiles ? `<div class="condition-mobile-grid">${conditionTiles}</div>` : ""}
        ${promoTiles ? `<details class="condition-mobile-details"><summary>프로모션 항목 보기</summary><div class="condition-mobile-promo-grid">${promoTiles}</div></details>` : ""}
      </article>`;
  }).join("");

  host.innerHTML = `
    <div class="manager-condition-summary-head">
      <div>
        <div class="dashboard-section-heading"><p class="eyebrow">CONDITION &amp; PROMOTION</p><h3>매니저별 조건·프로모션 현황</h3></div>
      </div>
      ${promo ? `<span class="manager-condition-promo-period">${escapeHtml(promo.name || "100점을 잡아라")}</span>` : ""}
    </div>
    <div class="manager-condition-mobile-list">${mobileHtml}</div>
    <div class="table-wrap manager-condition-table-wrap">
      <table class="manager-condition-table ${useGroupedDesktopHead ? "has-groups" : "single-group"}">
        <thead>${desktopHead}</thead>
        <tbody>${bodyRows}</tbody>
      </table>
    </div>
  `;
}


const koreanHolidayCache = new Map();
const koreanLunarFormatter = (() => {
  try {
    return new Intl.DateTimeFormat("en-u-ca-chinese", { month: "numeric", day: "numeric", year: "numeric" });
  } catch {
    return null;
  }
})();

function addIsoDays(iso, amount) {
  const date = new Date(`${iso}T12:00:00`);
  date.setDate(date.getDate() + amount);
  return formatLocalDate(date);
}

function koreanLunarDateParts(iso) {
  if (!koreanLunarFormatter) return null;
  try {
    const parts = koreanLunarFormatter.formatToParts(new Date(`${iso}T12:00:00`));
    const monthText = String(parts.find((part) => part.type === "month")?.value || "");
    const dayText = String(parts.find((part) => part.type === "day")?.value || "");
    const relatedYearText = String(parts.find((part) => part.type === "relatedYear")?.value || "");
    const leap = /bis|leap|윤/i.test(monthText);
    const month = Number.parseInt(monthText, 10);
    const day = Number.parseInt(dayText, 10);
    const relatedYear = Number.parseInt(relatedYearText, 10);
    if (!Number.isFinite(month) || !Number.isFinite(day)) return null;
    return { month, day, relatedYear, leap };
  } catch {
    return null;
  }
}

function buildKoreanHolidayMap(targetYear) {
  const year = Number(targetYear);
  if (!Number.isFinite(year)) return new Map();
  if (koreanHolidayCache.has(year)) return koreanHolidayCache.get(year);

  const baseDays = new Map();
  const publicDays = new Map();
  const singleEvents = [];
  const groupEvents = [];

  const addBase = (iso, name, source) => {
    if (!iso || !name) return;
    if (!baseDays.has(iso)) baseDays.set(iso, { names: new Set(), sources: new Set() });
    const entry = baseDays.get(iso);
    entry.names.add(name);
    entry.sources.add(source);
    publicDays.set(iso, entry);
  };

  const addSingle = (iso, name, substituteFromYear = Infinity) => {
    const id = `single:${name}:${iso}`;
    addBase(iso, name, id);
    singleEvents.push({ id, iso, name, substituteFromYear });
  };

  const addGroup = (centerIso, name, labels, substituteFromYear = 2014) => {
    if (!centerIso) return;
    const id = `group:${name}:${centerIso}`;
    const dates = [addIsoDays(centerIso, -1), centerIso, addIsoDays(centerIso, 1)];
    dates.forEach((iso, index) => addBase(iso, labels[index], id));
    groupEvents.push({ id, centerIso, name, dates, substituteFromYear });
  };

  for (let y = year - 1; y <= year + 1; y += 1) {
    addSingle(`${y}-01-01`, "신정");
    addSingle(`${y}-03-01`, "3·1절", 2022);
    if (y >= 2026) addSingle(`${y}-05-01`, "노동절", 2026);
    addSingle(`${y}-05-05`, "어린이날", 2014);
    addSingle(`${y}-06-06`, "현충일");
    addSingle(`${y}-08-15`, "광복절", 2022);
    addSingle(`${y}-10-03`, "개천절", 2022);
    addSingle(`${y}-10-09`, "한글날", 2022);
    addSingle(`${y}-12-25`, "성탄절", 2023);

    const start = new Date(`${y}-01-01T12:00:00`);
    const end = new Date(`${y}-12-31T12:00:00`);
    for (const cursor = new Date(start); cursor <= end; cursor.setDate(cursor.getDate() + 1)) {
      const iso = formatLocalDate(cursor);
      const lunar = koreanLunarDateParts(iso);
      if (!lunar || lunar.leap) continue;
      if (lunar.month === 1 && lunar.day === 1) {
        addGroup(iso, "설날", ["설날 연휴", "설날", "설날 연휴"], 2014);
      } else if (lunar.month === 4 && lunar.day === 8) {
        addSingle(iso, "부처님 오신 날", 2023);
      } else if (lunar.month === 8 && lunar.day === 15) {
        addGroup(iso, "추석", ["추석 연휴", "추석", "추석 연휴"], 2014);
      }
    }
  }

  const weekday = (iso) => new Date(`${iso}T12:00:00`).getDay();
  const hasOtherBaseSource = (iso, ownId) => {
    const entry = baseDays.get(iso);
    return Boolean(entry && [...entry.sources].some((source) => source !== ownId));
  };
  const isBlockedSubstituteDate = (iso) => {
    const day = weekday(iso);
    return day === 0 || day === 6 || publicDays.has(iso);
  };
  const scheduleSubstitute = (afterIso, name, source) => {
    let candidate = addIsoDays(afterIso, 1);
    while (isBlockedSubstituteDate(candidate)) candidate = addIsoDays(candidate, 1);
    const entry = { names: new Set([`${name} 대체공휴일`]), sources: new Set([source]) };
    publicDays.set(candidate, entry);
    return candidate;
  };

  const substituteTasks = [];
  singleEvents.forEach((event) => {
    const eventYear = Number(event.iso.slice(0, 4));
    if (eventYear < event.substituteFromYear) return;
    const day = weekday(event.iso);
    if (day === 0 || day === 6 || hasOtherBaseSource(event.iso, event.id)) {
      substituteTasks.push({ afterIso: event.iso, name: event.name, source: `sub:${event.id}` });
    }
  });

  groupEvents.forEach((event) => {
    const eventYear = Number(event.centerIso.slice(0, 4));
    if (eventYear < event.substituteFromYear) return;
    const needsSubstitute = event.dates.some((iso) => weekday(iso) === 0 || hasOtherBaseSource(iso, event.id));
    if (needsSubstitute) {
      substituteTasks.push({
        afterIso: event.dates.slice().sort().slice(-1)[0],
        name: event.name,
        source: `sub:${event.id}`
      });
    }
  });

  substituteTasks
    .sort((a, b) => a.afterIso.localeCompare(b.afterIso) || a.name.localeCompare(b.name, "ko"))
    .forEach((task) => scheduleSubstitute(task.afterIso, task.name, task.source));

  const result = new Map();
  [...publicDays.entries()].forEach(([iso, entry]) => {
    if (Number(iso.slice(0, 4)) !== year) return;
    result.set(iso, {
      name: [...entry.names].join(" · "),
      names: [...entry.names]
    });
  });
  koreanHolidayCache.set(year, result);
  return result;
}

function koreanHolidayInfo(iso) {
  const year = Number(String(iso || "").slice(0, 4));
  if (!Number.isFinite(year)) return null;
  return buildKoreanHolidayMap(year).get(iso) || null;
}

function renderDashboardCalendar() {
  const calendar = $("#dashboardCalendar");
  if (!calendar) return;
  const start = $("#startDateFilter").value;
  const end = $("#endDateFilter").value;
  $("#calendarRange").textContent = start && end ? `${start.slice(5)} ~ ${end.slice(5)}` : "-";
  const calendarMonthLabel = $("#calendarMonthLabel");
  if (calendarMonthLabel) calendarMonthLabel.textContent = formatMonthLabel($("#monthFilter").value || monthIso());
  const targetPeriod = monthPeriod($("#monthFilter").value);

  const countsByDate = state.records.reduce((map, record) => {
    if (record.status === "취소" || !record.receivedDate) return map;
    if (!inDateRange(record.receivedDate, targetPeriod.start, targetPeriod.end)) return map;
    map[record.receivedDate] = (map[record.receivedDate] || 0) + toNumber(record.count);
    return map;
  }, {});

  const safeStart = targetPeriod.start;
  const safeEnd = targetPeriod.end;
  const first = new Date(`${safeStart}T00:00:00`);
  const last = new Date(`${safeEnd}T00:00:00`);
  const gridStart = new Date(first);
  gridStart.setDate(first.getDate() - first.getDay());
  const gridEnd = new Date(last);
  gridEnd.setDate(last.getDate() + (6 - last.getDay()));

  const selectedDate = start && start === end ? start : "";
  const selectedStart = start && end ? (start < end ? start : end) : "";
  const selectedEnd = start && end ? (start < end ? end : start) : "";
  const weekdays = ["일", "월", "화", "수", "목", "금", "토"];
  const parts = weekdays.map((day) => `<div class="calendar-weekday">${day}</div>`);
  for (const date = new Date(gridStart); date <= gridEnd; date.setDate(date.getDate() + 1)) {
    const iso = formatLocalDate(date);
    const count = countsByDate[iso] || 0;
    const holiday = koreanHolidayInfo(iso);
    const isSunday = date.getDay() === 0;
    const holidayTitle = holiday?.name || (isSunday ? "일요일" : "");
    const classes = [
      "calendar-day",
      inDateRange(iso, safeStart, safeEnd) ? "in-period" : "",
      count ? "has-data" : "",
      isSunday ? "is-sunday" : "",
      holiday ? "is-holiday" : "",
      selectedStart && selectedEnd && inDateRange(iso, selectedStart, selectedEnd) ? "range-selected" : "",
      selectedStart && (iso === selectedStart || iso === selectedEnd) ? "range-edge" : "",
      iso === selectedDate ? "selected" : ""
    ].filter(Boolean).join(" ");
    parts.push(`
      <button class="${classes}" type="button" data-date="${iso}"${holidayTitle ? ` title="${escapeHtml(holidayTitle)}" aria-label="${escapeHtml(`${iso} ${holidayTitle}`)}"` : ""}>
        <span>${date.getDate()}</span>
        ${count ? `<small>${formatNumber(count)}건</small>` : "<small>&nbsp;</small>"}
      </button>
    `);
  }
  calendar.innerHTML = parts.join("");
}

function promotionSummary(records) {
  return promotionsForCurrentDashboardMonth().map((promo) => {
    const normalized = normalizePromotion(promo);
    const matched = records.filter((record) => recordMatchesPromo(record, normalized));
    const score = matched.reduce((sum, record) => sum + promoRecordScore(record, normalized), 0);
    return {
      ...normalized,
      count: matched.reduce((sum, record) => sum + toNumber(record.count), 0),
      score
    };
  }).filter((item) => item.count > 0 || item.score > 0);
}

function normalizePromotion(promo = {}) {
  const type = promo.type === "product" ? "product" : (promo.type === "score" ? "score" : "count");
  const countRules = Array.isArray(promo.countRules) ? promo.countRules : (Array.isArray(promo.rewardRules) ? promo.rewardRules.map(parsePromoRewardRule) : []);
  const rawScoreRules = Array.isArray(promo.scoreRules) ? promo.scoreRules : (Array.isArray(promo.keywords) ? promo.keywords.map(parsePromoKeyword) : []);
  const scoreRules = rawScoreRules.map((rule) => {
    const title = rule.title || rule.productTitle || rule.name || rule.keyword || "";
    const keywordsValue = rule.keywords !== undefined ? rule.keywords : (rule.keyword || "");
    const keywords = Array.isArray(keywordsValue)
      ? keywordsValue
      : String(keywordsValue || "").split(",").map((item) => item.trim()).filter(Boolean);
    const excludeValue = rule.excludeKeywords !== undefined ? rule.excludeKeywords : (rule.excludeKeyword || rule.exclude || "");
    const excludeKeywords = Array.isArray(excludeValue)
      ? excludeValue
      : String(excludeValue || "").split(",").map((item) => item.trim()).filter(Boolean);
    return {
      title: String(title || "").trim(),
      keyword: keywords.join(", "),
      keywords,
      excludeKeyword: excludeKeywords.join(", "),
      excludeKeywords,
      score: toNumber(rule.score || 1) || 1
    };
  }).filter((rule) => rule.title && rule.keywords.length);

  const scoreRewardRules = Array.isArray(promo.scoreRewardRules) ? promo.scoreRewardRules : countRules;
  const productRules = Array.isArray(promo.productRules) ? promo.productRules : [];
  return {
    id: promo.id || uid("p"),
    name: promo.name || "",
    startDate: promo.startDate || "",
    endDate: promo.endDate || "",
    dateBasis: "receivedDate",
    type,
    includePendingRecords: promo.includePendingRecords || {},
    keywords: Array.isArray(promo.keywords) ? promo.keywords : [],
    countRules: countRules.map((rule) => ({
      threshold: toNumber(rule.threshold || rule.count || 0),
      reward: rule.reward || "",
      quantity: toNumber(rule.quantity || 1) || 1
    })).filter((rule) => rule.threshold > 0 && rule.reward),
    scoreRules,
    scoreRewardRules: scoreRewardRules.map((rule) => ({
      threshold: toNumber(rule.threshold || 0),
      reward: rule.reward || "",
      quantity: toNumber(rule.quantity || 1) || 1
    })).filter((rule) => rule.threshold > 0 && rule.reward),
    productRules: productRules.map((rule) => {
      const title = rule.title || rule.productTitle || rule.name || rule.keyword || "";
      const keywordValue = rule.keywords !== undefined ? rule.keywords : (rule.keyword || "");
      const keywords = Array.isArray(keywordValue)
        ? keywordValue
        : String(keywordValue || "").split(",").map((item) => item.trim()).filter(Boolean);
      return {
        title: String(title || "").trim(),
        keyword: keywords.join(", "),
        keywords,
        reward: rule.reward || "",
        quantity: toNumber(rule.quantity || 1) || 1
      };
    }).filter((rule) => rule.title && rule.keywords.length),
    memo: promo.memo || ""
  };
}
function parsePromoKeyword(raw) {
  const [keyword, score] = String(raw || "").split("=");
  const title = (keyword || "").trim();
  return { title, keyword: title, keywords: title ? [title] : [], score: toNumber(score || 1) || 1 };
}
function parsePromoRewardRule(raw) {
  const [left, right] = String(raw || "").split("=");
  const rawReward = (right || "").trim();
  const quantityMatch = rawReward.match(/(.+?)\s*[xX*]\s*(\d+)$/);
  return {
    threshold: toNumber(left || 0),
    reward: quantityMatch ? quantityMatch[1].trim() : rawReward,
    quantity: quantityMatch ? toNumber(quantityMatch[2]) : 1
  };
}

function isInstalledRecord(record) {
  return ["설치완료", "완료"].includes(compactValue(record.status, ""));
}

function promoKeywordRules(promo) {
  promo = normalizePromotion(promo);
  if (promo.type === "count") return [];
  if (promo.type === "product") return promo.productRules.map((rule) => ({ title: rule.title || rule.keyword, keyword: rule.keyword, keywords: rule.keywords || [rule.keyword], score: 1 }));
  return promo.scoreRules;
}

function promoRewardRules(promo) {
  promo = normalizePromotion(promo);
  if (promo.type === "score") return promo.scoreRewardRules.sort((a, b) => b.threshold - a.threshold);
  if (promo.type === "product") return promo.productRules.map((rule) => ({ threshold: 1, reward: rule.reward, quantity: rule.quantity, keyword: rule.keyword, title: rule.title, keywords: rule.keywords }));
  return promo.countRules.sort((a, b) => b.threshold - a.threshold);
}

function promoRewardFor(value, promo, matchedKeyword = "") {
  promo = normalizePromotion(promo);
  if (promo.type === "product") {
    const rule = promo.productRules.find((item) => item.title === matchedKeyword || item.keyword === matchedKeyword) || promo.productRules.find((item) => value > 0);
    return rule ? { threshold: 1, reward: rule.reward || rule.title || rule.keyword, quantity: rule.quantity || 1 } : null;
  }
  return promoRewardRules(promo).find((rule) => value >= rule.threshold) || null;
}

function promoRecordKey(record) {
  return String(record?.id || [
    record?.receivedDate || "",
    record?.installDate || "",
    record?.manager || "",
    record?.customerNo || "",
    record?.previousCustomer || "",
    record?.customerName || "",
    record?.phone || "",
    record?.product || "",
    record?.count || ""
  ].join("|"));
}

function promotionStatus(promo) {
  promo = normalizePromotion(promo);
  const today = todayIso();
  if (promo.startDate && today < promo.startDate) return "예정";
  if (promo.endDate && today > promo.endDate) return "종료";
  return "진행중";
}

function promoBaseRecordMatches(record, promo) {
  promo = normalizePromotion(promo);
  if (record.status === "취소") return false;
  const dateValue = record.receivedDate || "";
  if (!inDateRange(dateValue, promo.startDate, promo.endDate)) return false;

  const rules = promoKeywordRules(promo);
  if (!rules.length) return true;
  const text = `${record.product}`.toLowerCase();
  return rules.some((rule) => {
    const keywords = Array.isArray(rule.keywords) && rule.keywords.length ? rule.keywords : [rule.keyword];
    const excludeKeywords = Array.isArray(rule.excludeKeywords) ? rule.excludeKeywords : [];
    const hasInclude = keywords.some((keyword) => keyword && text.includes(String(keyword).toLowerCase()));
    const hasExclude = excludeKeywords.some((keyword) => keyword && text.includes(String(keyword).toLowerCase()));
    return hasInclude && !hasExclude;
  });
}
function promoRecordApprovalValue(record, promo) {
  const key = promoRecordKey(record);
  const stored = normalizePromotion(promo).includePendingRecords?.[key];
  if (stored === true) return "yes";
  if (stored === false) return "no";
  return isInstalledRecord(record) ? "yes" : "no";
}

function isPromoRecordAccepted(record, promo) {
  return promoRecordApprovalValue(record, promo) === "yes";
}

function recordMatchesPromo(record, promo, managerName = "") {
  promo = normalizePromotion(promo);
  return promoBaseRecordMatches(record, promo) && isPromoRecordAccepted(record, promo);
}


function matchedPromoKeyword(record, promo) {
  const text = `${record.product}`.toLowerCase();
  return promoKeywordRules(promo).find((rule) => {
    const keywords = Array.isArray(rule.keywords) && rule.keywords.length ? rule.keywords : [rule.keyword];
    const excludeKeywords = Array.isArray(rule.excludeKeywords) ? rule.excludeKeywords : [];
    const hasInclude = keywords.some((keyword) => keyword && text.includes(String(keyword).toLowerCase()));
    const hasExclude = excludeKeywords.some((keyword) => keyword && text.includes(String(keyword).toLowerCase()));
    return hasInclude && !hasExclude;
  });
}
function promoRecordScore(record, promo) {
  promo = normalizePromotion(promo);
  if (promo.type === "count" || promo.type === "product") return toNumber(record.count);
  const matched = matchedPromoKeyword(record, promo);
  return matched ? toNumber(record.count) * matched.score : 0;
}

function promoCreditManagerName(record, month = "") {
  // 프로모션도 실제실적 표와 같은 실판매자 귀속 기준을 사용합니다.
  // 실판매자가 '팀장'이나 '고객센터' 등 역할명인 경우 빈 이름으로 버리던
  // 과거 코드는 정상 접수건이 프로모션에서 0건으로 나오는 원인이었습니다.
  const targetMonth = normalizeManagerMonth(month)
    || recordGoalMonth(record, currentPromotionMonth());
  const managerNames = new Set(teamManagerNames(targetMonth));
  const credited = actualPerformanceCreditManagerName(record, managerNames);
  return managerNames.has(credited) ? credited : "";
}

function promotionReferenceMonth(promo) {
  // 시작일의 달력월이 아니라, 그 날짜를 포함하는 '목표 산정월'을 사용합니다.
  // 예: 8/28~9/28 프로모션은 9월 목표월의 매니저·소속 이력으로 집계.
  const date = String(promo?.startDate || promo?.endDate || "").slice(0, 10);
  if (/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    return goalMonthForDate(date, date.slice(0, 7));
  }
  return normalizeManagerMonth(currentPromotionMonth()) || currentDashboardMonth();
}

function promoRecords(promo, managerName = "") {
  const month = promotionReferenceMonth(promo);
  return state.records
    .filter((record) => recordBelongsToCurrentUserTeam(record, recordGoalMonth(record, month)))
    .filter((record) => !managerName || promoCreditManagerName(record, month) === managerName)
    .filter((record) => recordMatchesPromo(record, promo, managerName));
}

function promoPendingRecords(promo, managerName) {
  promo = normalizePromotion(promo);
  const month = promotionReferenceMonth(promo);
  return state.records
    .filter((record) => recordBelongsToCurrentUserTeam(record, recordGoalMonth(record, month)))
    .filter((record) => promoCreditManagerName(record, month) === managerName)
    .filter((record) => promoBaseRecordMatches(record, promo))
    .filter((record) => !isInstalledRecord(record))
    .filter((record) => !isPromoRecordAccepted(record, promo));
}

function promoAllManagerRecords(promo, managerName) {
  promo = normalizePromotion(promo);
  const month = promotionReferenceMonth(promo);
  return state.records
    .filter((record) => recordBelongsToCurrentUserTeam(record, recordGoalMonth(record, month)))
    .filter((record) => promoCreditManagerName(record, promotionReferenceMonth(promo)) === managerName)
    .filter((record) => promoBaseRecordMatches(record, promo));
}


function promoManagerStats(promo) {
  promo = normalizePromotion(promo);
  const month = promotionReferenceMonth(promo);
  const managers = teamManagers(month);
  const names = new Set(managers.map((manager) => manager.name));
  const grouped = new Map(managers.map((manager) => [manager.name, []]));
  for (const record of state.records || []) {
    if (!promoBaseRecordMatches(record, promo)) continue;
    if (!recordBelongsToCurrentUserTeam(record, recordGoalMonth(record, month))) continue;
    const credited = actualPerformanceCreditManagerName(record, names);
    if (names.has(credited)) grouped.get(credited).push(record);
  }
  return managers.map((manager) => {
    const allRecords = grouped.get(manager.name);
    const managerRecords = allRecords.filter((record) => isPromoRecordAccepted(record, promo));
    const pendingRecords = allRecords.filter((record) => !isInstalledRecord(record) && !isPromoRecordAccepted(record, promo));
    const count = managerRecords.reduce((sum, record) => sum + toNumber(record.count), 0);
    const pendingCount = pendingRecords.reduce((sum, record) => sum + toNumber(record.count), 0);
    // 누적점수·달성단계는 미설치 대상 건까지 포함합니다. 인정 건수와 미설치 건수 표시는 기존 기준을 유지합니다.
    const score = allRecords.reduce((sum, record) => sum + promoRecordScore(record, promo), 0);
    const matched = allRecords.map((record) => { const rule = matchedPromoKeyword(record, promo); return rule?.title || rule?.keyword; }).filter(Boolean)[0] || "";
    const basisValue = promo.type === "score" ? score : count;
    const reward = promoRewardFor(basisValue, promo, matched);
    return { manager: manager.name, count, pendingCount, score, basisValue, reward, records: managerRecords, pendingRecords, allRecords };
  }).sort((a, b) => {
    if (b.basisValue !== a.basisValue) return b.basisValue - a.basisValue;
    if (b.pendingCount !== a.pendingCount) return b.pendingCount - a.pendingCount;
    return a.manager.localeCompare(b.manager, "ko");
  });
}


function activePromotion() {
  const selectedId = $("#promoId")?.value || state.promotions[0]?.id || "";
  return normalizePromotion(state.promotions.find((promo) => promo.id === selectedId) || state.promotions[0] || {});
}

function collectCountRuleRows() {
  return $$("#promoCountRuleRows .promo-rule-row").map((row) => ({
    threshold: toNumber(row.querySelector(".rule-threshold")?.value || 0),
    reward: row.querySelector(".rule-reward")?.value.trim() || "",
    quantity: toNumber(row.querySelector(".rule-quantity")?.value || 1) || 1
  })).filter((rule) => rule.threshold > 0 && rule.reward);
}

function collectScoreRuleRows() {
  return $$("#promoScoreRuleRows .promo-rule-row").map((row) => {
    const title = row.querySelector(".rule-title")?.value.trim() || "";
    const keywordText = row.querySelector(".rule-keyword")?.value.trim() || "";
    const excludeText = row.querySelector(".rule-exclude-keyword")?.value.trim() || "";
    const keywords = keywordText.split(",").map((item) => item.trim()).filter(Boolean);
    const excludeKeywords = excludeText.split(",").map((item) => item.trim()).filter(Boolean);
    return {
      title,
      keyword: keywordText,
      keywords,
      excludeKeyword: excludeText,
      excludeKeywords,
      score: toNumber(row.querySelector(".rule-score")?.value || 1) || 1
    };
  }).filter((rule) => rule.title && rule.keywords.length);
}

function collectScoreRewardRows() {
  return $$("#promoScoreRewardRows .promo-rule-row").map((row) => ({
    threshold: toNumber(row.querySelector(".rule-threshold")?.value || 0),
    reward: row.querySelector(".rule-reward")?.value.trim() || "",
    quantity: toNumber(row.querySelector(".rule-quantity")?.value || 1) || 1
  })).filter((rule) => rule.threshold > 0 && rule.reward);
}

function collectProductRuleRows() {
  return $$("#promoProductRuleRows .promo-rule-row").map((row) => {
    const title = row.querySelector(".rule-title")?.value.trim() || "";
    const keywordText = row.querySelector(".rule-keyword")?.value.trim() || "";
    const keywords = keywordText.split(",").map((item) => item.trim()).filter(Boolean);
    return {
      title,
      keyword: keywordText,
      keywords,
      reward: row.querySelector(".rule-reward")?.value.trim() || "",
      quantity: toNumber(row.querySelector(".rule-quantity")?.value || 1) || 1
    };
  }).filter((rule) => rule.title && rule.keywords.length);
}

function renderCountRuleRows(rules = []) {
  const wrap = $("#promoCountRuleRows");
  if (!wrap) return;
  const safeRules = rules.length ? rules : [{ threshold: 1, reward: "", quantity: 1 }];
  wrap.innerHTML = safeRules.map((rule) => `
    <div class="promo-rule-row count-rule-grid">
      <input class="rule-threshold" type="number" min="1" step="1" value="${escapeHtml(rule.threshold || 1)}" placeholder="1">
      <input class="rule-reward" value="${escapeHtml(rule.reward || "")}" placeholder="예: 냄비 2종 세트">
      <input class="rule-quantity" type="number" min="1" step="1" value="${escapeHtml(rule.quantity || 1)}" placeholder="1">
      <button class="icon-button remove-promo-rule" type="button">X</button>
    </div>
  `).join("");
}

function renderScoreRuleRows(rules = []) {
  const wrap = $("#promoScoreRuleRows");
  if (!wrap) return;
  const safeRules = rules.length ? rules : [{ title: "", keyword: "", keywords: [], excludeKeyword: "", excludeKeywords: [], score: 1 }];
  wrap.innerHTML = safeRules.map((rule) => {
    const keywordText = Array.isArray(rule.keywords) && rule.keywords.length ? rule.keywords.join(", ") : (rule.keyword || "");
    const excludeText = Array.isArray(rule.excludeKeywords) && rule.excludeKeywords.length ? rule.excludeKeywords.join(", ") : (rule.excludeKeyword || "");
    return `
    <div class="promo-rule-row score-rule-grid">
      <input class="rule-title" value="${escapeHtml(rule.title || "")}" placeholder="예: 100도 정수기">
      <input class="rule-keyword" value="${escapeHtml(keywordText)}" placeholder="예: AMS, AHS, AHSC">
      <input class="rule-exclude-keyword" value="${escapeHtml(excludeText)}" placeholder="예: 리퍼브">
      <input class="rule-score" type="number" min="0" step="0.5" value="${escapeHtml(rule.score || 1)}" placeholder="50">
      <button class="icon-button remove-promo-rule" type="button">X</button>
    </div>`;
  }).join("");
}

function renderScoreRewardRows(rules = []) {
  const wrap = $("#promoScoreRewardRows");
  if (!wrap) return;
  const safeRules = rules.length ? rules : [{ threshold: 1, reward: "", quantity: 1 }];
  wrap.innerHTML = safeRules.map((rule) => `
    <div class="promo-rule-row count-rule-grid">
      <input class="rule-threshold" type="number" min="1" step="1" value="${escapeHtml(rule.threshold || 1)}" placeholder="1">
      <input class="rule-reward" value="${escapeHtml(rule.reward || "")}" placeholder="예: 주유권 5만원">
      <input class="rule-quantity" type="number" min="1" step="1" value="${escapeHtml(rule.quantity || 1)}" placeholder="1">
      <button class="icon-button remove-promo-rule" type="button">X</button>
    </div>
  `).join("");
}

function renderProductRuleRows(rules = []) {
  const wrap = $("#promoProductRuleRows");
  if (!wrap) return;
  const safeRules = rules.length ? rules : [{ title: "", keyword: "", keywords: [], reward: "", quantity: 1 }];
  wrap.innerHTML = safeRules.map((rule) => {
    const keywordText = Array.isArray(rule.keywords) && rule.keywords.length ? rule.keywords.join(", ") : (rule.keyword || "");
    return `
    <div class="promo-rule-row product-rule-grid">
      <input class="rule-title" value="${escapeHtml(rule.title || "")}" placeholder="예: 안마의자">
      <input class="rule-keyword" value="${escapeHtml(keywordText)}" placeholder="예: CMS">
      <input class="rule-reward" value="${escapeHtml(rule.reward || "")}" placeholder="예: 주유권 5만원">
      <input class="rule-quantity" type="number" min="1" step="1" value="${escapeHtml(rule.quantity || 1)}" placeholder="1">
      <button class="icon-button remove-promo-rule" type="button">X</button>
    </div>`;
  }).join("");
}

function setPromoType(type) {
  const safeType = ["count", "score", "product"].includes(type) ? type : "count";
  const input = $("#promoTypeInput");
  if (input) input.value = safeType;
  $$(".promo-type-btn").forEach((button) => button.classList.toggle("active", button.dataset.promoType === safeType));
  $$(".promo-condition-editor[data-promo-section]").forEach((section) => {
    section.hidden = section.dataset.promoSection !== safeType;
  });
  renderPromotionDetail();
}

function syncPromoDateMode() {
  const mode = document.querySelector("input[name='promoDateMode']:checked")?.value || "range";
  const start = $("#promoStartInput");
  const end = $("#promoEndInput");
  if (mode === "single" && start && end) {
    end.value = start.value;
    end.disabled = true;
  } else if (end) {
    end.disabled = false;
  }
}


function recordSortValue(record, fallbackIndex = 0) {
  const value = Number(record.manualOrder);
  return Number.isFinite(value) && value > 0 ? value : fallbackIndex + 1;
}

function currentManagerFilterValue() {
  return $("#recordManagerFilter")?.value || "";
}

function managerOrderList(managerName) {
  if (!state.managerManualOrder) state.managerManualOrder = {};
  if (!Array.isArray(state.managerManualOrder[managerName])) state.managerManualOrder[managerName] = [];
  return state.managerManualOrder[managerName];
}

function normalizedPhoneDigits(value) {
  return String(value || "").replace(/\D/g, "");
}

function currentUserTeamName(month = currentDashboardMonth()) {
  const configured = configuredTeamNames();
  const targetMonth = normalizeManagerMonth(month) || currentDashboardMonth();
  if (teamOperationMode(targetMonth) === "1") return "";
  return masterTeamForMonth(targetMonth) || configured[0] || "원팀";
}

function currentTeamForManager(managerOrName, month = currentDashboardMonth()) {
  const manager = typeof managerOrName === "string" ? managerByName(managerOrName) : managerOrName;
  if (!manager?.name) return "";
  return normalizeTeamName(managerTeamForMonth(manager, month));
}

// 실적의 목표월과 조직 이력은 분리합니다. 조회는 목표산정기간으로 하고,
// 각 접수의 소속팀과 재직 상태는 실제 접수일의 달력월 이력으로 확인합니다.
// 기존 managerTeamAtRecord는 등록 매니저 이력이 없을 때의 보조정보로 보존합니다.
function recordTeamForGoalMonth(record, targetMonth = "") {
  const manager = managerById(record?.managerId) || managerByName(record?.managerNameAtRecord || record?.manager);
  const month = organizationMonthForDate(record?.receivedDate || record?.installDate, targetMonth || currentDashboardMonth());
  if (!manager?.name) return normalizeTeamName(record?.managerTeamAtRecord || "");
  const history = normalizeManagerTeamHistory(manager.teamHistory, manager.team, manager.joinedMonth);
  const matching = history
    .filter((item) => (!item.startMonth || item.startMonth <= month) && (!item.endMonth || month <= item.endMonth))
    .sort((a, b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0];
  if (matching?.team) return normalizeTeamName(matching.team);
  if (record?.managerTeamAtRecord) return normalizeTeamName(record.managerTeamAtRecord);
  return normalizeTeamName(manager.team || "");
}

function recordStatusForGoalMonth(record, targetMonth = "") {
  const manager = managerById(record?.managerId) || managerByName(record?.managerNameAtRecord || record?.manager);
  const month = organizationMonthForDate(record?.receivedDate || record?.installDate, targetMonth || currentDashboardMonth());
  if (!manager?.name) return record?.managerStatusAtRecord === "inactive" ? "inactive" : "active";
  const history = normalizeStatusHistory(manager.statusHistory, manager.status === "inactive" ? "inactive" : "active", manager.joinedMonth);
  const matching = history
    .filter((item) => (!item.startMonth || item.startMonth <= month) && (!item.endMonth || month <= item.endMonth))
    .sort((a, b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0];
  if (matching?.status) return matching.status;
  if (record?.managerStatusAtRecord === "inactive" || record?.managerStatusAtRecord === "active") return record.managerStatusAtRecord;
  return manager.status === "inactive" ? "inactive" : "active";
}

function recordBelongsToCurrentUserTeam(record, month = "") {
  if (!record) return false;
  const manager = managerById(record.managerId) || managerByName(record.managerNameAtRecord || record.manager);
  const calendarMonth = organizationMonthForDate(record.receivedDate || record.installDate, month || currentDashboardMonth());
  // 과거 접수는 퇴사 후에도 유지하며, 이미 저장된 접수를 비활성 상태로 숨기지 않습니다.
  if (teamOperationMode(calendarMonth) === "1") return Boolean(manager || record.manager);
  const managerTeam = recordTeamForGoalMonth(record, month);
  return Boolean(managerTeam) && managerTeam === currentUserTeamName(calendarMonth);
}

function filteredRecordSetForList() {
  const statusFilter = $("#recordStatusFilter")?.value || "";
  const managerFilter = $("#recordManagerFilter")?.value || "";
  const categoryFilter = $("#recordCategoryFilter")?.value || "";
  const sellerFilter = $("#recordSellerFilter")?.value || "";
  const simpleSearch = ($("#recordSimpleSearch")?.value || "").trim().toLowerCase();
  const simpleSearchDigits = normalizedPhoneDigits(simpleSearch);

  const teamScoped = state?.appMeta?.teamScopedRecords !== false;

  // 조회기간은 목표월 산정기간을 따르되, 접수별 조직이력은 실제 접수일의 달력월을 사용합니다.
  const selectedGoalMonth = normalizeManagerMonth($("#recordMonthFilter")?.value) || currentDashboardMonth();
  return recordsByRecordPeriod()
    .filter((record) => !isMembershipRecord(record))
    .filter((record) => !teamScoped || recordBelongsToCurrentUserTeam(record, selectedGoalMonth))
    .filter((record) => {
      if (!simpleSearch) return true;
      const searchableText = [
        record.status,
        record.receivedDate,
        record.installDate,
        record.manager,
        record.category,
        recordActivityType(record),
        record.count,
        record.previousCustomer,
        record.customerNo,
        record.phone,
        record.customerName,
        record.product,
        record.seller,
        record.memo,
        record.qr,
        record.cashAmount
      ].map((value) => compactValue(value, "").toLowerCase()).join(" ");
      const searchableDigits = normalizedPhoneDigits(searchableText);
      return searchableText.includes(simpleSearch) || (!!simpleSearchDigits && searchableDigits.includes(simpleSearchDigits));
    })
    .filter((record) => !statusFilter || record.status === statusFilter)
    .filter((record) => !managerFilter || record.manager === managerFilter)
    .filter((record) => !categoryFilter || normalizeCategory(record.category) === normalizeCategory(categoryFilter))
    .filter((record) => !sellerFilter || compactValue(record.seller, "") === sellerFilter);
}

function recordDateToTime(value) {
  const text = compactValue(value, "").trim();
  if (!text) return 0;

  const match = text.match(/^(\d{4})[-./](\d{1,2})[-./](\d{1,2})/);
  if (match) {
    const year = Number(match[1]);
    const month = Number(match[2]);
    const day = Number(match[3]);
    const time = new Date(year, month - 1, day).getTime();
    return Number.isFinite(time) ? time : 0;
  }

  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? parsed : 0;
}

function dateSortRecords(records, direction = "desc") {
  const sign = direction === "asc" ? 1 : -1;
  const statusOrder = Object.fromEntries(statuses.map((status, index) => [status, index]));
  return [...records].sort((a, b) => {
    const receivedCompare = (recordDateToTime(a.receivedDate) - recordDateToTime(b.receivedDate)) * sign;
    if (receivedCompare !== 0) return receivedCompare;

    const installCompare = (recordDateToTime(a.installDate) - recordDateToTime(b.installDate)) * sign;
    if (installCompare !== 0) return installCompare;

    const createdCompare = compactValue(a.createdAt || a.updatedAt || "", "")
      .localeCompare(compactValue(b.createdAt || b.updatedAt || "", "")) * sign;
    if (createdCompare !== 0) return createdCompare;

    const statusCompare = (statusOrder[a.status] ?? 99) - (statusOrder[b.status] ?? 99);
    if (statusCompare !== 0) return statusCompare;

    const managerCompare = compactValue(a.manager, "").localeCompare(compactValue(b.manager, ""), "ko");
    if (managerCompare !== 0) return managerCompare;

    return compactValue(a.id || promoRecordKey(a), "").localeCompare(compactValue(b.id || promoRecordKey(b), ""));
  });
}

function recordCreatedSortKey(record) {
  return compactValue(record.createdAt || record.updatedAt || "", "");
}

function recordFallbackSortKey(record) {
  return [
    compactValue(record.receivedDate, ""),
    compactValue(record.installDate, ""),
    compactValue(record.id, "")
  ].join("|");
}

function sortRecordsByCreatedAt(records, direction = "desc") {
  return dateSortRecords(records, direction);
}

function recordListOrderKey(isMembership = false) {
  return isMembership ? "__membership__" : "__records__";
}

function recordIdentity(record) {
  return record?.id || promoRecordKey(record);
}

function fullRecordSetForManualOrder(isMembership = false) {
  return (state.records || []).filter((record) => Boolean(isMembershipRecord(record)) === Boolean(isMembership));
}

function ensureRecordListManualOrder(isMembership = false) {
  const key = recordListOrderKey(isMembership);
  const baseline = dateSortRecords(fullRecordSetForManualOrder(isMembership), "desc");
  const baselineIds = baseline.map(recordIdentity).filter(Boolean);
  const validIds = new Set(baselineIds);
  const prior = managerOrderList(key).filter((id, index, arr) => validIds.has(id) && arr.indexOf(id) === index);

  if (!prior.length) {
    state.managerManualOrder[key] = baselineIds.slice();
    return state.managerManualOrder[key];
  }

  const result = prior.slice();
  const resultSet = new Set(result);
  baselineIds.forEach((id, baselineIndex) => {
    if (resultSet.has(id)) return;
    let insertAt = -1;

    for (let i = baselineIndex - 1; i >= 0; i -= 1) {
      const previousId = baselineIds[i];
      const previousPosition = result.indexOf(previousId);
      if (previousPosition >= 0) {
        insertAt = previousPosition + 1;
        break;
      }
    }
    if (insertAt < 0) {
      for (let i = baselineIndex + 1; i < baselineIds.length; i += 1) {
        const nextId = baselineIds[i];
        const nextPosition = result.indexOf(nextId);
        if (nextPosition >= 0) {
          insertAt = nextPosition;
          break;
        }
      }
    }
    if (insertAt < 0) insertAt = result.length;
    result.splice(insertAt, 0, id);
    resultSet.add(id);
  });

  state.managerManualOrder[key] = result;
  return result;
}

// 접수일은 항상 첫 번째 정렬 기준입니다. 오래된 __records__/__membership__
// 수동 배치 정보는 같은 날짜의 접수끼리만 순서를 결정합니다.
function compareReceiptDates(a, b, direction = "asc") {
  const aTime = recordDateToTime(a?.receivedDate);
  const bTime = recordDateToTime(b?.receivedDate);
  // 비어 있거나 잘못된 날짜는 방향과 관계없이 맨 뒤에 둡니다.
  if (!aTime && !bTime) return 0;
  if (!aTime) return 1;
  if (!bTime) return -1;
  return (aTime - bTime) * (direction === "desc" ? -1 : 1);
}

function sortReceiptListByDate(records, direction = "asc", membership = false) {
  const manualOrder = ensureRecordListManualOrder(membership);
  const manualIndex = new Map(manualOrder.map((id, index) => [id, index]));
  return [...records].sort((a, b) => {
    const dateCompare = compareReceiptDates(a, b, direction);
    if (dateCompare) return dateCompare;
    const aOrder = manualIndex.get(recordIdentity(a)) ?? Number.MAX_SAFE_INTEGER;
    const bOrder = manualIndex.get(recordIdentity(b)) ?? Number.MAX_SAFE_INTEGER;
    if (aOrder !== bOrder) return aOrder - bOrder;
    return String(recordIdentity(a) || "").localeCompare(String(recordIdentity(b) || ""));
  });
}

function sortRecordsForList(records) {
  return sortReceiptListByDate(records, recordSequenceSort, false);
}

function sortMembershipRecordsForList(records = []) {
  const ordered = sortReceiptListByDate(records, membershipRecordDateSort, true);
  return ordered.map((record, index) => ({
    ...record,
    displaySequence: membershipRecordDateSort === "asc" ? index + 1 : ordered.length - index
  }));
}

function syncReceiptDateSortButtons() {
  const configs = [
    ["recordDateSortBtn", "recordDateSortHeader", recordSequenceSort],
    ["mobileRecordDateSortBtn", "", recordSequenceSort],
    ["membershipDateSortBtn", "membershipDateSortHeader", membershipRecordDateSort]
  ];
  configs.forEach(([buttonId, headerId, direction]) => {
    const button = document.getElementById(buttonId);
    if (button) {
      const ascending = direction === "asc";
      button.textContent = buttonId.startsWith("mobile")
        ? `접수일 ${ascending ? "↑" : "↓"}`
        : `접수일 ${ascending ? "▲" : "▼"}`;
      button.title = `접수일 ${ascending ? "오름차순(과거→최근)" : "내림차순(최근→과거)"} · 클릭하여 반대 순서로 보기`;
      button.setAttribute("aria-label", button.title);
      button.setAttribute("aria-pressed", String(!ascending));
    }
    const header = headerId ? document.getElementById(headerId) : null;
    if (header) header.setAttribute("aria-sort", direction === "asc" ? "ascending" : "descending");
  });
}

function resetRecordListToLatestOrder(isMembership = false) {
  const key = recordListOrderKey(isMembership);
  state.managerManualOrder[key] = dateSortRecords(fullRecordSetForManualOrder(isMembership), "desc")
    .map(recordIdentity)
    .filter(Boolean);
}


function syncRecordPeriodFromDashboardMonth(month = $("#monthFilter")?.value || monthIso(), resetFilters = true) {
  if (!month) return;
  const setting = monthSetting(month);
  const recordMonth = $("#recordMonthFilter");
  const recordStart = $("#recordStartDateFilter");
  const recordEnd = $("#recordEndDateFilter");
  const recordBasis = $("#recordDateBasisFilter");

  if (recordMonth) recordMonth.value = month;
  if (recordStart) recordStart.value = setting.periodStart || `${month}-01`;
  if (recordEnd) recordEnd.value = setting.periodEnd || lastDayOfMonth(month);
  if (recordBasis) recordBasis.value = "receivedDate";

  if (resetFilters) {
    ["#recordSimpleSearch", "#recordStatusFilter", "#recordManagerFilter", "#recordCategoryFilter", "#recordSellerFilter"].forEach((selector) => {
      const control = $(selector);
      if (control) control.value = "";
    });
  }
}

function visibleRecordsForCurrentFilters() {
  return sortRecordsForList(filteredRecordSetForList());
}

function saveManagerOrder(managerName, records) {
  if (!managerName) return;
  if (!state.managerManualOrder) state.managerManualOrder = {};
  state.managerManualOrder[managerName] = records.map((record) => record.id || promoRecordKey(record));
}

function moveRecordInCurrentView(recordId, direction) {
  const visibleRecords = sortRecordsForList(filteredRecordSetForList());
  const currentIndex = visibleRecords.findIndex((record) => recordIdentity(record) === recordId);
  const targetIndex = currentIndex + direction;
  if (currentIndex < 0 || targetIndex < 0 || targetIndex >= visibleRecords.length) {
    showToast("더 이상 이동할 수 없습니다.");
    return;
  }
  if (compareReceiptDates(visibleRecords[currentIndex], visibleRecords[targetIndex]) !== 0) {
    showToast("접수일이 같은 내역끼리만 순서를 조정할 수 있습니다.");
    return;
  }

  const fullOrder = ensureRecordListManualOrder(false).slice();
  const currentId = recordIdentity(visibleRecords[currentIndex]);
  const targetId = recordIdentity(visibleRecords[targetIndex]);
  const currentFullIndex = fullOrder.indexOf(currentId);
  const targetFullIndex = fullOrder.indexOf(targetId);
  if (currentFullIndex < 0 || targetFullIndex < 0) return;
  [fullOrder[currentFullIndex], fullOrder[targetFullIndex]] = [fullOrder[targetFullIndex], fullOrder[currentFullIndex]];
  state.managerManualOrder[recordListOrderKey(false)] = fullOrder;
  selectedRecordId = recordId;
  persistState();
  renderRecords();
  showToast(direction < 0 ? "접수내역을 위로 이동했습니다." : "접수내역을 아래로 이동했습니다.");
}

function moveMembershipRecordInCurrentView(recordId, direction) {
  const visibleRecords = sortMembershipRecordsForList(filteredMembershipRecordsByMonth());
  const currentIndex = visibleRecords.findIndex((record) => recordIdentity(record) === recordId);
  const targetIndex = currentIndex + direction;
  if (currentIndex < 0 || targetIndex < 0 || targetIndex >= visibleRecords.length) {
    showToast("더 이상 이동할 수 없습니다.");
    return;
  }
  if (compareReceiptDates(visibleRecords[currentIndex], visibleRecords[targetIndex]) !== 0) {
    showToast("접수일이 같은 내역끼리만 순서를 조정할 수 있습니다.");
    return;
  }

  const fullOrder = ensureRecordListManualOrder(true).slice();
  const currentId = recordIdentity(visibleRecords[currentIndex]);
  const targetId = recordIdentity(visibleRecords[targetIndex]);
  const currentFullIndex = fullOrder.indexOf(currentId);
  const targetFullIndex = fullOrder.indexOf(targetId);
  if (currentFullIndex < 0 || targetFullIndex < 0) return;
  [fullOrder[currentFullIndex], fullOrder[targetFullIndex]] = [fullOrder[targetFullIndex], fullOrder[currentFullIndex]];
  state.managerManualOrder[recordListOrderKey(true)] = fullOrder;
  selectedRecordId = "";
  persistState();
  renderMembershipRecords();
  showToast(direction < 0 ? "맴버쉽 내역을 위로 이동했습니다." : "맴버쉽 내역을 아래로 이동했습니다.");
}


function recordsForPrint() {
  return visibleRecordsForCurrentFilters();
}

function recordPrintHtml(records) {
  const meta = state.appMeta || {};
  const periodText = `${$("#recordStartDateFilter")?.value || "-"} ~ ${$("#recordEndDateFilter")?.value || "-"}`;
  const rows = records.map((record, index) => {
    const phone = typeof formatPhoneNumber === "function" ? formatPhoneNumber(record.phone) : compactValue(record.phone, "");
    return `
      <tr>
        <td class="seq">${recordSequenceSort === "asc" ? index + 1 : records.length - index}</td>
        <td class="date"><strong>${escapeHtml(compactValue(record.receivedDate))}</strong>${record.installDate ? `<br><span>${escapeHtml(record.installDate)}</span>` : ""}</td>
        <td class="status">${escapeHtml(compactValue(record.status))}</td>
        <td class="manager">${escapeHtml(compactValue(record.manager))}</td>
        <td class="category">${escapeHtml(compactValue(record.category))}</td>
        <td class="activity">${escapeHtml(recordActivityType(record) || "")}</td>
        <td class="count">${formatNumber(record.count)}</td>
        <td class="customer-no">${record.previousCustomer ? `<span>${escapeHtml(record.previousCustomer)}</span><br>` : ""}<strong>${escapeHtml(compactValue(record.customerNo))}</strong></td>
        <td class="customer"><strong>${escapeHtml(compactValue(record.customerName))}</strong>${phone ? `<br><span>${escapeHtml(phone)}</span>` : ""}</td>
        <td class="product">${escapeHtml(compactValue(record.product))}</td>
        <td class="seller">${escapeHtml(compactValue(record.seller, "-"))}</td>
        <td class="memo">${escapeHtml(compactValue(record.memo))}</td>
      </tr>`;
  }).join("");

  return `<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>접수내역 출력</title>
<style>
  @page {
    size: A4 landscape;
    margin: 10mm 8mm 13mm 8mm;
    @bottom-center {
      content: counter(page) " / " counter(pages);
      font-size: 10px;
      color: #4f5c56;
      font-family: "Malgun Gothic", "Segoe UI", sans-serif;
    }
  }
  html, body { margin: 0; padding: 0; background: #fff; color: #17201c; font-family: "Malgun Gothic", "Segoe UI", sans-serif; }
  .print-page { width: 100%; }
  .print-head {
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
    border-bottom: 2px solid #1f7a56;
    padding-bottom: 8px;
    margin-bottom: 8px;
  }
  h1 { margin: 0; font-size: 22px; font-weight: 900; }
  .meta { font-size: 12px; color: #4f5c56; font-weight: 700; }
  .count-line { margin: 0 0 8px; font-size: 12px; font-weight: 800; color: #2e3b35; }
  table { width: 100%; border-collapse: collapse; table-layout: fixed; font-size: 10.5px; }
  thead { display: table-header-group; }
  tfoot { display: table-footer-group; }
  tr { break-inside: avoid; page-break-inside: avoid; }
  th, td { border: 1px solid #cfdad4; padding: 5px 4px; vertical-align: middle; text-align: center; word-break: keep-all; overflow-wrap: anywhere; }
  th { background: #dcebe4; color: #10231a; font-weight: 900; }
  .repeat-title-row th {
    background: #ffffff;
    color: #17201c;
    text-align: left;
    font-size: 13px;
    padding: 6px 4px;
    border-left: 0;
    border-right: 0;
    border-top: 0;
    border-bottom: 2px solid #1f7a56;
  }
  tbody tr:nth-child(even) td { background: #f8fbf9; }
  .seq { width: 22px; font-weight: 900; }
  .date { width: 72px; }
  .status { width: 34px; }
  .manager { width: 44px; font-weight: 800; }
  .category { width: 46px; }
  .activity { width: 42px; font-weight: 800; }
  .count { width: 28px; font-weight: 900; }
  .customer-no { width: 118px; }
  .customer { width: 74px; }
  .product { width: 360px; text-align: left; line-height: 1.25; }
  .seller { width: 38px; text-align: center; }
  .memo { width: 120px; text-align: left; line-height: 1.25; }
  th.product, th.product-col { text-align: center; }
  .seq, .status, .manager, .category, .activity, .count, .customer, .seller {
    white-space: nowrap;
    word-break: keep-all;
    overflow-wrap: normal;
  }
  th.product, th.product-col { text-align: center; }
  td.product, .product { white-space: normal; word-break: keep-all; overflow-wrap: anywhere; }
  span { color: #59645f; font-size: 9.5px; }
  .page-number-fallback {
    display: none;
    text-align: center;
    font-size: 10px;
    color: #4f5c56;
    margin-top: 6px;
  }
  @media print {
    .page-number-fallback { display: none; }
  }
</style>
</head>
<body>
  <div class="print-page">
    <div class="print-head">
      <div>
        <h1>${escapeHtml(meta.branchName || "SK지국")} ${escapeHtml(meta.masterName || "사용자")} ${escapeHtml(meta.masterRole || "담당자")} 접수내역</h1>
        <div class="meta">기간 ${escapeHtml(periodText)} · ${recordSequenceSort === "asc" ? "접수일 오름차순" : "접수일 내림차순"} · 순번은 표시순서</div>
      </div>
      <div class="meta">${escapeHtml(formatKoreanLongDate())}</div>
    </div>
    <p class="count-line">총 ${formatNumber(records.length)}건</p>
    <table>
      <thead>
        <tr class="repeat-title-row">
          <th colspan="12">
            ${escapeHtml(meta.branchName || "SK지국")} ${escapeHtml(meta.masterName || "사용자")} ${escapeHtml(meta.masterRole || "담당자")} 접수내역 · 기간 ${escapeHtml(periodText)} · ${recordSequenceSort === "asc" ? "접수일 오름차순" : "접수일 내림차순"} · 순번은 표시순서
          </th>
        </tr>
        <tr>
          <th class="seq">순번</th>
          <th class="date">접수일<br><span>설치요청일</span></th>
          <th class="status">상태</th>
          <th class="manager">매니저</th>
          <th class="category">판매종류</th>
          <th class="activity">구분</th>
          <th class="count">건수</th>
          <th class="customer-no">기존/신규 고객번호</th>
          <th class="customer">고객명<br><span>연락처</span></th>
          <th class="product">제품명</th>
          <th class="seller">실판매자</th>
          <th class="memo">기타내용</th>
        </tr>
      </thead>
      <tbody>${rows || `<tr><td colspan="12">출력할 접수내역이 없습니다.</td></tr>`}</tbody>
    </table>
    <div class="page-number-fallback">페이지 번호는 인쇄 미리보기 하단에 표시됩니다.</div>
  </div>
</body>
</html>`;
}

function printRecordList() {
  const records = recordsForPrint();
  const oldFrame = document.getElementById("record-print-frame");
  if (oldFrame) oldFrame.remove();

  const iframe = document.createElement("iframe");
  iframe.id = "record-print-frame";
  iframe.style.position = "fixed";
  iframe.style.right = "0";
  iframe.style.bottom = "0";
  iframe.style.width = "0";
  iframe.style.height = "0";
  iframe.style.border = "0";
  document.body.appendChild(iframe);

  const doc = iframe.contentWindow.document;
  doc.open();
  doc.write(recordPrintHtml(records));
  doc.close();

  const cleanup = () => {
    setTimeout(() => {
      const frame = document.getElementById("record-print-frame");
      if (frame) frame.remove();
    }, 1000);
  };

  iframe.onload = () => {
    const win = iframe.contentWindow;
    if (!win) return;
    win.onafterprint = cleanup;
    setTimeout(() => {
      win.focus();
      win.print();
    }, 250);
  };
}




function mobileOnlyViewport() {
  return window.matchMedia && window.matchMedia("(max-width: 850px)").matches;
}


/* V10.70 모바일 화면 2차 정밀 보정 */
function mobileHeaderLabels(table) {
  if (!table) return [];

  // 대시보드 조건/프로모션 표는 rowspan/colspan 헤더라 모바일 라벨을 별도로 정리
  if (table.classList.contains("manager-condition-table")) {
    const secondRow = table.querySelector("thead tr:last-child");
    const detailLabels = secondRow
      ? [...secondRow.children].map((cell) => String(cell.textContent || "").replace(/\s+/g, " ").trim())
      : [];
    return ["매니저", ...detailLabels];
  }

  const headerRows = [...table.querySelectorAll("thead tr")];
  if (!headerRows.length) return [];
  const lastRow = headerRows[headerRows.length - 1];
  return [...lastRow.children].map((cell) =>
    String(cell.textContent || "").replace(/\s+/g, " ").trim()
  );
}

function enhanceMobileDataTables(root = document) {
  const selectors = [
    "#dashboardView #managerStatsTable",
    "#dashboardView .manager-condition-table",
    "#analyticsView table",
    "#evaluationView table",
    "#promotionsView table",
    "#renewalguideView .renewal-sheet-table",
    "#payrollView table",
    "#contactnoteView .contact-note-table"
  ];

  root.querySelectorAll(selectors.join(",")).forEach((table) => {
    if (table.closest(".mini-calendar")) return;
    table.classList.add("mobile-card-table");

    const labels = mobileHeaderLabels(table);
    [...table.querySelectorAll("tbody tr, tfoot tr")].forEach((row) => {
      let logicalIndex = 0;
      [...row.children].forEach((cell) => {
        if (!cell.matches("td,th")) return;
        const span = Number(cell.getAttribute("colspan") || 1);
        const label = labels[logicalIndex] || "";
        cell.dataset.mobileLabel = label;
        logicalIndex += Math.max(1, span);
      });
    });
  });
}

function setupMobileSettingsAccordions() {
  const cards = [...document.querySelectorAll("#settingsView .settings-card")];
  cards.forEach((card, index) => {
    const head = card.querySelector(":scope > .panel-head");
    if (!head) return;

    let button = head.querySelector(".mobile-settings-toggle");
    if (!button) {
      button = document.createElement("button");
      button.type = "button";
      button.className = "mobile-settings-toggle";
      button.setAttribute("aria-label", "설정 펼치기/접기");
      head.appendChild(button);

      button.addEventListener("click", (event) => {
        event.stopPropagation();
        card.classList.toggle("mobile-collapsed");
        button.textContent = card.classList.contains("mobile-collapsed") ? "＋" : "−";
      });

      head.addEventListener("click", (event) => {
        if (event.target.closest("button, input, select, a, label")) return;
        if (!mobileOnlyViewport()) return;
        card.classList.toggle("mobile-collapsed");
        button.textContent = card.classList.contains("mobile-collapsed") ? "＋" : "−";
      });
    }

    if (!card.dataset.mobileAccordionReady) {
      card.dataset.mobileAccordionReady = "1";
      // 첫 카드(버전 확인)는 펼치고 나머지는 접힌 상태로 시작
      if (index > 0) card.classList.add("mobile-collapsed");
    }
    button.textContent = card.classList.contains("mobile-collapsed") ? "＋" : "−";
  });
}

function ensureMobilePromotionEditButtons() {
  const promoList = $("#promoList");
  if (!promoList) return;
  promoList.querySelectorAll(".promo-stable-card").forEach((card) => {
    if (card.querySelector(".mobile-promo-edit-btn")) return;
    const button = document.createElement("button");
    button.type = "button";
    button.className = "mobile-promo-edit-btn";
    button.textContent = "프로모션 편집";
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      card.click();
      const form = $("#promoForm");
      form?.classList.add("mobile-form-open");
      window.setTimeout(() => form?.scrollIntoView({ behavior: "smooth", block: "start" }), 30);
    });
    card.appendChild(button);
  });
}

function closeMobileEntryForms(view = "") {
  if (!mobileOnlyViewport()) return;
  if (view !== "records") $(".record-form-panel")?.classList.remove("mobile-form-open");
  if (view !== "checklist") $(".checklist-form-panel")?.classList.remove("mobile-form-open");
  if (view !== "contactnote") $(".contact-note-form")?.classList.remove("mobile-form-open");
  if (view !== "contactrequest") $(".contact-request-form")?.classList.remove("mobile-form-open");
  if (view !== "promotions") $(".promo-stable-form-panel")?.classList.remove("mobile-form-open");
}

function openMobilePanelForm(selector, resetFn) {
  const panel = $(selector);
  if (!panel) return;
  if (typeof resetFn === "function") resetFn();
  panel.classList.add("mobile-form-open");
  window.setTimeout(() => panel.scrollIntoView({ behavior: "smooth", block: "start" }), 30);
}

function attachMobileFullMenuEvents() {
  $("#mobileChecklistNewBtn")?.addEventListener("click", () => {
    const panel = $(".checklist-form-panel");
    if (panel?.classList.contains("mobile-form-open")) {
      panel.classList.remove("mobile-form-open");
      return;
    }
    openMobilePanelForm(".checklist-form-panel", resetChecklistForm);
  });

  $("#mobileContactNoteNewBtn")?.addEventListener("click", () => {
    const panel = $(".contact-note-form");
    if (panel?.classList.contains("mobile-form-open")) {
      panel.classList.remove("mobile-form-open");
      return;
    }
    openMobilePanelForm(".contact-note-form", resetContactNoteForm);
  });

  $("#mobileContactRequestNewBtn")?.addEventListener("click", () => {
    const panel = $(".contact-request-form");
    if (panel?.classList.contains("mobile-form-open")) {
      panel.classList.remove("mobile-form-open");
      return;
    }
    openMobilePanelForm(".contact-request-form", resetContactRequestForm);
  });

  $("#newPromotionBtn")?.addEventListener("click", () => {
    if (!mobileOnlyViewport()) return;
    const form = $("#promoForm");
    form?.classList.add("mobile-form-open");
    window.setTimeout(() => form?.scrollIntoView({ behavior: "smooth", block: "start" }), 30);
  });

  $("#checklistList")?.addEventListener("click", (event) => {
    if (!mobileOnlyViewport()) return;
    if (!event.target.closest("[data-checklist-edit]")) return;
    $(".checklist-form-panel")?.classList.add("mobile-form-open");
    window.setTimeout(() => $(".checklist-form-panel")?.scrollIntoView({ behavior: "smooth", block: "start" }), 30);
  });

  $("#contactNoteTableBody")?.addEventListener("click", (event) => {
    if (!mobileOnlyViewport()) return;
    if (!event.target.closest("[data-contact-edit]")) return;
    $(".contact-note-form")?.classList.add("mobile-form-open");
    window.setTimeout(() => $(".contact-note-form")?.scrollIntoView({ behavior: "smooth", block: "start" }), 30);
  });

  $("#contactRequestList")?.addEventListener("click", (event) => {
    if (!mobileOnlyViewport()) return;
    if (!event.target.closest("[data-contact-request-edit]")) return;
    $(".contact-request-form")?.classList.add("mobile-form-open");
    window.setTimeout(() => $(".contact-request-form")?.scrollIntoView({ behavior: "smooth", block: "start" }), 30);
  });

  $("#promoList")?.addEventListener("click", () => {
    window.setTimeout(ensureMobilePromotionEditButtons, 0);
  });

  $("#checklistForm")?.addEventListener("submit", () => {
    if (!mobileOnlyViewport()) return;
    window.setTimeout(() => $(".checklist-form-panel")?.classList.remove("mobile-form-open"), 250);
  });

  $("#contactNoteForm")?.addEventListener("submit", () => {
    if (!mobileOnlyViewport()) return;
    window.setTimeout(() => $(".contact-note-form")?.classList.remove("mobile-form-open"), 250);
  });

  $("#contactRequestForm")?.addEventListener("submit", () => {
    if (!mobileOnlyViewport()) return;
    window.setTimeout(() => $(".contact-request-form")?.classList.remove("mobile-form-open"), 250);
  });

  $("#promoForm")?.addEventListener("submit", () => {
    if (!mobileOnlyViewport()) return;
    window.setTimeout(() => $(".promo-stable-form-panel")?.classList.remove("mobile-form-open"), 250);
  });

  setupMobileSettingsAccordions();
}

function enhanceMobileFullAppUi() {
  syncDashboardSummaryMode();
  enhanceMobileDataTables(document);
  setupMobileSettingsAccordions();
  setupOperatingGoalMobilePanel();
  ensureMobilePromotionEditButtons();
  syncMobileAppNav(currentView);
  syncMobileMenuVisibility();
}

function syncOperatingGoalMobilePanel() {
  const panel = $("#operatingGoalPanel");
  const button = $("#operatingGoalMobileToggle");
  if (!panel || !button) return;

  const mobile = mobileOnlyViewport();
  if (!mobile) {
    panel.classList.remove("mobile-collapsed");
    button.hidden = true;
    button.textContent = "운영목표 접기";
    button.setAttribute("aria-expanded", "true");
    return;
  }

  button.hidden = false;
  if (!panel.dataset.mobileCollapseInitialized) {
    panel.classList.add("mobile-collapsed");
    panel.dataset.mobileCollapseInitialized = "1";
  }

  const collapsed = panel.classList.contains("mobile-collapsed");
  button.textContent = collapsed ? "운영목표 펼치기" : "운영목표 접기";
  button.setAttribute("aria-expanded", collapsed ? "false" : "true");
}

function setupOperatingGoalMobilePanel() {
  const button = $("#operatingGoalMobileToggle");
  const panel = $("#operatingGoalPanel");
  if (!button || !panel || button.dataset.bound === "1") {
    syncOperatingGoalMobilePanel();
    return;
  }
  button.dataset.bound = "1";
  button.addEventListener("click", () => {
    if (!mobileOnlyViewport()) return;
    panel.classList.toggle("mobile-collapsed");
    syncOperatingGoalMobilePanel();
  });
  syncOperatingGoalMobilePanel();
}

function syncMobileAppNav(view = currentView) {
  const primaryViews = new Set(["dashboard", "records", "checklist"]);
  $$(".mobile-app-nav-item[data-mobile-view]").forEach((button) => {
    button.classList.toggle("active", button.dataset.mobileView === view);
  });
  const moreButton = $("#mobileMoreBtn");
  if (moreButton) moreButton.classList.toggle("active", !primaryViews.has(view));
}

function openMobileMoreSheet() {
  const sheet = $("#mobileMoreSheet");
  const backdrop = $("#mobileMoreBackdrop");
  const button = $("#mobileMoreBtn");
  if (!sheet || !backdrop) return;
  sheet.classList.add("open");
  sheet.setAttribute("aria-hidden", "false");
  backdrop.hidden = false;
  backdrop.classList.add("open");
  if (button) button.setAttribute("aria-expanded", "true");
}

function closeMobileMoreSheet() {
  const sheet = $("#mobileMoreSheet");
  const backdrop = $("#mobileMoreBackdrop");
  const button = $("#mobileMoreBtn");
  if (sheet) {
    sheet.classList.remove("open");
    sheet.setAttribute("aria-hidden", "true");
  }
  if (backdrop) {
    backdrop.classList.remove("open");
    backdrop.hidden = true;
  }
  if (button) button.setAttribute("aria-expanded", "false");
}

function syncMobileMenuVisibility() {
  $$(".mobile-more-grid [data-mobile-more-view]").forEach((button) => {
    const desktopNav = document.querySelector(`.nav-item[data-view="${button.dataset.mobileMoreView}"]`);
    button.hidden = Boolean(desktopNav?.hidden);
  });
  const checklistButton = document.querySelector('.mobile-app-nav-item[data-mobile-view="checklist"]');
  const checklistNav = document.querySelector('.nav-item[data-view="checklist"]');
  if (checklistButton) checklistButton.hidden = Boolean(checklistNav?.hidden);
}

function setMobileRecordTab(tab = "main") {
  const view = $("#recordsView");
  if (!view) return;
  const next = tab === "membership" ? "membership" : "main";
  view.dataset.mobileRecordTab = next;
  $$(".mobile-record-tab").forEach((button) => {
    const active = button.dataset.mobileRecordTab === next;
    button.classList.toggle("active", active);
    button.setAttribute("aria-selected", String(active));
  });
}

function copySelectOptions(sourceSelector, targetSelector) {
  const source = $(sourceSelector);
  const target = $(targetSelector);
  if (!source || !target) return;
  const currentTargetValue = target.value;
  target.innerHTML = source.innerHTML;
  const desired = source.value || currentTargetValue || "";
  if ([...target.options].some((option) => option.value === desired)) target.value = desired;
}

function syncMobileRecordControls() {
  const search = $("#mobileRecordSearch");
  if (search && document.activeElement !== search) search.value = $("#recordSimpleSearch")?.value || "";

  copySelectOptions("#recordStatusFilter", "#mobileRecordStatusFilter");
  copySelectOptions("#recordManagerFilter", "#mobileRecordManagerFilter");
  copySelectOptions("#recordCategoryFilter", "#mobileRecordCategoryFilter");
  copySelectOptions("#recordSellerFilter", "#mobileRecordSellerFilter");

  const pairs = [
    ["#recordStatusFilter", "#mobileRecordStatusFilter"],
    ["#recordManagerFilter", "#mobileRecordManagerFilter"],
    ["#recordCategoryFilter", "#mobileRecordCategoryFilter"],
    ["#recordSellerFilter", "#mobileRecordSellerFilter"]
  ];
  pairs.forEach(([sourceSelector, targetSelector]) => {
    const source = $(sourceSelector);
    const target = $(targetSelector);
    if (source && target) target.value = source.value || "";
  });

  const mobileStart = $("#mobileRecordStartDate");
  const mobileEnd = $("#mobileRecordEndDate");
  if (mobileStart && document.activeElement !== mobileStart) mobileStart.value = $("#recordStartDateFilter")?.value || "";
  if (mobileEnd && document.activeElement !== mobileEnd) mobileEnd.value = $("#recordEndDateFilter")?.value || "";

  copySelectOptions("#membershipStatusFilter", "#mobileMembershipStatusFilter");
  copySelectOptions("#membershipManagerFilter", "#mobileMembershipManagerFilter");
  copySelectOptions("#membershipContactFilter", "#mobileMembershipContactFilter");

  const membershipPairs = [
    ["#membershipStatusFilter", "#mobileMembershipStatusFilter"],
    ["#membershipManagerFilter", "#mobileMembershipManagerFilter"],
    ["#membershipContactFilter", "#mobileMembershipContactFilter"]
  ];
  membershipPairs.forEach(([sourceSelector, targetSelector]) => {
    const source = $(sourceSelector);
    const target = $(targetSelector);
    if (source && target) target.value = source.value || "";
  });
}

function mobileRecordCardHtml(record, index, total, membership = false) {
  const recordId = record.id || promoRecordKey(record);
  const phone = typeof formatPhoneNumber === "function" ? formatPhoneNumber(record.phone) : compactValue(record.phone, "");
  const phoneHref = String(record.phone || "").replace(/[^0-9+]/g, "");
  const category = compactValue(record.category, membership ? "멤버십" : "-");
  const activityType = membership ? "" : recordActivityType(record);
  const previousNo = compactValue(record.previousCustomer, "");
  const newNo = compactValue(record.customerNo, "");
  const sellerLabel = membership ? "컨텍자" : "실판매자";
  const sellerValue = compactValue(record.seller, "-");
  const sequence = membership ? (record.displaySequence || (membershipRecordDateSort === "asc" ? index + 1 : total - index)) : (recordSequenceSort === "asc" ? index + 1 : total - index);
  const selectedClass = selectedRecordId === recordId ? " selected" : "";

  return `
    <article class="mobile-record-card${selectedClass}" data-mobile-record-id="${escapeHtml(recordId)}">
      <div class="mobile-record-card-top">
        <div class="mobile-record-manager">
          <strong>${escapeHtml(compactValue(record.manager, "매니저 미지정"))}</strong>
          <span>#${formatNumber(sequence)} · ${escapeHtml(compactValue(record.receivedDate, "-"))}</span>
        </div>
        <div class="mobile-record-status">
          <span class="status-pill ${statusClass(record.status)} ${typeof statusColorClass === "function" ? statusColorClass(record.status) : ""}">${escapeHtml(compactValue(record.status, "접수"))}</span>
        </div>
      </div>

      <div class="mobile-record-chip-row">
        <span class="category-chip ${typeof categoryColorClass === "function" ? categoryColorClass(category) : ""}">${escapeHtml(category)}</span>
        ${activityType ? `<span class="activity-type-chip ${activityTypeChipClass(activityType)}">${escapeHtml(activityType)}</span>` : ""}
        <span class="mobile-record-mini-chip">${escapeHtml(formatNumber(record.count))}건</span>
        ${membership && sellerValue !== "-" ? `<span class="mobile-record-mini-chip">${escapeHtml(sellerValue)}</span>` : ""}
      </div>

      <div class="mobile-record-customer">
        <strong>${escapeHtml(compactValue(record.customerName, "고객명 없음"))}</strong>
        ${phone ? (phoneHref ? `<a href="tel:${escapeHtml(phoneHref)}">${escapeHtml(phone)}</a>` : `<span>${escapeHtml(phone)}</span>`) : ""}
      </div>

      <div class="mobile-record-product">${escapeHtml(compactValue(record.product, "제품명 없음"))}</div>

      <div class="mobile-record-date-row">
        <div class="mobile-record-date-box">
          <span>접수일</span>
          <strong>${escapeHtml(compactValue(record.receivedDate, "-"))}</strong>
        </div>
        <div class="mobile-record-date-box">
          <span>설치요청일</span>
          <strong>${escapeHtml(compactValue(record.installDate, "-"))}</strong>
        </div>
      </div>

      <div class="mobile-record-actions">
        <button class="mobile-record-detail-btn" type="button" data-mobile-record-action="detail">상세보기</button>
        <button class="mobile-record-edit-btn" type="button" data-mobile-record-action="edit">수정</button>
      </div>

      <div class="mobile-record-detail">
        <div class="mobile-record-detail-grid">
          <div class="mobile-record-detail-row"><span>기존 고객번호</span><strong>${escapeHtml(previousNo || "-")}</strong></div>
          <div class="mobile-record-detail-row"><span>신규 고객번호</span><strong>${escapeHtml(newNo || "-")}</strong></div>
          <div class="mobile-record-detail-row"><span>${sellerLabel}</span><strong>${escapeHtml(sellerValue)}</strong></div>
          ${!membership ? `<div class="mobile-record-detail-row"><span>구분</span><strong>${escapeHtml(activityType || "")}</strong></div>` : ""}
          <div class="mobile-record-detail-row"><span>기타내용</span><strong>${escapeHtml(compactValue(record.memo, "-"))}</strong></div>
        </div>
      </div>
    </article>`;
}

function renderMobileRecordCards(records) {
  const container = $("#mobileRecordCardList");
  if (!container) return;
  container.innerHTML = records.length
    ? records.map((record, index) => mobileRecordCardHtml(record, index, records.length, false)).join("")
    : `<div class="mobile-record-empty">조건에 맞는 접수 내역이 없습니다.</div>`;
  syncMobileRecordControls();
}

function renderMobileMembershipCards(records) {
  const container = $("#mobileMembershipCardList");
  if (!container) return;
  container.innerHTML = records.length
    ? records.map((record, index) => mobileRecordCardHtml(record, index, records.length, true)).join("")
    : `<div class="mobile-record-empty">멤버십 접수내역이 없습니다.</div>`;
  syncMobileRecordControls();
}

function openMobileRecordForm(record = null) {
  const panel = $(".record-form-panel");
  if (!panel) return;
  if (record) {
    selectedRecordId = record.id || "";
    fillRecordForm(record);
  } else {
    resetRecordForm();
  }
  panel.classList.add("mobile-form-open");
  window.setTimeout(() => panel.scrollIntoView({ behavior: "smooth", block: "start" }), 30);
}

function bindMobileRecordCardActions(containerSelector, membership = false) {
  const container = $(containerSelector);
  if (!container) return;
  container.addEventListener("click", (event) => {
    const card = event.target.closest("[data-mobile-record-id]");
    if (!card) return;
    const actionButton = event.target.closest("[data-mobile-record-action]");
    if (!actionButton) return;

    const recordId = card.dataset.mobileRecordId;
    const record = state.records.find((item) => (item.id || promoRecordKey(item)) === recordId);
    if (!record) return;

    if (actionButton.dataset.mobileRecordAction === "detail") {
      const expanded = card.classList.toggle("expanded");
      actionButton.textContent = expanded ? "접기" : "상세보기";
      return;
    }

    if (actionButton.dataset.mobileRecordAction === "edit") {
      openMobileRecordForm(record);
      $$(".mobile-record-card").forEach((item) => item.classList.remove("selected"));
      card.classList.add("selected");
    }
  });
}

function attachMobileAppEvents() {
  $$(".mobile-app-nav-item[data-mobile-view]").forEach((button) => {
    button.addEventListener("click", () => {
      closeMobileMoreSheet();
      switchView(button.dataset.mobileView);
    });
  });

  $("#mobileMoreBtn")?.addEventListener("click", () => {
    const open = $("#mobileMoreSheet")?.classList.contains("open");
    if (open) closeMobileMoreSheet();
    else openMobileMoreSheet();
  });
  $("#mobileMoreCloseBtn")?.addEventListener("click", closeMobileMoreSheet);
  $("#mobileMoreBackdrop")?.addEventListener("click", closeMobileMoreSheet);
  $$(".mobile-more-grid [data-mobile-more-view]").forEach((button) => {
    button.addEventListener("click", () => {
      closeMobileMoreSheet();
      switchView(button.dataset.mobileMoreView);
    });
  });

  $$(".mobile-record-tab").forEach((button) => {
    button.addEventListener("click", () => setMobileRecordTab(button.dataset.mobileRecordTab));
  });

  $("#mobileNewRecordBtn")?.addEventListener("click", () => {
    const panel = $(".record-form-panel");
    if (panel?.classList.contains("mobile-form-open")) {
      panel.classList.remove("mobile-form-open");
      return;
    }
    openMobileRecordForm(null);
  });

  $("#mobileRecordFilterToggle")?.addEventListener("click", () => {
    const panel = $("#mobileRecordFilterPanel");
    const button = $("#mobileRecordFilterToggle");
    if (!panel || !button) return;
    panel.hidden = !panel.hidden;
    button.setAttribute("aria-expanded", String(!panel.hidden));
    button.textContent = panel.hidden ? "필터" : "닫기";
  });

  const mobileToDesktopPairs = [
    ["#mobileRecordStatusFilter", "#recordStatusFilter"],
    ["#mobileRecordManagerFilter", "#recordManagerFilter"],
    ["#mobileRecordCategoryFilter", "#recordCategoryFilter"],
    ["#mobileRecordSellerFilter", "#recordSellerFilter"],
    ["#mobileRecordStartDate", "#recordStartDateFilter"],
    ["#mobileRecordEndDate", "#recordEndDateFilter"],
    ["#mobileMembershipStatusFilter", "#membershipStatusFilter"],
    ["#mobileMembershipManagerFilter", "#membershipManagerFilter"],
    ["#mobileMembershipContactFilter", "#membershipContactFilter"]
  ];
  mobileToDesktopPairs.forEach(([mobileSelector, desktopSelector]) => {
    $(mobileSelector)?.addEventListener("change", (event) => {
      const target = $(desktopSelector);
      if (!target) return;
      target.value = event.target.value;
      target.dispatchEvent(new Event("change", { bubbles: true }));
    });
  });

  let mobileSearchTimer = null;
  $("#mobileRecordSearch")?.addEventListener("input", (event) => {
    window.clearTimeout(mobileSearchTimer);
    mobileSearchTimer = window.setTimeout(() => {
      const target = $("#recordSimpleSearch");
      if (!target) return;
      target.value = event.target.value;
      renderRecords();
    }, 120);
  });

  $$(".mobile-filter-actions [data-mobile-filter-action]").forEach((button) => {
    button.addEventListener("click", () => {
      const map = {
        goal: "#recordGoalPeriodBtn",
        today: "#recordTodayViewBtn",
        all: "#recordAllViewBtn",
        reset: "#clearRecordFiltersBtn"
      };
      $(map[button.dataset.mobileFilterAction])?.click();
      window.setTimeout(syncMobileRecordControls, 20);
    });
  });

  bindMobileRecordCardActions("#mobileRecordCardList", false);
  bindMobileRecordCardActions("#mobileMembershipCardList", true);
}

function statusCountSummary(records, labels = {}) {
  const total = records.length;
  const done = records.filter((record) => compactValue(record.status) === "완료").length;
  const received = records.filter((record) => compactValue(record.status) === "접수").length;
  const hold = records.filter((record) => compactValue(record.status) === "보류").length;
  const parts = [
    `총 ${formatNumber(total)}건`,
    `완료 ${formatNumber(done)}건`,
    `${labels.received || "진행"} ${formatNumber(received)}건`
  ];
  if (labels.includeHold !== false) parts.push(`보류 ${formatNumber(hold)}건`);
  return parts.join(" / ");
}

function recordSelectionKey(record) {
  return String(record?.id || promoRecordKey(record) || "");
}

function updateBulkRecordDeleteUI(visibleRecords = []) {
  const selectedVisible = visibleRecords.filter((record) => selectedRecordIds.has(recordSelectionKey(record))).length;
  const count = selectedRecordIds.size;
  const button = $("#bulkDeleteRecordsBtn");
  if (button) {
    button.disabled = count === 0;
    button.innerHTML = `선택 삭제 <span id="selectedRecordCount">${count}</span>`;
  }
  const selectAll = $("#selectAllRecords");
  if (selectAll) {
    selectAll.checked = visibleRecords.length > 0 && selectedVisible === visibleRecords.length;
    selectAll.indeterminate = selectedVisible > 0 && selectedVisible < visibleRecords.length;
  }
}

function deleteSelectedRecords() {
  const keys = new Set(selectedRecordIds);
  if (!keys.size) return;
  const targetRecords = (state.records || []).filter((record) => keys.has(recordSelectionKey(record)));
  if (!targetRecords.length) {
    selectedRecordIds.clear();
    updateBulkRecordDeleteUI([]);
    return;
  }
  const ok = window.confirm(`선택한 ${targetRecords.length}건의 접수 내역을 삭제하시겠습니까?\n삭제한 데이터는 되돌릴 수 없습니다.`);
  if (!ok) return;
  state.records = (state.records || []).filter((record) => !keys.has(recordSelectionKey(record)));
  if (selectedRecordId && keys.has(String(selectedRecordId))) selectedRecordId = "";
  selectedRecordIds.clear();
  resetRecordForm();
  saveState(`${targetRecords.length}건의 접수 내역을 삭제했습니다.`);
}

function renderRecords() {
  syncReceiptDateSortButtons();
  const records = visibleRecordsForCurrentFilters();

  $("#recordListCount").textContent = statusCountSummary(records, { received: "진행", includeHold: true });
  $("#recordTableBody").innerHTML = records.length
    ? records.map((record, index) => {
      const previousNo = compactValue(record.previousCustomer, "");
      const newNo = compactValue(record.customerNo, "");
      const phone = typeof formatPhoneNumber === "function" ? formatPhoneNumber(record.phone) : compactValue(record.phone, "");
      const statusKey = compactValue(record.status, "접수");
      const selectedClass = selectedRecordId === record.id ? " selected-record-row" : "";
      const sequence = recordSequenceSort === "asc" ? index + 1 : records.length - index;
      const recordKey = recordSelectionKey(record);
      return `
      <tr class="clickable-row clean-record-row status-${escapeHtml(statusKey)}${record.seller ? " seller-selected-row" : ""}${selectedClass}" data-record-id="${escapeHtml(recordKey)}" title="순번을 클릭하면 상하 이동 버튼이 보입니다.">
        <td class="record-select-col"><input type="checkbox" class="record-select-checkbox" data-record-select="${escapeHtml(recordKey)}" aria-label="접수 내역 선택" ${selectedRecordIds.has(recordKey) ? "checked" : ""}></td>
        <td class="seq-col" data-edit-type="none">
          <div class="seq-cell seq-cell-vertical">
            <button class="row-move-button row-move-up" type="button" data-move="up" title="위로">▲</button>
            <button class="seq-number-button" type="button" title="순번 이동">${formatNumber(sequence)}</button>
            <button class="row-move-button row-move-down" type="button" data-move="down" title="아래로">▼</button>
          </div>
        </td>
        <td class="date-col" data-edit-type="date-pair">
          <strong>${escapeHtml(compactValue(record.receivedDate))}</strong>
          ${record.installDate ? `<br><span>${escapeHtml(record.installDate)}</span>` : ""}
        </td>
        <td class="status-col" data-edit-type="status"><span class="status-pill ${statusClass(record.status)} ${typeof statusColorClass === "function" ? statusColorClass(record.status) : ""}">${escapeHtml(compactValue(record.status))}</span></td>
        <td class="manager-col" data-edit-type="manager"><strong>${escapeHtml(compactValue(record.manager))}</strong></td>
        <td class="category-col" data-edit-type="category"><span class="category-chip ${typeof categoryColorClass === "function" ? categoryColorClass(record.category) : ""}">${escapeHtml(compactValue(record.category))}</span></td>
        <td class="activity-col" data-edit-type="activity-type">${recordActivityType(record) ? `<span class="activity-type-chip ${activityTypeChipClass(recordActivityType(record))}">${escapeHtml(recordActivityType(record))}</span>` : ""}</td>
        <td class="count-col" data-edit-type="count"><strong>${formatNumber(record.count)}</strong></td>
        <td class="customer-no-col" data-edit-type="customer-no-pair">
          ${previousNo ? `<span class="old-no">${escapeHtml(previousNo)}</span>` : `<span class="old-no muted-text">기존 없음</span>`}
          ${newNo ? `<br><strong class="new-no">${escapeHtml(newNo)}</strong>` : ""}
        </td>
        <td class="customer-col" data-edit-type="customer-pair">
          <strong class="customer-name">${escapeHtml(compactValue(record.customerName))}</strong>
          ${phone ? `<br><strong class="phone-text">${escapeHtml(phone)}</strong>` : ""}
        </td>
        <td class="product-col" data-edit-type="product"><strong>${escapeHtml(compactValue(record.product))}</strong></td>
        <td class="seller-col" data-edit-type="seller">${escapeHtml(compactValue(record.seller))}</td>
        <td class="memo-col" data-edit-type="memo">${escapeHtml(compactValue(record.memo, ""))}</td>
      </tr>
    `;
    }).join("")
    : `<tr><td colspan="13" class="empty">조건에 맞는 접수 내역이 없습니다.</td></tr>`;

  renderMobileRecordCards(records);
  updateBulkRecordDeleteUI(records);
}


function renderMembershipRecords() {
  syncReceiptDateSortButtons();
  const tbody = $("#membershipTableBody");
  const countLabel = $("#membershipListCount");
  if (!tbody || !countLabel) return;

  const records = sortMembershipRecordsForList(filteredMembershipRecordsByMonth());

  countLabel.textContent = statusCountSummary(records, { received: "접수", includeHold: false });
  tbody.innerHTML = records.length
    ? records.map((record, index) => {
      const phone = typeof formatPhoneNumber === "function" ? formatPhoneNumber(record.phone) : compactValue(record.phone, "");
      const previousNo = compactValue(record.previousCustomer, "");
      const newNo = compactValue(record.customerNo, "");
      const statusKey = compactValue(record.status, "접수");
      return `
      <tr class="clickable-row membership-row clean-record-row status-${escapeHtml(statusKey)}${record.seller ? " seller-selected-row" : ""}" data-record-id="${escapeHtml(record.id || promoRecordKey(record))}">
        <td class="seq-col" data-edit-type="none">
          <div class="seq-cell seq-cell-vertical">
            <button class="row-move-button row-move-up" type="button" data-move="up" title="위로">▲</button>
            <button class="seq-number-button" type="button" title="순번 이동">${formatNumber(record.displaySequence || records.length - index)}</button>
            <button class="row-move-button row-move-down" type="button" data-move="down" title="아래로">▼</button>
          </div>
        </td>
        <td class="date-col" data-edit-type="date-pair"><strong>${escapeHtml(compactValue(record.receivedDate))}</strong>${record.installDate ? `<br><span>${escapeHtml(record.installDate)}</span>` : ""}</td>
        <td class="status-col" data-edit-type="status"><span class="status-pill ${statusClass(record.status)} ${statusColorClass(record.status)}">${escapeHtml(compactValue(record.status))}</span></td>
        <td class="manager-col" data-edit-type="manager"><strong>${escapeHtml(compactValue(record.manager))}</strong></td>
        <td class="count-col" data-edit-type="count"><strong>${formatNumber(record.count)}</strong></td>
        <td class="customer-no-col" data-edit-type="customer-no-pair">
          ${previousNo ? `<span class="old-no">${escapeHtml(previousNo)}</span>` : `<span class="old-no muted-text">기존 없음</span>`}
          ${newNo ? `<br><strong class="new-no">${escapeHtml(newNo)}</strong>` : ""}
        </td>
        <td class="customer-col" data-edit-type="customer-pair"><strong class="customer-name">${escapeHtml(compactValue(record.customerName))}</strong>${phone ? `<br><strong class="phone-text">${escapeHtml(phone)}</strong>` : ""}</td>
        <td class="product-col" data-edit-type="product"><strong>${escapeHtml(compactValue(record.product))}</strong></td>
        <td class="seller-col" data-edit-type="seller"><span class="contact-chip">${escapeHtml(compactValue(record.seller, "선택없음"))}</span></td>
        <td class="memo-col" data-edit-type="memo">${escapeHtml(compactValue(record.memo, ""))}</td>
      </tr>`;
    }).join("")
    : `<tr><td colspan="10" class="empty">맴버쉽 접수내역이 없습니다.</td></tr>`;

  renderMobileMembershipCards(records);
}



function promotionMonthlyReportRows(month = monthIso()) {
  // 조회월은 월초~월말이 아닌 월별 목표 산정기간을 기준으로 합니다.
  const period = monthPeriod(month);
  const start = period.start;
  const end = period.end;
  const rows = [];

  (state.promotions || []).map(normalizePromotion).forEach((promo) => {
    if (!promo.startDate || !promo.endDate) return;
    if (promo.endDate < start || promo.startDate > end) return;

    promoManagerStats(promo)
      .filter((item) => item.reward && item.count > 0)
      .forEach((item) => {
        rows.push({
          promoName: promo.name || "이름 없음",
          period: `${promo.startDate || "-"} ~ ${promo.endDate || "-"}`,
          type: promo.type === "count" ? "건수형" : (promo.type === "score" ? "점수형" : "제품형"),
          manager: item.manager,
          achieved: promo.type === "score" ? `${formatNumber(item.score)}점` : `${formatNumber(item.count)}건`,
          reward: item.reward?.reward || "-",
          quantity: item.reward?.quantity || 1
        });
      });
  });

  return rows.sort((a, b) => {
    const p = a.promoName.localeCompare(b.promoName, "ko");
    if (p !== 0) return p;
    return a.manager.localeCompare(b.manager, "ko");
  });
}

function promotionMonthlyReportHtml(month = monthIso()) {
  const meta = state.meta || {};
  const rows = promotionMonthlyReportRows(month);
  const periodText = `${month}-01 ~ ${lastDayOfMonth(month)}`;
  const groupedRows = rows.reduce((groups, row) => {
    const key = `${row.promoName}__${row.period}`;
    if (!groups[key]) groups[key] = [];
    groups[key].push(row);
    return groups;
  }, {});
  let seq = 0;
  const bodyRows = Object.values(groupedRows).map((group) => {
    return group.map((row, groupIndex) => {
      seq += 1;
      const rowspan = group.length;
      return `
    <tr>
      <td class="seq">${seq}</td>
      ${groupIndex === 0 ? `<td class="promo-name" rowspan="${rowspan}">${escapeHtml(row.promoName)}</td>` : ""}
      ${groupIndex === 0 ? `<td class="period" rowspan="${rowspan}">${escapeHtml(row.period)}</td>` : ""}
      <td class="manager">${escapeHtml(row.manager)}</td>
      <td class="achieved">${escapeHtml(row.achieved)}</td>
      <td class="reward">${escapeHtml(row.reward)}</td>
      <td class="qty">${formatNumber(row.quantity)}</td>
    </tr>`;
    }).join("");
  }).join("");

  return `<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>월별 프로모션 지급 대상 리포트</title>
<style>
  @page { size: A4 portrait; margin: 12mm; }
  * { box-sizing: border-box; }
  body { margin: 0; font-family: "Malgun Gothic", Arial, sans-serif; color: #10251c; background: #fff; }
  .report { width: 100%; }
  .top { display: flex; justify-content: space-between; align-items: flex-end; border-bottom: 3px solid #1f6b52; padding-bottom: 10px; margin-bottom: 12px; }
  h1 { margin: 0; font-size: 24px; font-weight: 900; letter-spacing: -0.04em; }
  .date { font-size: 13px; font-weight: 800; color: #4e615a; }
  .meta { display: flex; gap: 18px; font-size: 13px; font-weight: 800; margin-bottom: 12px; }
  table { width: 100%; border-collapse: collapse; table-layout: fixed; font-size: 11.5px; }
  th, td { border: 1px solid #d7e0dc; padding: 7px 6px; text-align: center; vertical-align: middle; word-break: keep-all; overflow-wrap: anywhere; }
  th { background: #eaf3ef; font-weight: 900; color: #264239; }
  .seq { width: 34px; }
  th.promo-name { text-align: center; }
  th.period { text-align: center; }
  .promo-name { width: 132px; text-align: center; font-weight: 900; }
  .period { width: 174px; }
  .manager { width: 64px; font-weight: 900; }
  .achieved { width: 78px; font-weight: 900; color: #0755b8; }
  .reward { width: auto; font-weight: 900; text-align: left; }
  .qty { width: 44px; font-weight: 900; }
  .empty { height: 120px; color: #7b8c85; font-weight: 800; }
  .foot { margin-top: 10px; font-size: 11px; color: #66756f; text-align: right; }
</style>
</head>
<body>
<div class="report">
  <div class="top">
    <div>
      <h1>${escapeHtml(meta.branchName || "SK지국")} ${escapeHtml(meta.masterName || "사용자")} ${escapeHtml(meta.masterRole || "담당자")} 월별 프로모션 지급 현황</h1>
    </div>
    <div class="date">${escapeHtml(todayIso())}</div>
  </div>
  <div class="meta">
    <div>조회월: ${escapeHtml(month)}</div>
    <div>조회기간: ${escapeHtml(periodText)}</div>
  </div>

  <table>
    <thead>
      <tr>
        <th class="seq">순번</th>
        <th class="promo-name">프로모션 제목</th>
        <th class="period">프로모션 기간</th>
        <th class="manager">매니저</th>
        <th class="achieved">달성건수</th>
        <th class="reward">지급상품</th>
        <th class="qty">수량</th>
      </tr>
    </thead>
    <tbody>${bodyRows || `<tr><td colspan="7" class="empty">해당 월에 지급 대상 프로모션이 없습니다.</td></tr>`}</tbody>
  </table>
  <div class="foot">프로모션 지급 현황은 현재 저장된 접수내역과 인정/미인정 기준으로 계산됩니다.</div>
</div>
</body>
</html>`;
}

function printPromotionMonthlyReport() {
  const month = currentPromotionMonth();
  const oldFrame = document.getElementById("promotion-monthly-print-frame");
  if (oldFrame) oldFrame.remove();

  const iframe = document.createElement("iframe");
  iframe.id = "promotion-monthly-print-frame";
  iframe.style.position = "fixed";
  iframe.style.right = "0";
  iframe.style.bottom = "0";
  iframe.style.width = "0";
  iframe.style.height = "0";
  iframe.style.border = "0";
  document.body.appendChild(iframe);

  const doc = iframe.contentWindow.document;
  doc.open();
  doc.write(promotionMonthlyReportHtml(month));
  doc.close();

  const cleanup = () => setTimeout(() => {
    const frame = document.getElementById("promotion-monthly-print-frame");
    if (frame) frame.remove();
  }, 1000);

  iframe.onload = () => {
    const win = iframe.contentWindow;
    if (!win) return;
    win.onafterprint = cleanup;
    setTimeout(() => {
      win.focus();
      win.print();
    }, 250);
  };
}



function currentPromotionMonth() {
  const input = $("#promoMonthFilter");
  return input?.value || monthIso();
}

function currentDashboardPromotionPeriod() {
  const month = $("#monthFilter")?.value || monthIso();
  const setting = monthSetting(month);
  return {
    month,
    start: setting.periodStart || `${month}-01`,
    end: setting.periodEnd || lastDayOfMonth(month)
  };
}

function currentPromotionPagePeriod() {
  const month = currentPromotionMonth();
  const setting = monthSetting(month);
  return {
    month,
    start: setting.periodStart || `${month}-01`,
    end: setting.periodEnd || lastDayOfMonth(month)
  };
}

function promotionOverlapsDashboardMonth(promo) {
  const period = currentDashboardPromotionPeriod();
  const start = promo?.startDate || "";
  const end = promo?.endDate || "";
  if (!start && !end) return true;
  const promoStart = start || end;
  const promoEnd = end || start;
  if (!promoStart || !promoEnd) return true;
  return promoStart <= period.end && promoEnd >= period.start;
}

function promotionsForCurrentDashboardMonth() {
  return (state.promotions || []).map(normalizePromotion).filter((promo) => promotionOverlapsDashboardMonth(promo));
}


function renderPromotions() {
  const promoList = $("#promoList");
  if (!promoList) return;

  state.promotions = (state.promotions || []).map(normalizePromotion);
  const pagePeriod = currentPromotionPagePeriod();
  const monthPromotions = (state.promotions || []).filter((promo) => {
    const start = promo?.startDate || "";
    const end = promo?.endDate || "";
    if (!start && !end) return true;
    const promoStart = start || end;
    const promoEnd = end || start;
    return promoStart <= pagePeriod.end && promoEnd >= pagePeriod.start;
  });
  const filtered = monthPromotions.filter((promo) => {
    const status = promotionStatus(promo);
    if (promoListFilter === "active") return status === "진행중" || status === "예정";
    if (promoListFilter === "done") return status === "종료";
    return true;
  });
  const totalCount = monthPromotions.length;
  const activeCount = monthPromotions.filter((promo) => {
    const status = promotionStatus(promo);
    return status === "진행중" || status === "예정";
  }).length;
  const doneCount = monthPromotions.filter((promo) => promotionStatus(promo) === "종료").length;
  const allTab = document.querySelector('[data-promo-filter="all"]');
  const activeTab = document.querySelector('[data-promo-filter="active"]');
  const doneTab = document.querySelector('[data-promo-filter="done"]');
  if (allTab) allTab.textContent = `프로모션 목록(총${formatNumber(totalCount)}개)`;
  if (activeTab) activeTab.textContent = `진행중 프로모션(${formatNumber(activeCount)}개)`;
  if (doneTab) doneTab.textContent = `종료된프로모션(${formatNumber(doneCount)}개)`;

  const visiblePromoIds = new Set(monthPromotions.map((promo) => promo.id));
  const currentPromoId = $("#promoId")?.value || "";
  const activeId = visiblePromoIds.has(currentPromoId) ? currentPromoId : (filtered[0]?.id || monthPromotions[0]?.id || "");
  if ($("#promoId") && !visiblePromoIds.has($("#promoId").value)) $("#promoId").value = activeId;
  promoList.innerHTML = filtered.length
    ? filtered.map((promo) => {
      const normalized = normalizePromotion(promo);
      const stats = promoManagerStats(normalized);
      const participants = stats.filter((item) => item.count > 0 || item.score > 0 || item.pendingCount > 0 || item.reward).length;
      const status = promotionStatus(normalized);
      return `
        <article class="promo-stable-card ${activeId === normalized.id ? "active" : ""}" data-promo-id="${escapeHtml(normalized.id)}">
          <div class="promo-stable-card-top">
            <span class="promo-status-badge ${status === "종료" ? "done" : ""}">${escapeHtml(status)}</span>
            <strong>${escapeHtml(normalized.name || "이름 없음")}</strong>
          </div>
          <div class="promo-stable-card-meta">
            <span><b>기간</b> ${escapeHtml(normalized.startDate || "-")} ~ ${escapeHtml(normalized.endDate || "-")}</span>
            <span><b>유형</b> ${normalized.type === "count" ? "건수형" : (normalized.type === "product" ? "제품형" : "점수형")}</span>
            <span><b>결과</b> ${formatNumber(participants)}명</span>
          </div>
        </article>`;
    }).join("")
    : `<div class="empty">현재 목표월 산정기간에 해당하는 프로모션이 없습니다.</div>`;

  const promoMonth = activePromotion() ? promotionReferenceMonth(activePromotion()) : currentDashboardMonth();
  const managerOptions = [{ value: "", label: "전체 매니저" }, ...teamManagers(promoMonth).map((manager) => ({ value: manager.name, label: manager.name }))];
  const managerScope = $("#promoManagerScopeInput");
  if (managerScope) setOptions(managerScope, managerOptions, managerScope.value);

  renderPromotionDetail();
}

function promotionEmptyResultReason(promo) {
  if (!promo?.name) return "조회할 프로모션을 선택해 주세요.";
  const records = (state.records || []).filter((record) => record?.status !== "취소"
    && inDateRange(record?.receivedDate || "", promo.startDate, promo.endDate));
  if (!records.length) return "프로모션 기간에 등록된 접수내역이 없습니다. 기간과 접수일을 확인해 주세요.";
  const matched = records.filter((record) => promoBaseRecordMatches(record, promo));
  if (!matched.length) return "프로모션 기간 내 접수는 있으나 제품 키워드 조건에 맞는 건이 없습니다.";
  const belonging = matched.filter((record) => recordBelongsToCurrentUserTeam(record, recordGoalMonth(record, promotionReferenceMonth(promo))));
  if (!belonging.length) return "대상 접수는 있으나 현재 지국의 매니저·소속 조건과 일치하지 않습니다.";
  const managerMonth = promotionReferenceMonth(promo);
  const managerNames = new Set(teamManagerNames(managerMonth));
  if (!belonging.some((record) => managerNames.has(promoCreditManagerName(record, managerMonth)))) {
    return "대상 접수는 있으나 해당 목표월의 매니저와 연결되지 않았습니다. 실판매자·담당매니저 및 적용월을 확인해 주세요.";
  }
  if (promo.type === "score" && !(promo.scoreRules || []).length) {
    return "점수형 프로모션의 제품 키워드·점수 조건을 확인해 주세요.";
  }
  return "대상 접수의 인정 상태·매니저 적용월을 확인해 주세요.";
}

function renderPromotionDetail() {
  const promo = activePromotion();

  const keywordBody = $("#promoKeywordRuleBody");
  if (keywordBody) {
    const rules = promoKeywordRules(promo);
    keywordBody.innerHTML = rules.length
      ? rules.map((rule) => `<tr><td>${escapeHtml(rule.keyword)}</td><td>${formatNumber(rule.score)}점</td></tr>`).join("")
      : `<tr><td colspan="2" class="empty">대상 제품 키워드를 입력해주세요.</td></tr>`;
  }

  const rewardBody = $("#promoRewardRuleBody");
  if (rewardBody) {
    const rules = promoRewardRules(promo);
    rewardBody.innerHTML = rules.length
      ? rules.map((rule) => `<tr><td>${formatNumber(rule.threshold)} 이상</td><td>${escapeHtml(rule.reward)}</td><td>${formatNumber(rule.quantity || 1)}</td></tr>`).join("")
      : `<tr><td colspan="3" class="empty">지급 기준을 입력해주세요.</td></tr>`;
  }

  const summary = $("#promoRuleSummary");
  if (summary) summary.textContent = promo.name ? `${promo.startDate || "-"} ~ ${promo.endDate || "-"}` : "선택된 프로모션 없음";

  const resultSummary = $("#promoResultSummary");
  const stats = promo.id ? promoManagerStats(promo) : [];
  const visible = stats.filter((item) => item.count > 0 || item.score > 0 || item.pendingCount > 0 || item.reward);
  const emptyReason = visible.length ? "" : promotionEmptyResultReason(promo);
  if (resultSummary) resultSummary.textContent = visible.length ? `${visible.length}명 집계` : "0명 · 조건 확인";

  const resultHeadRow = document.querySelector(".promo-result-table thead tr");
  if (resultHeadRow) {
    resultHeadRow.innerHTML = promo.type === "count"
      ? "<th>순위</th><th>매니저</th><th>인정 건수</th><th>미설치</th><th>달성 단계</th><th>지급 상품</th><th>수량</th>"
      : "<th>순위</th><th>매니저</th><th>인정 건수</th><th>미설치</th><th>누적 점수</th><th>달성 단계</th><th>지급 상품</th><th>수량</th>";
  }

  const resultBody = $("#promoManagerResultBody");
  if (resultBody) {
    resultBody.innerHTML = visible.length
      ? visible.map((item, index) => promo.type === "count"
        ? `
        <tr class="${item.count === 0 && item.pendingCount > 0 ? "promo-pending-only-row" : ""}">
          <td>${index + 1}</td>
          <td><strong>${escapeHtml(item.manager)}</strong></td>
          <td>${formatNumber(item.count)}건</td>
          <td>${formatNumber(item.pendingCount)}건</td>
          <td>${item.reward ? `<span class="promo-step-badge">${formatNumber(item.reward.threshold)} 이상</span>` : `<span class="muted">미달성</span>`}</td>
          <td><strong>${escapeHtml(item.reward?.reward || "-")}</strong></td>
          <td>${item.reward ? formatNumber(item.reward.quantity || 1) : "-"}</td>
        </tr>`
        : `
        <tr class="${item.count === 0 && item.pendingCount > 0 ? "promo-pending-only-row" : ""}">
          <td>${index + 1}</td>
          <td><strong>${escapeHtml(item.manager)}</strong></td>
          <td>${formatNumber(item.count)}건</td>
          <td>${formatNumber(item.pendingCount)}건</td>
          <td>${formatNumber(item.score)}점</td>
          <td>${item.reward ? `<span class="promo-step-badge">${formatNumber(item.reward.threshold)} 이상</span>` : `<span class="muted">미달성</span>`}</td>
          <td><strong>${escapeHtml(item.reward?.reward || "-")}</strong></td>
          <td>${item.reward ? formatNumber(item.reward.quantity || 1) : "-"}</td>
        </tr>`).join("")
      : `<tr><td colspan="${promo.type === "count" ? 7 : 8}" class="empty">${escapeHtml(emptyReason)}</td></tr>`;
  }

  const detailSelect = $("#promoDetailManagerSelect");
  if (detailSelect) {
    const managers = teamManagerNames(currentPromotionMonth());
    const current = detailSelect.value || "";
    setOptions(detailSelect, [{ value: "", label: "매니저" }, ...managers.map((name) => ({ value: name, label: name }))], current);
  }

  const detailHeadRow = document.querySelector(".promo-stable-detail-grid > div:first-child .mini-table thead tr");
  if (detailHeadRow) {
    detailHeadRow.innerHTML = promo.type === "count"
      ? "<th>일자</th><th>상태</th><th>인정/미인정</th><th>제품명</th><th>건수</th>"
      : "<th>일자</th><th>상태</th><th>인정/미인정</th><th>제품명</th><th>건수</th><th>점수</th>";
  }
  const productHeadRow = document.querySelector(".promo-stable-detail-grid > div:nth-child(2) .mini-table thead tr");
  if (productHeadRow) {
    productHeadRow.innerHTML = promo.type === "count"
      ? "<th>제품명/키워드</th><th>건수</th>"
      : "<th>제품명/키워드</th><th>건수</th><th>소계</th>";
  }

  renderPromotionManagerDetail(promo);
}

function renderPromotionManagerDetail(promo = activePromotion()) {
  promo = normalizePromotion(promo);
  const selectedManager = $("#promoDetailManagerSelect")?.value || "";
  const stats = promoManagerStats(promo);
  const stat = selectedManager
    ? (stats.find((item) => item.manager === selectedManager) || { count: 0, pendingCount: 0, score: 0, reward: null, records: [], pendingRecords: [], allRecords: [] })
    : { count: 0, pendingCount: 0, score: 0, reward: null, records: [], pendingRecords: [], allRecords: [] };

  const countNode = $("#promoDetailCount");
  const pendingNode = $("#promoDetailPending");
  const scoreNode = $("#promoDetailScore");
  const rewardNode = $("#promoDetailReward");

  if (countNode) countNode.textContent = `${formatNumber(stat.count)}건`;
  if (pendingNode) pendingNode.textContent = `${formatNumber(stat.pendingCount || 0)}건`;
  if (scoreNode) scoreNode.textContent = promo.type === "count" ? "-" : `${formatNumber(stat.score)}점`;
  if (rewardNode) rewardNode.textContent = stat.reward ? `${stat.reward.reward} × ${formatNumber(stat.reward.quantity || 1)}` : "-";

  const allDetailRecords = selectedManager
    ? [...(stat.allRecords || [])].sort((a, b) => (b.receivedDate || "").localeCompare(a.receivedDate || ""))
    : [];

  const recordBody = $("#promoDetailRecordBody");
  if (recordBody) {
    const isCountType = promo.type === "count";
    recordBody.innerHTML = allDetailRecords.length
      ? allDetailRecords.map((record) => {
          const acceptedValue = promoRecordApprovalValue(record, promo);
          const accepted = acceptedValue === "yes";
          const score = promoRecordScore(record, promo);
          const statusText = compactValue(record.status);
          const isPending = !isInstalledRecord(record);
          return `<tr class="${isPending ? "pending-row" : ""}">
            <td>${escapeHtml(record.receivedDate || "")}</td>
            <td>${escapeHtml(statusText)}</td>
            <td><select class="promo-record-include-toggle" data-record-id="${escapeHtml(promoRecordKey(record))}">
                <option value="no" ${accepted ? "" : "selected"}>미인정</option>
                <option value="yes" ${accepted ? "selected" : ""}>인정</option>
              </select></td>
            <td class="promo-product-cell">${escapeHtml(compactValue(record.product))}</td>
            <td>${formatNumber(record.count)}</td>
            ${isCountType ? "" : `<td>${formatNumber(score)}</td>`}
          </tr>`;
        }).join("")
      : `<tr><td colspan="${promo.type === "count" ? 5 : 6}" class="empty">${selectedManager ? "선택한 매니저의 대상 접수내역이 없습니다." : "매니저를 선택하면 접수 내역이 표시됩니다."}</td></tr>`;
  }

  const productBody = $("#promoDetailProductBody");
  if (productBody) {
    const groups = new Map();
    stat.allRecords.forEach((record) => {
      const matched = matchedPromoKeyword(record, promo);
      const key = matched?.title || matched?.keyword || (promo.type === "count" ? "전체 건수" : "기타");
      const prev = groups.get(key) || { count: 0, score: 0 };
      prev.count += toNumber(record.count);
      prev.score += promoRecordScore(record, promo);
      groups.set(key, prev);
    });
    const rows = Array.from(groups.entries());
    productBody.innerHTML = rows.length
      ? rows.map(([keyword, item]) => `<tr><td>${escapeHtml(keyword)}</td><td>${formatNumber(item.count)}건</td>${promo.type === "count" ? "" : `<td>${formatNumber(item.score)}점</td>`}</tr>`).join("")
      : `<tr><td colspan="${promo.type === "count" ? 2 : 3}" class="empty">${selectedManager ? "제품별 합산 내역이 없습니다." : "매니저를 선택하면 제품별 합산이 표시됩니다."}</td></tr>`;
  }

  const detailTable = document.querySelector(".promo-stable-detail-grid > div:first-child .mini-table");
  if (detailTable) detailTable.classList.toggle("count-promo-table", promo.type === "count");
  const productTable = document.querySelector(".promo-stable-detail-grid > div:nth-child(2) .mini-table");
  if (productTable) productTable.classList.toggle("count-promo-table", promo.type === "count");
}



function historyEntryForMonth(history, targetMonth, valueKey) {
  const month = normalizeManagerMonth(targetMonth) || monthIso();
  const source = Array.isArray(history) ? history : [];
  const matching = source
    .filter((item) => (!item.startMonth || item.startMonth <= month) && (!item.endMonth || month <= item.endMonth))
    .sort((a, b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0];
  if (matching) return matching;
  const first = source.slice().sort((a, b) => String(a.startMonth || "").localeCompare(String(b.startMonth || "")))[0];
  return first || null;
}

function managerSettingsRowMarkup(rawManager, targetMonth, isNew = false) {
  const manager = normalizeManager(rawManager);
  const status = managerStatusForMonth(manager, targetMonth);
  const team = managerTeamForMonth(manager, targetMonth);
  const teamOptions = [...new Set([...configuredTeamNames(), team])].map((name) => `<option value="${escapeHtml(name)}"${team === name ? " selected" : ""}>${escapeHtml(name)}</option>`).join("");
  return `<div class="manager-row manager-team-row ${status === "inactive" ? "inactive-manager-row" : ""}" data-manager-id="${escapeHtml(manager.id)}" data-is-new="${isNew ? "true" : "false"}" data-display-order="${manager.displayOrder || 0}">
    <div class="manager-line manager-line-primary">
      <div class="manager-order-control"><span class="manager-order-number">${manager.displayOrder || "-"}</span><div><button class="ghost-button small manager-order-button manager-order-up" type="button" title="위로 이동">▲</button><button class="ghost-button small manager-order-button manager-order-down" type="button" title="아래로 이동">▼</button></div></div>
      <label>매니저 이름<input class="manager-name" value="${escapeHtml(manager.name)}" placeholder="매니저 이름"></label>
      ${isNew ? `<label>입사 소속팀<select class="manager-team">${teamOptions}</select></label><label>입사월 (달력월)<input class="manager-effective-month" type="month" value="${escapeHtml(targetMonth)}" required></label><label>입사 상태<select class="manager-status"><option value="active">재직</option><option value="inactive">비활성</option></select></label>` : `<div class="manager-current-assignment"><strong>${escapeHtml(formatMonthLabel(targetMonth))} 기준</strong><span>${escapeHtml(team)} · ${status === "inactive" ? "비활성" : "재직"}</span><small>팀 이동·퇴사·복귀는 아래 이력에서 수정</small></div>`}
    </div>
    <div class="manager-line manager-line-secondary">
      <label>담당지역<input class="manager-areas" value="${escapeHtml((manager.areas || []).join(", "))}" placeholder="예: 온천1동, 명륜동"></label>
      <label>${escapeHtml(formatMonthLabel(targetMonth))} 목표<input class="manager-goal" type="number" min="0" step="0.5" value="${escapeHtml(managerGoalFor(manager.name, targetMonth))}"></label>
      <div class="manager-safe-action">${isNew ? `<button class="ghost-button small cancel-new-manager" type="button">등록취소</button>` : `<div class="manager-row-actions"><button class="ghost-button small edit-manager-row" type="button">수정</button><button class="ghost-button small remove-manager" type="button">등록 삭제</button></div>`}</div>
    </div>
    ${isNew ? `<p class="manager-new-hint">입사월 1일부터 적용됩니다. 이름·입사월·소속팀을 확인한 뒤 ‘매니저 설정 저장’을 누르세요.</p>` : managerTimelineMarkup(manager)}
  </div>`;
}

function normalizeMonthHistory(history, fallbackValue, defaultStartMonth = monthIso()) {
  const source = Array.isArray(history) ? history : [];
  const normalized = source.map((item) => ({
    value: String(item?.value ?? fallbackValue ?? "").trim(),
    startMonth: normalizeManagerMonth(item?.startMonth),
    endMonth: normalizeManagerMonth(item?.endMonth)
  })).filter((item) => item.value);
  if (!normalized.length && fallbackValue) {
    normalized.push({ value: String(fallbackValue).trim(), startMonth: defaultStartMonth, endMonth: "" });
  }
  normalized.sort((a, b) => String(a.startMonth || "").localeCompare(String(b.startMonth || "")));
  return normalized;
}

function teamOperationHistory() {
  if (!state.appMeta) state.appMeta = {};
  const current = String(state.appMeta.teamOperationMode || "").trim();
  const fallback = current === "2" ? "2" : (current === "1" ? "1" : "");
  const history = normalizeMonthHistory(state.appMeta.teamOperationHistory, fallback, monthIso());
  if (!history.length) history.push({ value: "1", startMonth: monthIso(), endMonth: "" });
  return history;
}

function teamOperationMode(month = currentDashboardMonth()) {
  const targetMonth = normalizeManagerMonth(month) || monthIso();
  const history = teamOperationHistory();
  const matching = history
    .filter((item) => (!item.startMonth || item.startMonth <= targetMonth) && (!item.endMonth || targetMonth <= item.endMonth))
    .sort((a, b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0];
  if (matching?.value === "1" || matching?.value === "2") return matching.value;
  const stored = String(state?.appMeta?.teamOperationMode || "").trim();
  if (stored === "1" || stored === "2") return stored;
  return configuredTeamNames().length >= 2 ? "2" : "1";
}

function masterTeamHistory() {
  if (!state.appMeta) state.appMeta = {};
  const existing = Array.isArray(state.appMeta.masterTeamHistory) ? state.appMeta.masterTeamHistory : [];
  if (existing.length) return existing;
  const legacy = normalizeTeamName(state.appMeta.userTeam);
  const masterName = String(state.appMeta.masterName || "").trim();
  const master = masterName ? managerByName(masterName) : null;
  const inferred = legacy || (master ? managerTeamForMonth(master, monthIso()) : "");
  return inferred ? [{ team: inferred, startMonth: monthIso(), endMonth: "" }] : [];
}

function masterTeamForMonth(month = currentDashboardMonth()) {
  const targetMonth = normalizeManagerMonth(month) || monthIso();
  const history = masterTeamHistory();
  const matching = history
    .filter((item) => (!item.startMonth || item.startMonth <= targetMonth) && (!item.endMonth || targetMonth <= item.endMonth))
    .sort((a, b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0];
  if (matching?.team) return normalizeTeamName(matching.team);
  const firstHistory = history.slice().sort((a, b) => String(a.startMonth || "").localeCompare(String(b.startMonth || "")))[0];
  if (firstHistory?.team && (!firstHistory.startMonth || targetMonth < firstHistory.startMonth)) {
    const masterName = String(state?.appMeta?.masterName || "").trim();
    const master = masterName ? managerByName(masterName) : null;
    if (master) {
      const historicalMasterTeam = managerTeamForMonth(master, targetMonth);
      if (historicalMasterTeam) return normalizeTeamName(historicalMasterTeam);
    }
    const legacy = normalizeTeamName(state?.appMeta?.userTeam);
    if (legacy) return legacy;
    return normalizeTeamName(firstHistory.team);
  }
  const legacy = normalizeTeamName(state?.appMeta?.userTeam);
  if (legacy) return legacy;
  const masterName = String(state?.appMeta?.masterName || "").trim();
  const master = masterName ? managerByName(masterName) : null;
  return master ? managerTeamForMonth(master, targetMonth) : defaultTeamName();
}

function setMasterTeamForMonth(team, effectiveMonth) {
  const month = normalizeManagerMonth(effectiveMonth) || monthIso();
  const normalizedTeam = normalizeTeamName(team);
  if (!normalizedTeam) return;
  const history = masterTeamHistory().map((item) => ({ ...item }));
  const currentEntry = historyEntryForMonth(history, monthIso(), "team");

  if (currentEntry && month <= (currentEntry.startMonth || monthIso())) {
    const others = history.filter((item) => item !== currentEntry);
    const prior = others.filter((item) => item.startMonth && item.startMonth < month).sort((a,b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")));
    const future = others.filter((item) => item.startMonth && item.startMonth > month).sort((a,b) => String(a.startMonth || "").localeCompare(String(b.startMonth || "")));
    const nextStart = future[0]?.startMonth || "";
    if (prior[0] && (!prior[0].endMonth || prior[0].endMonth >= month)) prior[0].endMonth = shiftMonth(month, -1);
    state.appMeta.masterTeamHistory = [...prior, { ...currentEntry, team: normalizedTeam, startMonth: month, endMonth: nextStart ? shiftMonth(nextStart, -1) : "" }, ...future];
  } else {
    const prior = history.filter((item) => item.startMonth && item.startMonth < month);
    const future = history.filter((item) => item.startMonth && item.startMonth > month);
    const previous = prior.slice().sort((a,b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0];
    if (previous && (!previous.endMonth || previous.endMonth >= month)) previous.endMonth = shiftMonth(month, -1);
    const nextStart = future.slice().sort((a,b) => String(a.startMonth || "").localeCompare(String(b.startMonth || "")))[0]?.startMonth || "";
    state.appMeta.masterTeamHistory = [...prior, { team: normalizedTeam, startMonth: month, endMonth: nextStart ? shiftMonth(nextStart, -1) : "" }, ...future];
  }
  state.appMeta.userTeam = normalizedTeam;
}

function teamOperationSettingsMonth() {
  return normalizeManagerMonth($("#teamOperationEffectiveMonth")?.value)
    || currentDashboardMonth()
    || monthIso();
}

function setTeamOperationMode(mode, effectiveMonth = "") {
  const normalized = String(mode) === "2" ? "2" : "1";
  if (!state.appMeta) state.appMeta = {};
  const month = normalizeManagerMonth(effectiveMonth) || teamOperationSettingsMonth();
  const history = teamOperationHistory().map((item) => ({ ...item }));
  const currentEntry = historyEntryForMonth(history, monthIso(), "value");

  if (currentEntry && month <= (currentEntry.startMonth || monthIso())) {
    const others = history.filter((item) => item !== currentEntry);
    const prior = others.filter((item) => item.startMonth && item.startMonth < month).sort((a,b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")));
    const future = others.filter((item) => item.startMonth && item.startMonth > month).sort((a,b) => String(a.startMonth || "").localeCompare(String(b.startMonth || "")));
    const nextStart = future[0]?.startMonth || "";
    if (prior[0] && (!prior[0].endMonth || prior[0].endMonth >= month)) prior[0].endMonth = shiftMonth(month, -1);
    state.appMeta.teamOperationHistory = [...prior, { ...currentEntry, value: normalized, startMonth: month, endMonth: nextStart ? shiftMonth(nextStart, -1) : "" }, ...future];
  } else {
    const prior = history.filter((item) => item.startMonth && item.startMonth < month);
    const future = history.filter((item) => item.startMonth && item.startMonth > month);
    const previous = prior.slice().sort((a,b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0];
    if (previous && (!previous.endMonth || previous.endMonth >= month)) previous.endMonth = shiftMonth(month, -1);
    const nextStart = future.slice().sort((a,b) => String(a.startMonth || "").localeCompare(String(b.startMonth || "")))[0]?.startMonth || "";
    state.appMeta.teamOperationHistory = [...prior, { value: normalized, startMonth: month, endMonth: nextStart ? shiftMonth(nextStart, -1) : "" }, ...future];
  }
  state.appMeta.teamOperationMode = normalized;

  const existing = normalizeTeamNames(state?.teamNames, state?.managers || []);
  const isLegacySingle = existing.length <= 1 && (!existing[0] || existing[0] === "원팀" || existing[0] === "1팀");
  if (normalized === "2" && isLegacySingle) state.teamNames = ["1팀", "2팀"];
  if (normalized === "1" && isLegacySingle) state.teamNames = ["1팀"];

  renderTeamOperationSettings();
  renderManagerSettings?.();
  renderSettings();
  invalidateManagerCaches();
  persistState();
  render();
}

function renderTeamOperationSettings() {
  const effectiveInput = $("#teamOperationEffectiveMonth");
  if (effectiveInput && !effectiveInput.value) effectiveInput.value = currentDashboardMonth();
  const mode = teamOperationMode(teamOperationSettingsMonth());
  const single = $("#teamOperationSingleBtn");
  const dual = $("#teamOperationDualBtn");
  [single, dual].forEach((button) => {
    if (!button) return;
    const active = button.dataset.teamOperation === mode;
    button.classList.toggle("primary-button", active);
    button.classList.toggle("ghost-button", !active);
    button.setAttribute("aria-pressed", active ? "true" : "false");
  });
}
function renderSettings() {
  state.appMeta = { ...sampleState.appMeta, ...(state.appMeta || {}) };
  $("#branchNameInput").value = state.appMeta.branchName;
  $("#masterNameInput").value = state.appMeta.masterName;
  $("#masterRoleInput").value = state.appMeta.masterRole;
  const masterTeamInput = $("#masterTeamInput");
  const masterTeamEffectiveMonth = $("#masterTeamEffectiveMonth");
  if (masterTeamInput) {
    const names = configuredTeamNames();
    masterTeamInput.innerHTML = names.map((team) => `<option value="${escapeHtml(team)}">${escapeHtml(team)}</option>`).join("");
    const history = masterTeamHistory();
    const latestTeam = history.slice().sort((a,b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0]?.team;
    const currentTeam = latestTeam || masterTeamForMonth(currentDashboardMonth());
    masterTeamInput.value = names.includes(currentTeam) ? currentTeam : (names[0] || "");
  }
  if (masterTeamEffectiveMonth) {
    const history = masterTeamHistory();
    const latest = history.slice().sort((a,b) => String(b.startMonth || "").localeCompare(String(a.startMonth || "")))[0];
    masterTeamEffectiveMonth.value = latest?.startMonth || currentDashboardMonth();
  }
  const masterTeamWrap = masterTeamInput?.closest("label");
  const masterTeamMonthWrap = masterTeamEffectiveMonth?.closest("label");
  const isDualOperation = teamOperationMode(teamOperationSettingsMonth()) === "2";
  if (masterTeamWrap) masterTeamWrap.hidden = !isDualOperation;
  if (masterTeamMonthWrap) masterTeamMonthWrap.hidden = !isDualOperation;
  if (masterTeamInput) masterTeamInput.disabled = !isDualOperation || !settingsEditMode.user;
  if (masterTeamEffectiveMonth) masterTeamEffectiveMonth.disabled = !isDualOperation || !settingsEditMode.user;
  const menuVisibility = optionalMenuVisibility();
  if ($("#menuVisibilityChecklist")) $("#menuVisibilityChecklist").checked = menuVisibility.checklist;
  if ($("#menuVisibilityAnalytics")) $("#menuVisibilityAnalytics").checked = menuVisibility.analytics;
  if ($("#menuVisibilityEvaluation")) $("#menuVisibilityEvaluation").checked = menuVisibility.evaluation;
  if ($("#menuVisibilityRenewalGuide")) $("#menuVisibilityRenewalGuide").checked = menuVisibility.renewalguide;
  if ($("#menuVisibilityPayroll")) $("#menuVisibilityPayroll").checked = menuVisibility.payroll;
  renderGoalSettingsForMonth($("#goalMonthInput")?.value || $("#monthFilter").value);
  renderCustomDashboardCardSettings();
  renderTeamOperationSettings();
  renderAnalyticsSettings();

  managerSettingsDeletedIds.clear();
  const managerTargetMonth = $("#goalMonthInput")?.value || $("#monthFilter").value || monthIso();
  $("#managerSettings").innerHTML = sortManagersByDisplayOrder(state.managers)
    .map((rawManager) => managerSettingsRowMarkup(rawManager, managerTargetMonth, false))
    .join("");
  refreshManagerOrderNumbers();

  setSettingsSectionEditable("user", settingsEditMode.user);
  setSettingsSectionEditable("manager", settingsEditMode.manager);
  setSettingsSectionEditable("goal", settingsEditMode.goal);
}

function setSettingsSectionEditable(section, editable) {
  const selectorMap = {
    user: "#branchNameInput, #masterNameInput, #masterRoleInput, #masterTeamInput, #masterTeamEffectiveMonth",
    manager: "#managerSettings input, #managerSettings select, #managerSettings button.cancel-new-manager, #managerSettings button.manager-order-button, #managerSettings button.remove-manager, #managerSettings button.history-add, #managerSettings button.history-remove, #cancelManagerSettingsBtn, #addManagerBtn",
    team: "#teamSettingsList input, #teamSettingsList button.remove-team-setting, #addTeamBtn",
    goal: "#goalMonthInput, #accountCountInput, #packageRateInput, #newWeightInput, #newIndexInput, #rentalWeightInput, #rentalIndexInput, #renewalWeightInput, #renewalIndexInput, #periodStartInput, #periodEndInput"
  };
  const saveButtonMap = {
    user: "#saveUserSettingsBtn",
    manager: "#saveManagerSettingsBtn",
    team: "#saveTeamSettingsBtn",
    goal: "#saveGoalSettingsBtn"
  };
  $$(selectorMap[section]).forEach((node) => {
    node.disabled = !editable;
  });
  if (section === "manager") {
    $$("#managerSettings button.edit-manager-row").forEach((node) => {
      node.disabled = false;
    });
  }
  const saveButton = $(saveButtonMap[section]);
  if (saveButton) saveButton.disabled = !editable;
}

function unlockSettingsSection(section) {
  settingsEditMode[section] = true;
  setSettingsSectionEditable(section, true);
}

function lockSettingsSection(section) {
  settingsEditMode[section] = false;
  setSettingsSectionEditable(section, false);
}


function backupFileName() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  const hh = String(now.getHours()).padStart(2, "0");
  const mm = String(now.getMinutes()).padStart(2, "0");
  return `SK_Sales_Manager_Backup_${y}${m}${d}_${hh}${mm}.json`;
}

function exportFullBackup() {
  ensureManagerDataIntegrity(state);
  const backup = {
    backupType: "SK_Sales_Manager_FullBackup",
    appName: "SK_Sales_Manager",
    schemaVersion: STATE_SCHEMA_VERSION,
    exportedAt: new Date().toISOString(),
    version: versionLabelForDisplay(APP_VERSION),
    description: "접수내역, 경영평가 월별 입력값·주력상품 상대평가 예상점수·팀 정책이행 수기건수, 접수일 기준 매니저 귀속, 매니저 고유번호·노출순번·재직상태·팀 이동이력, 월별 목표·수기실적, 운영목표, 실판매자 귀속, 급여계산·미매칭·저장내역 및 제품분석 설정을 포함한 전체 데이터 백업",
    data: state
  };
  const blob = new Blob([JSON.stringify(backup, null, 2)], { type: "application/json;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = backupFileName();
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
  showToast("전체 데이터 백업 파일을 내보냈습니다.");
}

function extractBackupData(payload) {
  if (!payload || typeof payload !== "object") return null;
  if (payload.data && typeof payload.data === "object") return payload.data;
  if (payload.state && typeof payload.state === "object") return payload.state;
  return payload;
}

function looksLikeBackupData(data) {
  if (!data || typeof data !== "object" || Array.isArray(data)) return false;
  // 전체 백업/구버전 원본 state 모두 records 또는 managers 배열을 핵심 구조로 가집니다.
  // appMeta 하나만 있는 임의 JSON을 전체 백업으로 오인하지 않도록 최소 구조를 확인합니다.
  const hasCoreState = Array.isArray(data.records) || Array.isArray(data.managers);
  if (!hasCoreState) return false;
  return Array.isArray(data.records)
    || Array.isArray(data.managers)
    || Array.isArray(data.promotions)
    || Array.isArray(data.checklistItems)
    || Array.isArray(data.contactNotes)
    || Array.isArray(data.contactRequests)
    || Array.isArray(data.payrollRecords)
    || Array.isArray(data.payrollUnmatchedRecords)
    || Array.isArray(data.payrollArchives)
    || Array.isArray(data.todos)
    || Boolean(data.todosByDate)
    || Boolean(data.appMeta)
    || Boolean(data.monthSettings)
    || Array.isArray(data.dashboardCustomCards)
    || Boolean(data.managerManualStats)
    || Boolean(data.managerManualOrder)
    || Boolean(data.managerMonthlyGoals);
}

async function readBackupFileText(file) {
  if (!file) {
    throw new Error("백업 파일이 선택되지 않았습니다.");
  }

  console.log("[BACKUP IMPORT] selected file", {
    name: file.name,
    size: file.size,
    type: file.type
  });

  // 1차: 최신 브라우저 기본 API
  if (typeof file.text === "function") {
    try {
      const text = await file.text();
      if (text) return text;
    } catch (error) {
      console.warn("[BACKUP IMPORT] file.text() failed, fallback to arrayBuffer()", error);
    }
  }

  // 2차: ArrayBuffer + TextDecoder 폴백
  if (typeof file.arrayBuffer === "function") {
    const buffer = await file.arrayBuffer();
    return new TextDecoder("utf-8").decode(buffer);
  }

  throw new Error("이 브라우저에서는 선택한 백업 파일을 읽을 수 없습니다.");
}


function confirmBackupRestoreInApp(recordCount, managerCount) {
  return new Promise((resolve) => {
    const old = document.getElementById("backupRestoreConfirmOverlay");
    if (old) old.remove();

    const overlay = document.createElement("div");
    overlay.id = "backupRestoreConfirmOverlay";
    overlay.style.cssText = [
      "position:fixed",
      "inset:0",
      "z-index:30000",
      "display:flex",
      "align-items:center",
      "justify-content:center",
      "padding:24px",
      "background:rgba(15,23,42,.42)",
      "backdrop-filter:blur(2px)"
    ].join(";");

    const box = document.createElement("div");
    box.style.cssText = [
      "width:min(460px,calc(100vw - 48px))",
      "background:#fff",
      "border-radius:18px",
      "box-shadow:0 22px 70px rgba(15,23,42,.28)",
      "padding:26px",
      "font-family:inherit",
      "color:#172033"
    ].join(";");

    box.innerHTML = `
      <div style="font-size:20px;font-weight:800;margin-bottom:10px;">전체 백업 복원</div>
      <div style="font-size:14px;line-height:1.65;color:#526071;margin-bottom:18px;">
        선택한 백업 파일을 정상적으로 확인했습니다.<br>
        현재 웹 프로그램의 모든 데이터가 백업 내용으로 교체됩니다.
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:20px;">
        <div style="background:#f4f7fb;border:1px solid #e4eaf2;border-radius:12px;padding:14px;text-align:center;">
          <div style="font-size:12px;color:#738094;margin-bottom:4px;">접수내역</div>
          <strong style="font-size:22px;">${Number(recordCount || 0).toLocaleString()}건</strong>
        </div>
        <div style="background:#f4f7fb;border:1px solid #e4eaf2;border-radius:12px;padding:14px;text-align:center;">
          <div style="font-size:12px;color:#738094;margin-bottom:4px;">매니저</div>
          <strong style="font-size:22px;">${Number(managerCount || 0).toLocaleString()}명</strong>
        </div>
      </div>
      <div style="font-size:13px;line-height:1.55;color:#b42318;background:#fff4f2;border:1px solid #ffd6d2;border-radius:10px;padding:11px 12px;margin-bottom:18px;">
        복원하면 현재 브라우저에 저장된 데이터가 백업 내용으로 교체됩니다.
      </div>
      <div style="display:flex;gap:10px;justify-content:flex-end;">
        <button type="button" data-action="cancel"
          style="border:1px solid #d7dde7;background:#fff;color:#344054;border-radius:10px;padding:10px 18px;font-weight:700;cursor:pointer;">취소</button>
        <button type="button" data-action="restore"
          style="border:0;background:#174c3b;color:#fff;border-radius:10px;padding:10px 20px;font-weight:800;cursor:pointer;">복원하기</button>
      </div>
    `;

    overlay.appendChild(box);
    document.body.appendChild(overlay);

    const finish = (result) => {
      overlay.remove();
      resolve(result);
    };

    box.querySelector('[data-action="cancel"]')?.addEventListener("click", () => finish(false), { once: true });
    box.querySelector('[data-action="restore"]')?.addEventListener("click", () => finish(true), { once: true });
    overlay.addEventListener("click", (event) => {
      if (event.target === overlay) finish(false);
    }, { once: true });

    const onKey = (event) => {
      if (event.key === "Escape") {
        document.removeEventListener("keydown", onKey);
        finish(false);
      }
    };
    document.addEventListener("keydown", onKey);
  });
}

async function importFullBackupFile(file) {
  if (!file) return false;

  showToast("백업 파일을 확인하고 있습니다...");

  let parsed;
  try {
    const text = await readBackupFileText(file);
    showToast(`백업 파일 읽기 완료 · ${Math.round((file.size || 0) / 1024)}KB · JSON 확인 중...`);
    parsed = JSON.parse(text);
    showToast("백업 JSON 확인 완료 · 데이터 구조를 검사하고 있습니다...");
  } catch (error) {
    console.error("[BACKUP IMPORT] read/parse failed", error);
    showToast("백업 파일을 읽지 못했습니다.");
    window.alert("백업 파일을 읽지 못했습니다.\n현재 버전에서 내보낸 JSON 전체 백업 파일인지 확인해주세요.");
    return false;
  }

  const data = extractBackupData(parsed);
  if (!looksLikeBackupData(data)) {
    console.error("[BACKUP IMPORT] invalid backup format", parsed);
    showToast("SK Sales 전체 백업 파일 형식이 아닙니다.");
    window.alert("전체 백업 파일 형식이 아닙니다.\n'전체 백업 내보내기'로 생성한 JSON 파일을 선택해주세요.");
    return false;
  }

  const recordCount = Array.isArray(data.records) ? data.records.length : 0;
  const managerCount = Array.isArray(data.managers) ? data.managers.length : 0;

  showToast(`백업 파일 확인 완료 · 접수내역 ${recordCount}건 · 매니저 ${managerCount}명`);
  const ok = await confirmBackupRestoreInApp(recordCount, managerCount);
  if (!ok) {
    showToast("전체 백업 복원을 취소했습니다.");
    return false;
  }

  try {
    showToast("현재 데이터를 복원 전 안전백업으로 저장하고 있습니다...");
    const importSafetyBackup = await createDbBackup("before-import", { silent: true });
    if (stateDataCount(state) > 0 && !importSafetyBackup) {
      const continueWithoutSafety = window.confirm("복원 전 안전백업 생성에 실패했습니다. 그래도 전체 백업 복원을 계속할까요?\n\n가능하면 취소한 뒤 '전체 백업 내보내기'로 현재 데이터를 먼저 보관해 주세요.");
      if (!continueWithoutSafety) {
        showToast("현재 데이터 보호를 위해 전체 백업 복원을 취소했습니다.");
        return false;
      }
    }
    showToast("백업 데이터 적용 중...");
    const currentPersistRevision = Number(state?.appMeta?.persistRevision || 0);
    state = normalizeState(data);
    state.appMeta = state.appMeta && typeof state.appMeta === "object" ? state.appMeta : {};
    const restoredPersistRevision = Number(state.appMeta.persistRevision || 0);
    state.appMeta.persistRevision = Math.max(
      Number.isFinite(currentPersistRevision) ? currentPersistRevision : 0,
      Number.isFinite(restoredPersistRevision) ? restoredPersistRevision : 0
    );
    invalidateManagerCaches();
    const restoredRecordCount = Array.isArray(state.records) ? state.records.length : 0;
    showToast(`백업 데이터 적용 완료 · 접수내역 ${restoredRecordCount}건`);

    const restoreSaveResult = await persistState({ ensureManagers: true, immediateServer: true, allowEmptyServer: true });
    if (restoreSaveResult?.ok === false) throw new Error("복원 데이터 저장에 실패했습니다.");
    showToast("전체 백업 복원 완료 · 안전하게 저장했습니다.");

    selectedRecordId = "";
    selectedChecklistId = "";
    selectedContactNoteId = "";
    selectedContactRequestId = "";
    recordSequenceSort = "asc";

    renderNow();
    window.alert(`전체 백업 복원이 완료되었습니다.\n\n접수내역 ${restoredRecordCount}건이 현재 브라우저에 저장되었습니다.`);
    return true;
  } catch (error) {
    console.error("[BACKUP IMPORT] restore/save failed", error);
    showToast("백업 복원 또는 현재 데이터 저장 중 오류가 발생했습니다.");
    window.alert("백업 파일은 읽었지만 복원 또는 현재 데이터 저장 중 오류가 발생했습니다.\n\n백업 파일 형식과 브라우저 저장 상태를 확인해주세요.");
    return false;
  }
}

window.SK_IMPORT_FULL_BACKUP_FROM_INPUT = async function(input) {
  try {
    const file = input?.files?.[0];
    if (!file) return;
    await importFullBackupFile(file);
  } catch (error) {
    console.error("[BACKUP IMPORT] input handler failed", error);
    window.alert("백업 불러오기 처리 중 오류가 발생했습니다.\nF12 콘솔의 [BACKUP IMPORT] 내용을 확인해주세요.");
  } finally {
    if (input) input.value = "";
  }
};



function installTodayStorageKey() {
  return `SK_Sales_skip_install_today_${todayIso()}`;
}

function isoDateOffset(baseIso, offsetDays) {
  const d = new Date(`${baseIso}T00:00:00`);
  if (Number.isNaN(d.getTime())) return "";
  d.setDate(d.getDate() + offsetDays);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function yesterdayIso() {
  return isoDateOffset(todayIso(), -1);
}

function installRecordsByDate(dateIso) {
  return state.records
    .filter((record) => !isMembershipRecord(record))
    .filter((record) => record.installDate === dateIso)
    .sort((a, b) => {
      const managerCompare = compactValue(a.manager, "").localeCompare(compactValue(b.manager, ""), "ko");
      if (managerCompare !== 0) return managerCompare;
      return compactValue(a.customerName, "").localeCompare(compactValue(b.customerName, ""), "ko");
    });
}

function todayInstallRecords() {
  return installRecordsByDate(todayIso());
}

function yesterdayInstallRecords() {
  return installRecordsByDate(yesterdayIso());
}

function closeInstallTodayModal() {
  const skip = $("#skipInstallTodayOnce")?.checked;
  if (skip) localStorage.setItem(installTodayStorageKey(), "1");
  const modal = $("#installTodayModal");
  if (modal) modal.hidden = true;
}

function renderInstallAlertRows(records, includeCompleteAction = false) {
  return records.slice(0, 20).map((record) => {
    const phone = typeof formatPhoneNumber === "function" ? formatPhoneNumber(record.phone) : compactValue(record.phone, "");
    const done = compactValue(record.status, "") === "완료";
    return `
      <tr>
        <td>${escapeHtml(compactValue(record.manager))}</td>
        <td><strong>${escapeHtml(compactValue(record.customerName))}</strong>${phone ? `<br><span>${escapeHtml(phone)}</span>` : ""}</td>
        <td>${escapeHtml(compactValue(record.product))}</td>
        <td>${formatNumber(record.count)}</td>
        <td>${escapeHtml(compactValue(record.status))}</td>
        ${includeCompleteAction ? `<td>${done ? '<span class="install-done-label">완료</span>' : `<button class="ghost-button small install-complete-btn" type="button" data-install-complete="${escapeHtml(record.id)}">설치완료</button>`}</td>` : ""}
      </tr>`;
  }).join("");
}

function renderInstallAlertCards(records, includeCompleteAction = false) {
  return records.slice(0, 20).map((record) => {
    const phone = typeof formatPhoneNumber === "function" ? formatPhoneNumber(record.phone) : compactValue(record.phone, "");
    const done = compactValue(record.status, "") === "완료";
    const statusText = compactValue(record.status, "") || "-";
    return `
      <article class="install-mobile-card">
        <div class="install-mobile-card-head">
          <strong>${escapeHtml(compactValue(record.customerName) || "고객명 미입력")}</strong>
          <span class="install-mobile-status ${done ? "done" : "pending"}">${escapeHtml(statusText)}</span>
        </div>
        <div class="install-mobile-meta">
          <span>매니저</span><strong>${escapeHtml(compactValue(record.manager) || "-")}</strong>
          <span>연락처</span><strong>${phone ? escapeHtml(phone) : "-"}</strong>
          <span>제품명</span><strong>${escapeHtml(compactValue(record.product) || "-")}</strong>
          <span>건수</span><strong>${formatNumber(record.count)}</strong>
        </div>
        ${includeCompleteAction ? `<div class="install-mobile-card-actions">${done ? '<span class="install-done-label">완료 처리됨</span>' : `<button class="ghost-button small install-complete-btn" type="button" data-install-complete="${escapeHtml(record.id)}">설치완료</button>`}</div>` : ""}
      </article>`;
  }).join("");
}

function renderInstallTodayModalContent() {
  const todayRecords = todayInstallRecords();
  const yesterdayRecords = yesterdayInstallRecords();
  const todayCount = todayRecords.length;
  const yesterdayCount = yesterdayRecords.length;

  const todaySummary = $("#installTodaySummary");
  if (todaySummary) todaySummary.textContent = `오늘 ${todayIso()} 설치요청 ${todayCount}건 · 어제 ${yesterdayIso()} 설치요청 ${yesterdayCount}건`;

  const todayBody = $("#installTodayTableBody");
  if (todayBody) {
    todayBody.innerHTML = todayCount
      ? renderInstallAlertRows(todayRecords, false)
      : `<tr><td colspan="5" class="install-empty-row">오늘 설치요청 건이 없습니다.</td></tr>`;
  }

  const yesterdayBody = $("#installYesterdayTableBody");
  if (yesterdayBody) {
    yesterdayBody.innerHTML = yesterdayCount
      ? renderInstallAlertRows(yesterdayRecords, true)
      : `<tr><td colspan="6" class="install-empty-row">어제 설치요청 건이 없습니다.</td></tr>`;
  }

  const todayCardList = $("#installTodayCardList");
  if (todayCardList) {
    todayCardList.innerHTML = todayCount
      ? renderInstallAlertCards(todayRecords, false)
      : `<div class="install-mobile-empty">오늘 설치요청 건이 없습니다.</div>`;
  }

  const yesterdayCardList = $("#installYesterdayCardList");
  if (yesterdayCardList) {
    yesterdayCardList.innerHTML = yesterdayCount
      ? renderInstallAlertCards(yesterdayRecords, true)
      : `<div class="install-mobile-empty">어제 설치요청 건이 없습니다.</div>`;
  }

  const todayTitle = $("#installTodaySectionTitle");
  if (todayTitle) todayTitle.textContent = `오늘 설치요청 (${todayCount}건)`;
  const yesterdayTitle = $("#installYesterdaySectionTitle");
  if (yesterdayTitle) yesterdayTitle.textContent = `어제 설치요청 (${yesterdayCount}건)`;
}


function openInstallTodayModalIfNeeded() {
  const modal = $("#installTodayModal");
  if (!modal) return;
  if (localStorage.getItem(installTodayStorageKey()) === "1") return;

  const todayRecords = todayInstallRecords();
  const yesterdayRecords = yesterdayInstallRecords();
  if (!todayRecords.length && !yesterdayRecords.length) return;

  renderInstallTodayModalContent();
  modal.hidden = false;
}

function applyTodayInstallFilter() {
  const today = todayIso();
  const dateBasis = $("#recordDateBasisFilter");
  const month = $("#recordMonthFilter");
  const start = $("#recordStartDateFilter");
  const end = $("#recordEndDateFilter");

  if (dateBasis) dateBasis.value = "installDate";
  if (month) month.value = "";
  if (start) start.value = today;
  if (end) end.value = today;

  closeInstallTodayModal();
  switchView("records");
  renderRecords();
  showToast("오늘 설치요청건만 표시합니다.");
}

function applyYesterdayInstallFilter() {
  const yesterday = yesterdayIso();
  const dateBasis = $("#recordDateBasisFilter");
  const month = $("#recordMonthFilter");
  const start = $("#recordStartDateFilter");
  const end = $("#recordEndDateFilter");

  if (dateBasis) dateBasis.value = "installDate";
  if (month) month.value = "";
  if (start) start.value = yesterday;
  if (end) end.value = yesterday;

  closeInstallTodayModal();
  switchView("records");
  renderRecords();
  showToast("어제 설치건만 표시합니다.");
}

function completeInstallFromAlert(recordId) {
  const record = state.records.find((item) => item.id === recordId);
  if (!record) return;
  if (record.status === "완료") return;
  record.status = "완료";
  record.updatedAt = new Date().toISOString();
  persistState({ immediateServer: true });
  renderNow();
  renderInstallTodayModalContent();
  showToast(`${compactValue(record.customerName, "설치건")}을 완료 처리했습니다.`);
}


function renderSidebarClock() {
  const clock = $("#sidebarClock");
  const dateNode = $("#sidebarClockDate");
  if (!clock && !dateNode) return;

  const now = new Date();
  const hours = String(now.getHours()).padStart(2, "0");
  const minutes = String(now.getMinutes()).padStart(2, "0");
  const seconds = String(now.getSeconds()).padStart(2, "0");
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  const weekdays = ["일", "월", "화", "수", "목", "금", "토"];
  const weekday = weekdays[now.getDay()];

  if (clock) {
    clock.textContent = `${hours}:${minutes}:${seconds}`;
  }

  if (dateNode) {
    dateNode.textContent = `${year}.${month}.${day} (${weekday})`;
  }
}

function startSidebarClock() {
  renderSidebarClock();
  window.setInterval(renderSidebarClock, 1000);
}


function switchView(view) {
  currentView = view;
  $$(".nav-item").forEach((item) => item.classList.toggle("active", item.dataset.view === view));
  $$(".view").forEach((section) => section.classList.remove("active"));
  $(`#${view}View`)?.classList.add("active");
  document.body.dataset.view = view;
  syncMobileAppNav(view);
  closeMobileMoreSheet();
  closeMobileEntryForms(view);
  renderNow();
}

function collectUserSettings() {
  const previousMeta = state.appMeta || {};
  state.appMeta = {
    ...previousMeta,
    branchName: $("#branchNameInput").value.trim(),
    masterName: $("#masterNameInput").value.trim(),
    masterRole: $("#masterRoleInput").value.trim() || "마스터"
  };
  const mode = teamOperationMode(teamOperationSettingsMonth());
  const teamInput = $("#masterTeamInput");
  const effectiveInput = $("#masterTeamEffectiveMonth");
  if (mode === "2" && teamInput) {
    const team = normalizeTeamName(teamInput.value);
    if (team) setMasterTeamForMonth(team, effectiveInput?.value || currentDashboardMonth());
  } else if (mode === "1") {
    state.appMeta.userTeam = "";
  }
}

function saveMenuVisibilitySettings() {
  state.menuVisibility = normalizeMenuVisibility({
    checklist: Boolean($("#menuVisibilityChecklist")?.checked),
    analytics: Boolean($("#menuVisibilityAnalytics")?.checked),
    evaluation: Boolean($("#menuVisibilityEvaluation")?.checked),
    renewalguide: Boolean($("#menuVisibilityRenewalGuide")?.checked),
    payroll: Boolean($("#menuVisibilityPayroll")?.checked),
    contactnote: false,
    contactrequest: false
  });
  persistState();
  applyOptionalMenuVisibility();
  const hiddenCurrent = ["checklist", "analytics", "evaluation", "renewalguide", "payroll"].includes(currentView) && !state.menuVisibility[currentView];
  if (hiddenCurrent) switchView("dashboard");
  showToast("메뉴 노출 설정을 저장했습니다.");
}


function collectGoalSettings() {
  syncGoalMonthFromPeriodEnd();
  const targetMonth = goalSettingsMonth();
  const setting = monthSetting(targetMonth);
  setting.accountCount = toNumber($("#accountCountInput").value);
  const packageRateInput = $("#packageRateInput");
  setting.packageRate = packageRateInput ? toNumber(packageRateInput.value) : 45;
  setting.newWeight = toNumber($("#newWeightInput").value);
  setting.rentalWeight = toNumber($("#rentalWeightInput").value);
  setting.renewalWeight = toNumber($("#renewalWeightInput").value);
  setting.newIndex = toNumber($("#newIndexInput").value);
  setting.rentalIndex = toNumber($("#rentalIndexInput").value);
  setting.renewalIndex = toNumber($("#renewalIndexInput").value);
  setting.periodStart = $("#periodStartInput").value;
  setting.periodEnd = $("#periodEndInput").value;

  const dashboardMonth = currentDashboardMonth();
  if (targetMonth === dashboardMonth) {
    setDashboardRange(setting.periodStart, dashboardDefaultEnd({ start: setting.periodStart, end: setting.periodEnd }));
  }
  const goalMonthInput = $("#goalMonthInput");
  if (goalMonthInput) goalMonthInput.value = targetMonth;
}

function refreshManagerOrderNumbers() {
  const rows = $$("#managerSettings .manager-row");
  rows.forEach((row, index) => {
    row.dataset.displayOrder = String(index + 1);
    const number = row.querySelector(".manager-order-number");
    if (number) number.textContent = String(index + 1);
    const up = row.querySelector(".manager-order-up");
    const down = row.querySelector(".manager-order-down");
    if (up) up.disabled = !settingsEditMode.manager || index === 0;
    if (down) down.disabled = !settingsEditMode.manager || index === rows.length - 1;
  });
}

function moveManagerSettingsRow(row, direction) {
  if (!row || !settingsEditMode.manager) return;
  const sibling = direction < 0 ? row.previousElementSibling : row.nextElementSibling;
  if (!sibling) return;
  if (direction < 0) row.parentElement.insertBefore(row, sibling);
  else row.parentElement.insertBefore(sibling, row);
  refreshManagerOrderNumbers();
}

function renameManagerBucketKeys(source, renameById) {
  if (!source || typeof source !== "object" || Array.isArray(source)) return;
  Object.values(source).forEach((bucket) => {
    if (!bucket || typeof bucket !== "object" || Array.isArray(bucket)) return;
    const moved = [];
    Object.entries(bucket).forEach(([key, value]) => {
      const replacement = renameById.find((item) => item.previousName === key)?.nextName;
      if (replacement && replacement !== key) moved.push([key, replacement, value]);
    });
    moved.forEach(([previousName, nextName, value]) => {
      if (!Object.prototype.hasOwnProperty.call(bucket, nextName)) bucket[nextName] = value;
      delete bucket[previousName];
    });
  });
}

function renameManagerReferences(renameById = []) {
  const changes = renameById.filter((item) => item?.previousName && item?.nextName && item.previousName !== item.nextName);
  if (!changes.length) return;
  const nameFor = (value, managerId = "") => {
    const byId = changes.find((item) => item.id && item.id === managerId);
    if (byId) return byId.nextName;
    return changes.find((item) => item.previousName === String(value || "").trim())?.nextName || value;
  };

  (state.records || []).forEach((record) => {
    if (!record || typeof record !== "object") return;
    const managerName = nameFor(record.manager, record.managerId);
    if (managerName !== record.manager) {
      record.manager = managerName;
      record.managerNameAtRecord = managerName;
    }
    const sellerName = nameFor(record.seller, record.sellerId);
    if (sellerName !== record.seller) {
      record.seller = sellerName;
      record.sellerNameAtRecord = sellerName;
    }
  });

  renameManagerBucketKeys(state.managerMonthlyGoals, changes);
  renameManagerBucketKeys(state.managerManualStats, changes);
  if (state.managerManualOrder && typeof state.managerManualOrder === "object") {
    const moved = [];
    Object.entries(state.managerManualOrder).forEach(([key, value]) => {
      const nextName = changes.find((item) => item.previousName === key)?.nextName;
      if (nextName && nextName !== key) moved.push([key, nextName, value]);
    });
    moved.forEach(([previousName, nextName, value]) => {
      if (!Object.prototype.hasOwnProperty.call(state.managerManualOrder, nextName)) state.managerManualOrder[nextName] = value;
      delete state.managerManualOrder[previousName];
    });
  }
}

function collectManagerSettings() {
  const targetMonth = $("#goalMonthInput")?.value || $("#monthFilter").value || monthIso();
  const existingManagers = (state.managers || [])
    .map(normalizeManager)
    .filter((manager) => !managerSettingsDeletedIds.has(manager.id));
  const existingById = new Map(existingManagers.map((manager) => [manager.id, manager]));
  const seenNames = new Set();
  const nextManagers = [];
  const renamedManagers = [];
  let invalidMessage = "";

  $$("#managerSettings .manager-row").forEach((row, rowIndex) => {
    if (invalidMessage) return;
    const id = row.dataset.managerId || uid("m");
    const existing = existingById.get(id);
    const name = String(row.querySelector(".manager-name")?.value || "").trim();
    if (!name) {
      if (row.dataset.isNew === "true") return;
      invalidMessage = "기존 매니저의 이름은 비워둘 수 없습니다.";
      return;
    }
    if (seenNames.has(name)) {
      invalidMessage = `매니저명 '${name}'이 중복되어 있습니다.`;
      return;
    }
    seenNames.add(name);

    const effectiveMonth = normalizeManagerMonth(row.querySelector(".manager-effective-month")?.value) || targetMonth;
    const selectedTeam = String(row.querySelector(".manager-team")?.value || "").trim();
    const nextTeam = configuredTeamNames().includes(selectedTeam) ? selectedTeam : defaultTeamName();
    const changeType = String(row.querySelector(".manager-change-type")?.value || "none");
    let nextStatus = row.querySelector(".manager-status")?.value === "inactive" ? "inactive" : "active";
    if (changeType === "leave" || changeType === "pause") nextStatus = "inactive";
    if (changeType === "return") nextStatus = "active";
    const goal = toNumber(row.querySelector(".manager-goal")?.value);
    const areas = String(row.querySelector(".manager-areas")?.value || "")
      .split(",").map((item) => item.trim()).filter(Boolean);
    const nowIso = new Date().toISOString();

    let manager;
    if (existing) {
      let timeline;
      try { timeline = readManagerTimelineDraft(row, existing); }
      catch (error) { invalidMessage = `${name}: ${error.message}`; return; }
      const before = managerHistoryFields(existing);
      const after = managerHistoryFields(timeline);
      const historyEdits = (existing.historyEdits || []).slice();
      if (JSON.stringify(before) !== JSON.stringify(after)) historyEdits.push({at: nowIso, before: structuredClone(before), after: structuredClone(after)});
      manager = normalizeManager({ ...timeline, name, areas, goal: existing.goal,
        displayOrder: rowIndex + 1, historyEdits, updatedAt: nowIso });
      if (existing.name !== name) renamedManagers.push({ id, previousName: existing.name, nextName: name });
    } else {
      if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(row.querySelector(".manager-effective-month")?.value || "")) {
        invalidMessage = `${name}: 입사월을 입력해 주세요.`; return;
      }
      manager = normalizeManager({
        id,
        name,
        team: nextTeam,
        areas,
        goal,
        displayOrder: rowIndex + 1,
        status: nextStatus,
        joinedMonth: effectiveMonth,
        inactiveMonth: nextStatus === "inactive" ? effectiveMonth : "",
        statusHistory: [{ status: nextStatus, startMonth: effectiveMonth, endMonth: "" }],
        teamHistory: [{ team: nextTeam, startMonth: effectiveMonth, endMonth: "" }],
        createdAt: nowIso,
        updatedAt: nowIso
      });
    }

    nextManagers.push({ ...manager, __goalForTargetMonth: goal });
  });

  if (invalidMessage) {
    showToast(invalidMessage);
    return false;
  }

  // 명시적으로 삭제한 행 외에 화면에서 누락된 기존 매니저는 보존합니다.
  existingManagers.forEach((existing) => {
    if (!nextManagers.some((manager) => manager.id === existing.id)) {
      nextManagers.push({ ...existing, displayOrder: nextManagers.length + 1, __goalForTargetMonth: managerGoalFor(existing.name, targetMonth) });
    }
  });

  renameManagerReferences(renamedManagers);
  state.managers = nextManagers
    .map(({ __goalForTargetMonth, ...manager }) => normalizeManager(manager))
    .map((manager, index) => ({ ...manager, displayOrder: index + 1 }));
  nextManagers.forEach((manager) => {
    const saved = state.managers.find((item) => item.id === manager.id);
    if (saved) setManagerGoalFor(saved.name, toNumber(manager.__goalForTargetMonth), targetMonth);
  });
  managerSettingsDeletedIds.clear();
  ensureManagerDataIntegrity(state);
  return true;
}

function resetRecordForm() {
  $("#recordForm").reset();
  $("#recordId").value = "";
  $("#recordFormTitle").textContent = "접수 등록";
  $("#deleteRecordBtn").hidden = true;
  $("#statusInput").value = "접수";
  $("#receivedDateInput").value = todayIso();
  $("#countInput").value = "1";
  renderCommonControls();
  if ($("#activityTypeInput")) $("#activityTypeInput").value = "";
  refreshRecordManagerOptions(goalMonthForDate($("#receivedDateInput").value || "", $("#monthFilter")?.value || monthIso()));
  updateSellerInputOptions("");
}

function fillRecordForm(record) {
  if (currentView !== "records") switchView("records");
  selectedRecordId = record.id;
  $("#recordId").value = record.id;
  $("#statusInput").value = record.status;
  $("#receivedDateInput").value = record.receivedDate;
  $("#installDateInput").value = record.installDate || "";
  refreshRecordManagerOptions(recordGoalMonth(record), record.manager, true);
  $("#managerInput").value = record.manager;
  $("#countInput").value = record.count;
  $("#categoryInput").value = record.category;
  if ($("#activityTypeInput")) {
    const existingActivityType = recordActivityType(record);
    $("#activityTypeInput").value = activityTypes.includes(existingActivityType) ? existingActivityType : "";
  }
  $("#previousCustomerInput").value = record.previousCustomer || "";
  $("#customerInput").value = record.customerNo || "";
  $("#phoneInput").value = formatPhoneNumber(record.phone);
  $("#customerNameInput").value = record.customerName || "";
  $("#qrInput").value = record.qr || "";
  $("#cashAmountInput").value = record.cashAmount || "";
  updateSellerInputOptions(record.seller || "");
  $("#sellerInput").value = record.seller || "";
  $("#productInput").value = record.product || "";
  $("#memoInput").value = record.memo || "";
  $("#recordFormTitle").textContent = "접수 수정";
  $("#deleteRecordBtn").hidden = false;
}


function refreshEditedRecordReferences(record, previous = null) {
  const dateChanged = Boolean(previous && (previous.receivedDate !== record.receivedDate || previous.installDate !== record.installDate));
  if (!previous || previous.manager !== record.manager) {
    const manager = managerByName(record.manager);
    record.managerId = manager?.id || "";
    record.managerNameAtRecord = String(record.manager || "");
    record.managerTeamAtRecord = "";
    record.managerStatusAtRecord = "";
  }
  if (dateChanged || !previous || previous.manager !== record.manager) {
    const manager = managerById(record.managerId) || managerByName(record.manager);
    if (manager) {
      const month = organizationMonthForDate(record.receivedDate || record.installDate);
      record.managerTeamAtRecord = managerTeamForMonth(manager, month);
      record.managerStatusAtRecord = managerStatusForMonth(manager, month);
    }
  }
  if (!previous || previous.seller !== record.seller) {
    record.sellerId = managerByName(record.seller)?.id || "";
    record.sellerNameAtRecord = String(record.seller || "");
  }
  return ensureRecordManagerReference(record);
}

async function updateRecordState(recordId, patch, message = "접수내역을 수정했습니다.") {
  const record = state.records.find((item) => item.id === recordId);
  if (!record) return;
  if (patch.phone !== undefined) patch.phone = formatPhoneNumber(patch.phone);
  const previous = { ...record };
  Object.assign(record, patch);
  refreshEditedRecordReferences(record, previous);
  // 접수일 변경 후 renderRecords()가 표시 목록을 새 날짜 기준으로 정렬합니다.
  record.updatedAt = new Date().toISOString();
  if (patch.category) record.category = normalizeCategory(record.category);
  if (patch.activityType !== undefined) record.activityType = normalizeActivityType(record.activityType);
  selectedRecordId = recordId;
  const saved = await persistState();
  if (!saved.ok) return;
  renderRecords();
  renderMembershipRecords();
  fillRecordForm(record);
  showToast(message);
}

function buildInlineEditor(type, record) {
  const recordMonth = organizationMonthForDate(record.receivedDate || record.installDate);
  const managers = sortManagerNamesByDisplayOrder([...new Set([...activeTeamManagerNames(recordMonth), record.manager].filter(Boolean))]);
  const selectMarkup = (field, current, values, emptyLabel = "") => `<select class="cell-input" data-field="${field}">${values.map((value) => `<option value="${escapeHtml(value)}"${value === current ? " selected" : ""}>${escapeHtml(value || emptyLabel)}</option>`).join("")}</select>`;
  if (type === "date-pair") return `
    <div class="cell-editor-stack">
      <input class="cell-input" data-field="receivedDate" type="date" value="${escapeHtml(record.receivedDate || "")}">
      <input class="cell-input sub" data-field="installDate" type="date" value="${escapeHtml(record.installDate || "")}">
    </div>`;
  if (type === "status") return selectMarkup("status", record.status, statuses);
  if (type === "manager") return selectMarkup("manager", record.manager, managers);
  if (type === "category") return selectMarkup("category", normalizeCategory(record.category), categories);
  if (type === "activity-type") return selectMarkup("activityType", activityTypes.includes(recordActivityType(record)) ? recordActivityType(record) : "", activityTypes, "-");
  if (type === "count") return `<input class="cell-input" data-field="count" type="number" min="0" step="0.5" value="${escapeHtml(record.count ?? 0)}">`;
  if (type === "customer-no-pair") return `
    <div class="cell-editor-stack">
      <input class="cell-input sub" data-field="previousCustomer" value="${escapeHtml(record.previousCustomer || "")}" placeholder="기존 고객번호">
      <input class="cell-input emphasis" data-field="customerNo" value="${escapeHtml(record.customerNo || "")}" placeholder="신규 고객번호">
    </div>`;
  if (type === "customer-pair") return `
    <div class="cell-editor-stack">
      <input class="cell-input emphasis" data-field="customerName" value="${escapeHtml(record.customerName || "")}" placeholder="고객명">
      <input class="cell-input phone" data-field="phone" value="${escapeHtml(record.phone || "")}" placeholder="연락처">
    </div>`;
  if (type === "product") return `<textarea class="cell-input cell-textarea" data-field="product" rows="3">${escapeHtml(record.product || "")}</textarea>`;
  if (type === "seller") return `<select class="cell-input" data-field="seller">${sellerOptionsForCategory(record.category, record.seller).map((value) => `<option value="${escapeHtml(value)}"${value === record.seller ? " selected" : ""}>${escapeHtml(value || "선택")}</option>`).join("")}</select>`;
  if (type === "memo") return `<textarea class="cell-input cell-textarea" data-field="memo" rows="3">${escapeHtml(record.memo || "")}</textarea>`;
  return "";
}

function enterRecordCellEdit(cell) {
  if (!cell || cell.classList.contains("editing-cell")) return;
  const row = cell.closest("[data-record-id]");
  const type = cell.dataset.editType;
  if (!row || !type) return;
  const record = state.records.find((item) => item.id === row.dataset.recordId || promoRecordKey(item) === row.dataset.recordId);
  if (!record) return;
  if (!record.id) record.id = promoRecordKey(record);
  cell.classList.add("editing-cell");
  cell.dataset.originalHtml = cell.innerHTML;
  cell.innerHTML = buildInlineEditor(type, record);
  const first = cell.querySelector("input, select, textarea");
  if (first) {
    first.focus();
    first.select?.();
  }
}

function saveRecordCellEdit(cell, shouldPersist = true) {
  if (!cell || !cell.classList.contains("editing-cell")) return;
  const row = cell.closest("[data-record-id]");
  const type = cell.dataset.editType;
  if (!row || !type) return;
  const record = state.records.find((item) => item.id === row.dataset.recordId || promoRecordKey(item) === row.dataset.recordId);
  if (record && row.dataset.recordId !== record.id) row.dataset.recordId = record.id;
  const patch = {};
  cell.querySelectorAll("[data-field]").forEach((input) => {
    let value = input.value;
    if (input.dataset.field === "count") value = toNumber(value);
    patch[input.dataset.field] = value;
  });
  cell.classList.remove("editing-cell");
  delete cell.dataset.originalHtml;
  if (shouldPersist) updateRecordState(row.dataset.recordId, patch);
}

function cancelRecordCellEdit(cell) {
  if (!cell || !cell.classList.contains("editing-cell")) return;
  cell.classList.remove("editing-cell");
  cell.innerHTML = cell.dataset.originalHtml || cell.innerHTML;
}

function resetPromoForm() {
  $("#promoForm").reset();
  $("#promoId").value = "";
  $("#promoFormTitle").textContent = "프로모션 등록";
  if ($("#promoStartInput")) $("#promoStartInput").value = todayIso();
  if ($("#promoEndInput")) $("#promoEndInput").value = todayIso();
  $("#deletePromoBtn").hidden = true;
  renderCountRuleRows();
  renderScoreRuleRows();
  renderScoreRewardRows();
  renderProductRuleRows();
  setPromoType("count");
  syncPromoDateMode();
}

function fillPromoForm(promo) {
  promo = normalizePromotion(promo);
  switchView("promotions");
  $("#promoId").value = promo.id;
  $("#promoNameInput").value = promo.name;
  $("#promoStartInput").value = promo.startDate;
  $("#promoEndInput").value = promo.endDate;
  renderCountRuleRows(promo.countRules);
  renderScoreRuleRows(promo.scoreRules);
  renderScoreRewardRows(promo.scoreRewardRules);
  renderProductRuleRows(promo.productRules);
  $("#promoMemoInput").value = promo.memo || "";
  $("#promoFormTitle").textContent = "프로모션 등록";
  $("#deletePromoBtn").hidden = false;
  setPromoType(promo.type || "count");
  const singleMode = promo.startDate && promo.startDate === promo.endDate;
  const modeInput = document.querySelector(`input[name='promoDateMode'][value='${singleMode ? "single" : "range"}']`);
  if (modeInput) modeInput.checked = true;
  syncPromoDateMode();
  renderPromotionDetail();
}

let toastHideTimer = null;

function showToast(message) {
  const toast = $("#toast");
  if (!toast) return;

  toast.textContent = persistenceErrorMessage || message;

  // WEB: 오른쪽 하단 대신 화면 중앙에 표시
  toast.style.position = "fixed";
  toast.style.left = "50%";
  toast.style.top = "50%";
  toast.style.right = "auto";
  toast.style.bottom = "auto";
  toast.style.transform = "translate(-50%, -50%)";
  toast.style.zIndex = "32000";
  toast.style.maxWidth = "min(560px, calc(100vw - 40px))";
  toast.style.width = "max-content";
  toast.style.padding = "14px 20px";
  toast.style.borderRadius = "12px";
  toast.style.textAlign = "center";
  toast.style.fontWeight = "700";
  toast.style.lineHeight = "1.45";
  toast.style.boxShadow = "0 14px 45px rgba(15,23,42,.22)";

  toast.classList.add("show");

  window.clearTimeout(toastHideTimer);
  toastHideTimer = window.setTimeout(() => toast.classList.remove("show"), 2600);
}

function downloadFile(filename, mime, content) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

function toCsvValue(value) {
  const text = String(value ?? "");
  return `"${text.replaceAll('"', '""')}"`;
}

function exportCsv() {
  exportExcel();
}

function exportExcel() {
  const header = ["완료여부", "접수날짜", "설치날짜", "매니저", "건수", "전 고객번호", "신규 고객번호", "연락처", "고객명", "판매종류", "구분", "일시불QR", "일시불금액", "제품명", "실판매자", "기타내용"];
  const rows = state.records.map((record) => [
    record.status, record.receivedDate, record.installDate, record.manager, record.count,
    record.previousCustomer, record.customerNo, record.phone, record.customerName,
    record.category, recordActivityType(record), record.qr, record.cashAmount, record.product, record.seller, record.memo
  ]);
  const blob = createXlsxBlob("접수내역", [header, ...rows]);
  downloadBlob(`sales-records-${todayIso()}.xlsx`, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", blob);
  showToast("엑셀 파일로 내보냈습니다.");
}

function downloadBlob(filename, mime, blob) {
  const url = URL.createObjectURL(blob instanceof Blob ? blob : new Blob([blob], { type: mime }));
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

function createXlsxBlob(sheetName, rows) {
  const safeSheetName = String(sheetName || "Sheet1").replace(/[\[\]\*\?\/\\:]/g, " ").slice(0, 31) || "Sheet1";
  const files = {
    "[Content_Types].xml": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/><Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/></Types>`,
    "_rels/.rels": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/></Relationships>`,
    "docProps/core.xml": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><dc:creator>SK Sales Manager</dc:creator><cp:lastModifiedBy>SK Sales Manager</cp:lastModifiedBy><dcterms:created xsi:type="dcterms:W3CDTF">${new Date().toISOString()}</dcterms:created><dcterms:modified xsi:type="dcterms:W3CDTF">${new Date().toISOString()}</dcterms:modified></cp:coreProperties>`,
    "docProps/app.xml": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes"><Application>SK Sales Manager</Application></Properties>`,
    "xl/workbook.xml": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="${escapeXmlAttr(safeSheetName)}" sheetId="1" r:id="rId1"/></sheets></workbook>`,
    "xl/_rels/workbook.xml.rels": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>`,
    "xl/worksheets/sheet1.xml": sheetXml(rows)
  };
  return new Blob([createZipStore(files)], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });
}

function sheetXml(rows) {
  const sheetRows = rows.map((row, rowIndex) => {
    const rowNumber = rowIndex + 1;
    const cells = row.map((value, colIndex) => {
      const ref = `${columnName(colIndex + 1)}${rowNumber}`;
      return `<c r="${ref}" t="inlineStr"><is><t>${escapeXmlText(value)}</t></is></c>`;
    }).join("");
    return `<row r="${rowNumber}">${cells}</row>`;
  }).join("");
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>${sheetRows}</sheetData></worksheet>`;
}

function columnName(index) {
  let name = "";
  while (index > 0) {
    const remainder = (index - 1) % 26;
    name = String.fromCharCode(65 + remainder) + name;
    index = Math.floor((index - 1) / 26);
  }
  return name;
}

function escapeXmlText(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function escapeXmlAttr(value) {
  return escapeXmlText(value).replaceAll('"', "&quot;");
}

function createZipStore(files) {
  const encoder = new TextEncoder();
  const chunks = [];
  const central = [];
  let offset = 0;

  Object.entries(files).forEach(([filename, content]) => {
    const nameBytes = encoder.encode(filename);
    const dataBytes = encoder.encode(content);
    const crc = crc32(dataBytes);
    const localHeader = new Uint8Array(30 + nameBytes.length);
    const localView = new DataView(localHeader.buffer);
    localView.setUint32(0, 0x04034b50, true);
    localView.setUint16(4, 20, true);
    localView.setUint16(6, 0, true);
    localView.setUint16(8, 0, true);
    localView.setUint16(10, 0, true);
    localView.setUint16(12, 0, true);
    localView.setUint32(14, crc, true);
    localView.setUint32(18, dataBytes.length, true);
    localView.setUint32(22, dataBytes.length, true);
    localView.setUint16(26, nameBytes.length, true);
    localView.setUint16(28, 0, true);
    localHeader.set(nameBytes, 30);
    chunks.push(localHeader, dataBytes);

    const centralHeader = new Uint8Array(46 + nameBytes.length);
    const centralView = new DataView(centralHeader.buffer);
    centralView.setUint32(0, 0x02014b50, true);
    centralView.setUint16(4, 20, true);
    centralView.setUint16(6, 20, true);
    centralView.setUint16(8, 0, true);
    centralView.setUint16(10, 0, true);
    centralView.setUint16(12, 0, true);
    centralView.setUint16(14, 0, true);
    centralView.setUint32(16, crc, true);
    centralView.setUint32(20, dataBytes.length, true);
    centralView.setUint32(24, dataBytes.length, true);
    centralView.setUint16(28, nameBytes.length, true);
    centralView.setUint16(30, 0, true);
    centralView.setUint16(32, 0, true);
    centralView.setUint16(34, 0, true);
    centralView.setUint16(36, 0, true);
    centralView.setUint32(38, 0, true);
    centralView.setUint32(42, offset, true);
    centralHeader.set(nameBytes, 46);
    central.push(centralHeader);
    offset += localHeader.length + dataBytes.length;
  });

  const centralOffset = offset;
  central.forEach((item) => { chunks.push(item); offset += item.length; });
  const centralSize = offset - centralOffset;
  const end = new Uint8Array(22);
  const endView = new DataView(end.buffer);
  endView.setUint32(0, 0x06054b50, true);
  endView.setUint16(8, central.length, true);
  endView.setUint16(10, central.length, true);
  endView.setUint32(12, centralSize, true);
  endView.setUint32(16, centralOffset, true);
  chunks.push(end);
  return new Blob(chunks, { type: "application/zip" });
}

function crc32(bytes) {
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.length; i += 1) {
    crc ^= bytes[i];
    for (let bit = 0; bit < 8; bit += 1) {
      crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function exportPromotionSummaryCsv() {
  const promo = activePromotion();
  const stats = promoManagerStats(promo);
  const header = ["프로모션명", "기간", "매니저", "대상건수", "누적점수", "달성단계", "지급상품"];
  const rows = stats.map((item) => [
    promo.name,
    `${promo.startDate} ~ ${promo.endDate}`,
    item.manager,
    item.count,
    item.score,
    item.reward ? `${item.reward.threshold} 이상` : "미달성",
    item.reward?.reward || ""
  ]);
  const csv = [header, ...rows].map((row) => row.map(toCsvValue).join(",")).join("\\n");
  downloadFile(`promotion-summary-${todayIso()}.csv`, "text/csv;charset=utf-8", `\\ufeff${csv}`);
}


function printCurrentManagerPerformance() {
  const table = $("#managerStatsTable");
  if (!table) {
    showToast("출력할 실적현황 표를 찾지 못했습니다.");
    return;
  }

  const actualMode = managerPerformanceMode === "actual";
  const reportTitle = actualMode ? "실제 실적현황" : "매니저별 실적현황";
  const targetPeriod = $("#targetPeriodLabel")?.textContent?.trim() || "";
  const lookupPeriod = $("#periodLabel")?.textContent?.trim() || "";
  const guide = $("#managerPerformanceGuide")?.textContent?.trim() || "";
  const meta = state.appMeta || sampleState.appMeta || {};
  const branchName = String(meta.branchName || "").trim();
  const masterName = String(meta.masterName || "").trim();
  const masterRole = String(meta.masterRole || "담당자").trim() || "마스터";

  const clone = table.cloneNode(true);
  clone.querySelectorAll("input").forEach((input) => {
    const span = document.createElement("span");
    span.textContent = input.value || "";
    input.replaceWith(span);
  });
  clone.querySelectorAll(".manager-share-icon").forEach((button) => button.remove());
  clone.removeAttribute("id");
  clone.classList.add("print-manager-performance-table");

  const oldFrame = document.getElementById("manager-performance-print-frame");
  if (oldFrame) oldFrame.remove();

  const iframe = document.createElement("iframe");
  iframe.id = "manager-performance-print-frame";
  iframe.style.position = "fixed";
  iframe.style.right = "0";
  iframe.style.bottom = "0";
  iframe.style.width = "0";
  iframe.style.height = "0";
  iframe.style.border = "0";
  document.body.appendChild(iframe);

  const doc = iframe.contentWindow.document;
  doc.open();
  doc.write(`<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>${escapeHtml(reportTitle)} 출력</title>
<style>
  @page { size: A4 landscape; margin: 10mm; }
  * { box-sizing: border-box; }
  html, body {
    margin: 0;
    padding: 0;
    background: #fff;
    color: #17221d;
    font-family: "Malgun Gothic", "Apple SD Gothic Neo", Arial, sans-serif;
  }
  body { padding: 0; }
  .report {
    width: 100%;
  }
  .report-head {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    gap: 16px;
    border-bottom: 2px solid #2c6b55;
    padding: 0 0 8px;
    margin-bottom: 10px;
  }
  .report-head h1 {
    margin: 0;
    font-size: 22px;
    color: #173f32;
  }
  .report-head .meta {
    text-align: right;
    font-size: 10px;
    line-height: 1.6;
    color: #4d6259;
  }
  .period-row {
    display: flex;
    justify-content: space-between;
    gap: 20px;
    margin: 0 0 6px;
    font-size: 10px;
    font-weight: 700;
  }
  .guide {
    margin: 0 0 10px;
    color: #5c6d65;
    font-size: 9px;
    line-height: 1.45;
  }
  .print-manager-performance-table {
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
    font-size: ${actualMode ? "9.4px" : "8.8px"};
  }
  .print-manager-performance-table th,
  .print-manager-performance-table td {
    border: 1px solid #d9e3de;
    padding: 6px 4px;
    text-align: center;
    vertical-align: middle;
    word-break: keep-all;
  }
  .print-manager-performance-table th {
    background: #edf4f0;
    font-weight: 900;
  }
  .print-manager-performance-table th:first-child,
  .print-manager-performance-table td:first-child {
    width: 78px;
  }
  .print-manager-performance-table .manager-total-row td {
    border-top: 2px solid #d7a636;
    border-bottom: 2px solid #d7a636;
    font-weight: 900;
  }
  .print-manager-performance-table .business-cell,
  .print-manager-performance-table .final-cell {
    background: #dcefe7 !important;
    color: #0f4f38 !important;
    font-weight: 900;
  }
  ${actualMode ? `
  .print-manager-performance-table th:nth-child(2),
  .print-manager-performance-table th:nth-child(3),
  .print-manager-performance-table th:nth-child(4),
  .print-manager-performance-table th:nth-child(5),
  .print-manager-performance-table th:nth-child(7),
  .print-manager-performance-table td:nth-child(2),
  .print-manager-performance-table td:nth-child(3),
  .print-manager-performance-table td:nth-child(4),
  .print-manager-performance-table td:nth-child(5),
  .print-manager-performance-table td:nth-child(7) { background: #f5f9ff; }
  .print-manager-performance-table th:nth-child(6),
  .print-manager-performance-table th:nth-child(9),
  .print-manager-performance-table td:nth-child(6),
  .print-manager-performance-table td:nth-child(9) { background: #dcefe7; color: #0f4f38; font-weight: 900; }
  .print-manager-performance-table th:nth-child(8),
  .print-manager-performance-table td:nth-child(8) { background: #fff; }
  .print-manager-performance-table th:nth-child(10),
  .print-manager-performance-table td:nth-child(10) { background: #fff0bd; }
  .print-manager-performance-table th:nth-child(11),
  .print-manager-performance-table td:nth-child(11) { background: #eee9fa; }
  ` : `
  .print-manager-performance-table th:nth-child(2),
  .print-manager-performance-table th:nth-child(3),
  .print-manager-performance-table th:nth-child(4),
  .print-manager-performance-table th:nth-child(5),
  .print-manager-performance-table td:nth-child(2),
  .print-manager-performance-table td:nth-child(3),
  .print-manager-performance-table td:nth-child(4),
  .print-manager-performance-table td:nth-child(5) { background: #f5f9ff; }
  .print-manager-performance-table th:nth-child(6),
  .print-manager-performance-table th:nth-child(7),
  .print-manager-performance-table td:nth-child(6),
  .print-manager-performance-table td:nth-child(7) { background: #f4f0fb; color: #56476d; }
  .print-manager-performance-table th:nth-child(8),
  .print-manager-performance-table th:nth-child(11),
  .print-manager-performance-table td:nth-child(8),
  .print-manager-performance-table td:nth-child(11) { background: #dcefe7; color: #0f4f38; font-weight: 900; }
  .print-manager-performance-table th:nth-child(12),
  .print-manager-performance-table td:nth-child(12) { background: #fff0bd; }
  .print-manager-performance-table th:nth-child(13),
  .print-manager-performance-table td:nth-child(13) { background: #eee9fa; }
  `}
  .mini-rate-track {
    width: 52px;
    height: 7px;
    margin: 0 auto 2px;
    border-radius: 999px;
    background: #e7eeea;
    overflow: hidden;
  }
  .mini-rate-track span {
    display: block;
    height: 100%;
    background: linear-gradient(90deg, #2f7b61, #cf9c2f);
  }
  .mini-rate-wrap strong { font-size: 8px; }
  .refund-text, .shortage-text { color: #b12b2b; font-weight: 800; }
  .manager-name-cell strong { white-space: nowrap; }
</style>
</head>
<body>
  <main class="report">
    <header class="report-head">
      <div>
        <h1>${escapeHtml(reportTitle)}</h1>
      </div>
      <div class="meta">
        ${branchName ? `<div>${escapeHtml(branchName)}</div>` : ""}
        ${masterName ? `<div>${escapeHtml(masterName)} ${escapeHtml(masterRole)}</div>` : ""}
        <div>출력일 ${escapeHtml(formatKoreanLongDate())}</div>
      </div>
    </header>
    <div class="period-row">
      <span>목표산정기간 ${escapeHtml(targetPeriod)}</span>
      <span>조회 ${escapeHtml(lookupPeriod)}</span>
    </div>
    <p class="guide">${escapeHtml(guide)}</p>
    ${clone.outerHTML}
  </main>
<script>
  window.addEventListener("load", () => {
    setTimeout(() => {
      window.focus();
      window.print();
    }, 180);
  });
<\/script>
</body>
</html>`);
  doc.close();

  const cleanup = () => setTimeout(() => iframe.remove(), 800);
  iframe.contentWindow.onafterprint = cleanup;
}


async function printDashboard() {
  let blob;
  try {
    blob = await printDashboardImageBlob();
  } catch (error) {
    console.error(error);
    showToast("프린트 이미지 생성이 실패했습니다. 다시 시도해 주세요.");
    return;
  }

  const dataUrl = await new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onloadend = () => resolve(reader.result);
    reader.onerror = reject;
    reader.readAsDataURL(blob);
  });

  const oldFrame = document.getElementById('print-frame');
  if (oldFrame) oldFrame.remove();

  const iframe = document.createElement('iframe');
  iframe.id = 'print-frame';
  iframe.style.position = 'fixed';
  iframe.style.right = '0';
  iframe.style.bottom = '0';
  iframe.style.width = '0';
  iframe.style.height = '0';
  iframe.style.border = '0';
  document.body.appendChild(iframe);

  const doc = iframe.contentWindow.document;
  doc.open();
  doc.write(`<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>영업현황 프린트</title>
<style>
  @page { size: A4 portrait; margin: 0; }
  html, body { margin: 0; padding: 0; background: #fff; }
  body { display: flex; justify-content: center; align-items: flex-start; }
  img { width: 210mm; height: 297mm; object-fit: contain; display: block; }
</style>
</head>
<body>
  <img id="reportImg" src="${dataUrl}" alt="영업현황 보고서">
  <script>
    const img = document.getElementById('reportImg');
    function triggerPrint(){
      setTimeout(() => {
        window.focus();
        window.print();
      }, 250);
    }
    if (img.complete) triggerPrint();
    else img.addEventListener('load', triggerPrint, { once: true });
  <\/script>
</body>
</html>`);
  doc.close();

  const cleanup = () => {
    setTimeout(() => {
      const frame = document.getElementById('print-frame');
      if (frame) frame.remove();
    }, 1000);
  };
  iframe.onload = () => {
    const win = iframe.contentWindow;
    if (win) {
      win.onafterprint = cleanup;
    }
  };
}

function inlineComputedStyles(source, target) {
  const computed = window.getComputedStyle(source);
  const keep = [
    "display", "grid-template-columns", "gap", "align-items", "justify-content",
    "width", "height", "padding", "margin", "border", "border-radius",
    "background", "background-color", "color", "font", "font-size", "font-weight",
    "line-height", "text-align", "box-shadow", "overflow", "white-space", "transform", "transform-origin"
  ];
  keep.forEach((name) => target.style.setProperty(name, computed.getPropertyValue(name)));
  Array.from(source.children).forEach((child, index) => inlineComputedStyles(child, target.children[index]));
}



async function reportImageBlob() {
  const canvas = document.createElement("canvas");
  const ctx = canvas.getContext("2d");
  if (!ctx) throw new Error("이미지 캔버스를 만들 수 없습니다.");

  const month = $("#monthFilter")?.value || monthIso();
  const setting = monthSetting(month);
  const targetPeriod = monthPeriod(month);
  const periodStart = targetPeriod.start || "1900-01-01";
  const periodEnd = targetPeriod.end || "2999-12-31";
  const records = state.records.filter((record) => record && record.status !== "취소" && recordBelongsToCurrentUserTeam(record, month) && inDateRange(record.receivedDate, periodStart, periodEnd));
  const goals = calculatedGoals(month);
  const totals = applyManualStatsToTotals(actuals(records));
  const waterMetrics = waterPurifierEvaluationMetrics(month, records);
  const managers = teamManagers(month);
  const meta = state.appMeta || sampleState.appMeta;
  // 100점 제품은 선택한 목표월에 등록된 프로모션만 사용합니다.
  // 따라서 월별로 제품을 추가·삭제하면 공유 이미지의 열도 자동으로 바뀝니다.
  const promo = hundredPointPromotion(month);
  const promoRules = promo ? promoKeywordRules(promo) : [];
  const selectedConditionCards = dashboardCustomCards()
    .filter((card) => card.enabled && card.value);

  const n = (value) => Number.isFinite(Number(value)) ? Number(value) : 0;
  const fmt = (value, digits = null) => digits == null ? formatNumber(n(value)) : n(value).toFixed(digits);
  const pct = (actual, goal, digits = 0) => {
    const g = n(goal); if (g <= 0) return `${digits ? "0.0" : "0"}%`;
    const rate = n(actual) / g * 100;
    return `${digits ? rate.toFixed(digits) : Math.round(rate)}%`;
  };
  const blank = (value) => n(value) === 0 ? "" : formatNumber(value);
  const now = new Date();
  const weekdays = ["일", "월", "화", "수", "목", "금", "토"];
  const monthLabel = `${Number(month.slice(0,4))}년 ${Number(month.slice(5,7))}월`;
  const branchTitle = `${meta.branchName || "SK지국"} ${meta.masterName || "사용자"} ${meta.masterRole || "담당자"}`;
  const todayLabel = `${now.getMonth()+1}월 ${now.getDate()}일 ${weekdays[now.getDay()]}요일`;
  const compactPeriod = `${String(setting.periodStart || "").replace(/^20/, "").replace(/-/g, ".")} ~ ${String(setting.periodEnd || "").replace(/^20/, "").replace(/-/g, ".")}`;

  const managerRows = managers.map((manager) => {
    const managerRecords = records.filter((record) => record.manager === manager.name);
    const metrics = exactManagerSalesMetrics(managerRecords, manager.name);
    const goal = n(managerGoalFor(manager.name));
    const promoScores = hundredPointPromotionScoresForManager(manager.name, promo, records);
    return {
      manager: manager.name,
      ...metrics,
      goal,
      shortage: metrics.final - goal,
      conditionValues: selectedConditionCards.map((card) => dashboardConditionPhysicalCount(managerRecords, card)),
      promoValues: promoScores.values,
      promoTotal: promoScores.total
    };
  });

  const totalGoal = managerRows.reduce((sum,row)=>sum+n(row.goal),0);
  const totalMetrics = exactManagerSalesMetrics(records);
  const totalConditionValues = selectedConditionCards.map((_, index) =>
    managerRows.reduce((sum, row) => sum + n(row.conditionValues[index]), 0)
  );
  const totalPromoValues = promoRules.map((_,i)=>managerRows.reduce((sum,row)=>sum+n(row.promoValues[i]),0));
  const totalPromoScore = totalPromoValues.reduce((sum,v)=>sum+n(v),0);

  // top summary
  const coreGoal = n(goals.generalGoal);
  const overallGoal = n(goals.overallGoal);
  const coreActual = n(totals.coreActual);
  const overallActual = n(totals.overallActual);
  const shortage = coreActual - coreGoal;
  const summaryHeaders = ["목표", "신규", "패키지", "재탈", "합계", "부족분", "환수", "재약정", "종합달성율", "정수기"];
  const summaryHeaderGoals = ["", goals.newGoal, goals.packageGoal, goals.rentalGoal, coreGoal, "", "", goals.renewalGoal, overallGoal, waterMetrics.goal];
  const summaryActuals = ["실적", totals.newActual, totals.packageCount, totals.rentalActual, coreActual, shortage, totals.refundActual ? -n(totals.refundActual) : "", totals.renewalActual, overallActual, waterMetrics.current];
  const summaryRates = ["달성율", pct(totals.newActual,goals.newGoal), pct(totals.packageCount,goals.packageGoal), pct(totals.rentalActual,goals.rentalGoal), pct(coreActual,coreGoal,1), coreGoal>0?`${Math.round(shortage/coreGoal*100)}%`:"0%", "", pct(totals.renewalActual,goals.renewalGoal), pct(overallActual,overallGoal), pct(waterMetrics.current,waterMetrics.goal,1)];

  const baseManagerHeaders = ["매니저","신규","패키지","재탈","일시불","컨스","지원","영업실적","재약정","최종실적","상시","부족건"];
  const conditionHeaders = selectedConditionCards.map((card) => card.title || "조건");
  const promoHeaders = promoRules.map((rule)=>rule.title || rule.keyword || "항목");
  const managerHeaders = [...baseManagerHeaders, ...conditionHeaders, ...promoHeaders, ...(promoRules.length ? ["합계"] : [])];
  const baseWidths = [92,58,68,58,58,58,58,78,68,82,62,62];
  const conditionWidths = selectedConditionCards.map(()=>62);
  const promoWidths = promoRules.map(()=>62);
  const managerWidths = [...baseWidths, ...conditionWidths, ...promoWidths, ...(promoRules.length?[62]:[])];
  const tableW = managerWidths.reduce((a,b)=>a+b,0);
  const titleH=58, accountH=42, sumHeaderH=66, sumRowH=46, bandH=48, managerHeaderH=54, managerRowH=46;
  const summaryWidths=[76,92,108,92,112,96,92,104,158,150];
  const summaryW=summaryWidths.reduce((a,b)=>a+b,0);
  const canvasW = Math.max(1056, tableW, summaryW);
  const summaryY=titleH+accountH+18;
  const managerBandY=summaryY+sumHeaderH+sumRowH*2+22;
  const managerHeaderY=managerBandY+bandH;
  const canvasH=managerHeaderY+managerHeaderH+managerRowH*(managerRows.length+1)+2;
  canvas.width=canvasW; canvas.height=canvasH;

  const C={white:"#fff",line:"#222",title:"#cfe2f3",peach:"#f6d5b8",blue:"#5b9bd5",blueDark:"#2f75b5",conditionBand:"#4472c4",conditionHeader:"#d9e2f3",conditionCell:"#edf3fa",lightBlue:"#d9eaf7",lightBlue2:"#dce6f1",yellow:"#fff200",gray:"#e7e6e6",promoBand:"#70ad47",green:"#c6e0b4",red:"#f00",black:"#111",water:"#d9ead3"};
  const rect=(x,y,w,h,fill=C.white,stroke=C.line,lw=1)=>{ctx.fillStyle=fill;ctx.fillRect(x,y,w,h);if(stroke){ctx.strokeStyle=stroke;ctx.lineWidth=lw;ctx.strokeRect(x,y,w,h);}};
  const fit=(value,max,base=19,weight=700)=>{let s=base;const t=String(value??"");while(s>9){ctx.font=`${weight} ${s}px "Malgun Gothic",sans-serif`;if(ctx.measureText(t).width<=max)return s;s--;}return s;};
  const text=(value,x,y,size=18,weight=700,color=C.black,align="center")=>{ctx.font=`${weight} ${size}px "Malgun Gothic",sans-serif`;ctx.fillStyle=color;ctx.textAlign=align;ctx.textBaseline="middle";ctx.fillText(String(value??""),x,y);};
  ctx.fillStyle=C.white;ctx.fillRect(0,0,canvasW,canvasH);

  // title
  rect(0,0,Math.min(canvasW,688),titleH,C.title);
  text(monthLabel,95,titleH/2,22,800);
  text(branchTitle,344,titleH/2,fit(branchTitle,380,22,800),800);
  text(todayLabel,610,titleH/2,20,800);

  // account line
  const accountY=titleH+8;
  rect(0,accountY,58,accountH,C.white);rect(58,accountY,60,accountH,C.white);rect(118,accountY,170,accountH,C.white);
  text("계정",29,accountY+accountH/2,16,600);text(fmt(setting.accountCount),88,accountY+accountH/2,16,700);text(compactPeriod,203,accountY+accountH/2,16,700);

  // summary: 목표값을 헤더 안으로 올리고, 실적/달성율 2행만 표시
  let x=0;
  summaryHeaders.forEach((h,i)=>{
    const fill = i===4 || i===8 ? C.yellow : (i===9 ? "#d9ead3" : C.peach);
    rect(x,summaryY,summaryWidths[i],sumHeaderH,fill);
    if (i === 0) {
      text(h,x+summaryWidths[i]/2,summaryY+sumHeaderH/2,18,800);
    } else {
      const goalValue = summaryHeaderGoals[i];
      const hasGoal = goalValue !== "" && goalValue !== null && goalValue !== undefined;
      text(h,x+summaryWidths[i]/2,summaryY+22,fit(h,summaryWidths[i]-8,18,800),800);
      if (hasGoal) {
        text(i === 9 ? `(${Math.round(n(goalValue))})` : `(${fmt(goalValue)})`,x+summaryWidths[i]/2,summaryY+47,fit(i === 9 ? `(${Math.round(n(goalValue))})` : `(${fmt(goalValue)})`,summaryWidths[i]-8,17,800),800,i === 9 ? "#c62828" : C.red);
      }
    }
    x+=summaryWidths[i];
  });
  const summaryRows=[summaryActuals,summaryRates];
  summaryRows.forEach((row,ri)=>{
    const y=summaryY+sumHeaderH+ri*sumRowH;
    let xx=0;
    row.forEach((v,i)=>{
      let fill=C.white;
      if(i===4||i===8) fill=C.yellow;
      if(i===9) fill=ri===0 ? "#eaf4ff" : "#f2fbf1";
      if(i===6&&ri===1) fill="#d0cece";
      rect(xx,y,summaryWidths[i],sumRowH,fill);
      let color=C.black;
      if(i===5&&v!=="") color=C.red;
      if(i===9 && ri===0) color="#1268d8";
      if(i===9 && ri===1) color=waterMetrics.achievementRate >= 100 ? "#118a43" : waterMetrics.achievementRate >= 80 ? "#e7780d" : "#de2f2a";
      text(v,xx+summaryWidths[i]/2,y+sumRowH/2,fit(v,summaryWidths[i]-8,17,700),700,color);
      xx+=summaryWidths[i];
    });
  });

  // manager section bands
  const productStartX=baseWidths.reduce((a,b)=>a+b,0);
  const conditionStartX=productStartX;
  const conditionW=conditionWidths.reduce((sum,width)=>sum+width,0);
  const promoStartX=conditionStartX+conditionW;
  const promoW=promoWidths.reduce((sum,width)=>sum+width,0)+(promoRules.length?62:0);
  rect(0,managerBandY,productStartX,bandH,C.blue);
  text("매니저별 실적현황",productStartX/2,managerBandY+bandH/2,22,800,C.white);
  if(conditionHeaders.length){
    rect(conditionStartX,managerBandY,conditionW,bandH,C.conditionBand);
    text("집중관리 제품",conditionStartX+conditionW/2,managerBandY+bandH/2,18,800,C.white);
  }
  if(promoRules.length){
    rect(promoStartX,managerBandY,promoW,bandH,C.promoBand);
    text("100점을 잡아라!",promoStartX+promoW/2,managerBandY+bandH/2,20,800,C.white);
  }

  // headers
  x=0;
  managerHeaders.forEach((h,i)=>{
    const isConditionColumn=i>=baseWidths.length&&i<baseWidths.length+conditionHeaders.length;
    const isPromotionColumn=i>=baseWidths.length+conditionHeaders.length;
    let fill=i<5?C.lightBlue:(i===7||i===9?C.yellow:(isConditionColumn?C.conditionHeader:(isPromotionColumn?C.green:C.lightBlue2)));
    rect(x,managerHeaderY,managerWidths[i],managerHeaderH,fill);
    text(h,x+managerWidths[i]/2,managerHeaderY+managerHeaderH/2,fit(h,managerWidths[i]-4,15,700),700);
    x+=managerWidths[i];
  });

  const drawRow=(row,ri,isTotal=false)=>{
    const y=managerHeaderY+managerHeaderH+ri*managerRowH;
    const values=[row.manager,blank(row.newCount),blank(row.packageCount),blank(row.rentalCount),blank(row.cashCount),blank(row.consCount),blank(row.supportCount),blank(row.business),blank(row.renewal),blank(row.final),blank(row.goal),row.shortage===0?"":fmt(row.shortage),...(row.conditionValues||[]).map((v)=>n(v)>0?fmt(v):""),...(row.promoValues||[]).map((v)=>n(v)>0?`${fmt(v)}점`:""),...(promoRules.length?[n(row.promoTotal)>0?`${fmt(row.promoTotal)}점`:""]:[])];
    let xx=0;
    values.forEach((v,i)=>{
      let fill=isTotal?C.peach:C.white;
      if(i===5||i===6)fill=isTotal?C.peach:C.lightBlue2;
      if(i===7||i===9)fill=C.yellow;
      const isConditionColumn=i>=baseWidths.length&&i<baseWidths.length+conditionHeaders.length;
      const isPromotionColumn=i>=baseWidths.length+conditionHeaders.length;
      if(isConditionColumn)fill=isTotal?C.conditionHeader:C.conditionCell;
      if(isPromotionColumn)fill=isTotal?C.green:C.gray;
      rect(xx,y,managerWidths[i],managerRowH,fill);
      let color=i===11&&v!==""?C.red:C.black;
      if(isTotal&&(i===9||i===11))color=i===11?C.red:C.black;
      text(v,xx+managerWidths[i]/2,y+managerRowH/2,fit(v,managerWidths[i]-4,15,700),700,color);
      xx+=managerWidths[i];
    });
  };
  managerRows.forEach((row,i)=>drawRow(row,i,false));

  // 합계 행: 매니저별 실적현황 + 집중관리 제품만 합계를 표시합니다.
  // 100점을 잡아라 프로모션은 매니저별 점수만 표시하고 하단 합계는 비워둡니다.
  const totalRow = {
    manager: "합계",
    ...totalMetrics,
    goal: totalGoal,
    shortage: totalMetrics.final - totalGoal,
    conditionValues: totalConditionValues,
    promoValues: promoRules.map(() => 0),
    promoTotal: 0
  };
  drawRow(totalRow, managerRows.length, true);

  const blob=await canvasToPngBlob(canvas);
  if(!blob) throw new Error("공유 이미지 생성에 실패했습니다.");
  return blob;
}

function dashboardShareRecordsForManagerImage() {
  const start = $("#startDateFilter")?.value || "1900-01-01";
  const end = $("#endDateFilter")?.value || "2999-12-31";
  const month = $("#monthFilter")?.value || monthIso();
  return state.records.filter((record) => {
    if (!record) return false;
    if (record.status === "취소") return false;
    const receivedDate = compactValue(record.receivedDate, "");
    if (!receivedDate) return false;
    return recordBelongsToCurrentUserTeam(record, recordGoalMonth(record, month)) && inDateRange(receivedDate, start, end);
  });
}


function filteredDashboardRecords() {
  return dashboardShareRecordsForManagerImage();
}

function managerDashboardPayload(managerName) {
  const month = $("#monthFilter")?.value || monthIso();
  const manager = teamManagers(month).find((item) => item.name === managerName);
  if (!manager) return null;

  const dashboardRecords = dashboardShareRecordsForManagerImage();
  const managerRecords = dashboardRecords.filter((record) => record.manager === manager.name);
  const totals = applyManualStatsToTotals(actuals(managerRecords), manager.name);
  const goal = managerGoalFor(manager.name);
  const rate = goal > 0 ? Math.round((totals.managerFinalActual / goal) * 1000) / 10 : 0;
  const shortage = Math.max(toNumber(goal) - totals.managerFinalActual, 0);
  const meta = state.appMeta || sampleState.appMeta;

  return {
    manager: { ...manager, goal },
    records: managerRecords,
    totals,
    rate,
    shortage,
    periodStart: $("#startDateFilter")?.value || "",
    periodEnd: $("#endDateFilter")?.value || "",
    branchName: meta.branchName || "SK지국",
    teamName: managerTeamForMonth(manager, currentDashboardMonth())
  };
}

function roundedRectPath(ctx, x, y, w, h, r) {
  const radius = Math.min(r, w / 2, h / 2);
  ctx.beginPath();
  ctx.moveTo(x + radius, y);
  ctx.arcTo(x + w, y, x + w, y + h, radius);
  ctx.arcTo(x + w, y + h, x, y + h, radius);
  ctx.arcTo(x, y + h, x, y, radius);
  ctx.arcTo(x, y, x + w, y, radius);
  ctx.closePath();
}

function fillRoundRect(ctx, x, y, w, h, r, fill, stroke = "", lineWidth = 1) {
  roundedRectPath(ctx, x, y, w, h, r);
  ctx.fillStyle = fill;
  ctx.fill();
  if (stroke) {
    ctx.lineWidth = lineWidth;
    ctx.strokeStyle = stroke;
    ctx.stroke();
  }
}

function drawCanvasText(ctx, text, x, y, size, weight = 700, color = "#0f172a", align = "left") {
  ctx.font = `${weight} ${size}px system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif`;
  ctx.fillStyle = color;
  ctx.textAlign = align;
  ctx.textBaseline = "middle";
  ctx.fillText(String(text ?? ""), x, y);
}

function formatShareNumber(value) {
  const n = toNumber(value);
  if (Number.isInteger(n)) return formatNumber(n);
  return formatNumber(Math.round(n * 10) / 10);
}



function todayKoreanDateText() {
  const now = new Date();
  const weekdays = ["일", "월", "화", "수", "목", "금", "토"];
  return `${now.getFullYear()}년 ${now.getMonth() + 1}월 ${now.getDate()}일 ${weekdays[now.getDay()]}요일`;
}

function drawShareIconTarget(ctx, x, y, size, color) {
  ctx.save();
  ctx.strokeStyle = color;
  ctx.lineWidth = Math.max(5, Math.round(size * 0.08));
  ctx.beginPath();
  ctx.arc(x, y, size * 0.38, 0, Math.PI * 2);
  ctx.stroke();
  ctx.beginPath();
  ctx.moveTo(x, y);
  ctx.lineTo(x + size * 0.36, y - size * 0.36);
  ctx.stroke();
  ctx.beginPath();
  ctx.moveTo(x + size * 0.36, y - size * 0.36);
  ctx.lineTo(x + size * 0.20, y - size * 0.34);
  ctx.moveTo(x + size * 0.36, y - size * 0.36);
  ctx.lineTo(x + size * 0.34, y - size * 0.20);
  ctx.stroke();
  ctx.restore();
}

function drawShareIconChart(ctx, x, y, size, color) {
  ctx.save();
  ctx.fillStyle = color;
  const w = size * 0.16;
  [0.35, 0.55, 0.78].forEach((h, i) => {
    ctx.fillRect(x + i * w * 1.9, y + size * (0.82 - h), w, size * h);
  });
  ctx.strokeStyle = color;
  ctx.lineWidth = Math.max(5, Math.round(size * 0.08));
  ctx.beginPath();
  ctx.moveTo(x, y + size * 0.42);
  ctx.lineTo(x + size * 0.32, y + size * 0.24);
  ctx.lineTo(x + size * 0.55, y + size * 0.30);
  ctx.lineTo(x + size * 0.86, y + size * 0.10);
  ctx.stroke();
  ctx.restore();
}

function drawShareIconTrophy(ctx, x, y, size, color) {
  ctx.save();
  ctx.fillStyle = color;
  ctx.strokeStyle = color;
  ctx.lineWidth = Math.max(4, Math.round(size * 0.07));
  fillRoundRect(ctx, x + size * 0.20, y + size * 0.12, size * 0.60, size * 0.42, size * 0.08, color);
  ctx.beginPath();
  ctx.arc(x + size * 0.20, y + size * 0.26, size * 0.17, Math.PI * 0.5, Math.PI * 1.5);
  ctx.stroke();
  ctx.beginPath();
  ctx.arc(x + size * 0.80, y + size * 0.26, size * 0.17, Math.PI * 1.5, Math.PI * 0.5);
  ctx.stroke();
  ctx.fillRect(x + size * 0.45, y + size * 0.54, size * 0.10, size * 0.20);
  fillRoundRect(ctx, x + size * 0.32, y + size * 0.72, size * 0.36, size * 0.10, size * 0.02, color);
  ctx.restore();
}

function drawShareIconWarning(ctx, x, y, size, color) {
  ctx.save();
  ctx.strokeStyle = color;
  ctx.fillStyle = color;
  ctx.lineWidth = Math.max(5, Math.round(size * 0.08));
  ctx.beginPath();
  ctx.moveTo(x + size * 0.50, y + size * 0.10);
  ctx.lineTo(x + size * 0.90, y + size * 0.82);
  ctx.lineTo(x + size * 0.10, y + size * 0.82);
  ctx.closePath();
  ctx.stroke();
  ctx.fillRect(x + size * 0.47, y + size * 0.34, size * 0.06, size * 0.28);
  ctx.beginPath();
  ctx.arc(x + size * 0.50, y + size * 0.70, size * 0.04, 0, Math.PI * 2);
  ctx.fill();
  ctx.restore();
}


async function printDashboardImageBlob() {
  const canvas = document.createElement("canvas");
  canvas.width = 1240;
  canvas.height = 1754;
  const ctx = canvas.getContext("2d");
  if (!ctx) throw new Error("프린트용 이미지 캔버스를 만들 수 없습니다.");

  const month = $("#monthFilter")?.value || monthIso();
  const setting = monthSetting(month);
  const targetPeriod = monthPeriod(month);
  const periodStart = targetPeriod.start || "1900-01-01";
  const periodEnd = targetPeriod.end || "2999-12-31";
  const records = state.records.filter((record) => {
    if (!record || record.status === "취소") return false;
    if (!recordBelongsToCurrentUserTeam(record, month)) return false;
    return inDateRange(record.receivedDate, periodStart, periodEnd);
  });
  const goals = calculatedGoals(month);
  const totals = applyManualStatsToTotals(actuals(records));
  const managers = teamManagers(month);
  const meta = state.appMeta || sampleState.appMeta;
  const branchTitle = `${meta.branchName || "SK지국"} ${meta.masterName || "사용자"} ${meta.masterRole || "담당자"}`;
  const todayLabel = todayKoreanDateText();

  const n = (value) => {
    const num = Number(value);
    return Number.isFinite(num) ? num : 0;
  };
  const safeRate = (actual, goal) => {
    const g = n(goal);
    if (g <= 0) return 0;
    return Math.max(0, (n(actual) / g) * 100);
  };
  const fmtCount = (value) => formatNumber(Math.round(n(value) * 10) / 10).replace(/\.0$/, "");
  const fmtPercent = (value, digits = 0) => `${Number(value || 0).toFixed(digits).replace(/\.0$/, "")}%`;
  const shortage = (goal, actual) => Math.max(0, n(goal) - n(actual));

  const summaryRows = [
    { label: "신규", goal: goals.newGoal, actual: totals.newCount, color: "#f0b400" },
    { label: "패키지", goal: goals.packageGoal, actual: totals.packageCount, color: "#f57d20" },
    { label: "재렌탈", goal: goals.rentalGoal, actual: totals.rentalActual, color: "#2ba24c" },
    { label: "합계", goal: goals.overallGoal, actual: totals.businessActual, color: "#2ba24c" },
    { label: "재약정", goal: goals.renewalGoal, actual: totals.renewalActual, color: "#c9ced6" },
    { label: "종합달성", goal: goals.overallGoal + goals.renewalGoal, actual: totals.managerFinalActual, color: "#2ba24c" }
  ];

  const managerRows = managers.map((manager) => {
    const managerRecords = records.filter((record) => record.manager === manager.name);
    const managerTotals = applyManualStatsToTotals(actuals(managerRecords), manager.name);
    const goal = n(managerGoalFor(manager.name));
    const final = n(managerTotals.managerFinalActual);
    return {
      manager: manager.name,
      newCount: n(managerTotals.newCount),
      packageCount: n(managerTotals.packageCount),
      rentalCount: n(managerTotals.rentalActual),
      cashCount: n(managerTotals.cashActual),
      business: n(managerTotals.businessActual),
      renewal: n(managerTotals.renewalActual),
      cons: n(managerTotals.orderConsActual),
      refund: n(managerTotals.refundActual),
      final,
      goal,
      shortage: Math.max(0, goal - final),
      rate: safeRate(final, goal)
    };
  });

  const totalRow = {
    manager: "합계",
    newCount: n(totals.newCount),
    packageCount: n(totals.packageCount),
    rentalCount: n(totals.rentalActual),
    cashCount: n(totals.cashActual),
    business: n(totals.businessActual),
    renewal: n(totals.renewalActual),
    cons: n(totals.orderConsActual),
    refund: n(totals.refundActual),
    final: n(totals.managerFinalActual),
    goal: managerRows.reduce((sum, row) => sum + row.goal, 0),
    shortage: Math.max(0, managerRows.reduce((sum, row) => sum + row.goal, 0) - n(totals.managerFinalActual)),
    rate: safeRate(n(totals.managerFinalActual), managerRows.reduce((sum, row) => sum + row.goal, 0))
  };

  ctx.fillStyle = "#f5f8fb";
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  drawCanvasText(ctx, `${branchTitle} 영업현황`, 56, 60, 28, 900, "#173558", "left");
  drawCanvasText(ctx, todayLabel, canvas.width - 56, 60, 18, 700, "#56657a", "right");
  ctx.strokeStyle = "#28a35c";
  ctx.lineWidth = 3;
  ctx.beginPath();
  ctx.moveTo(48, 90);
  ctx.lineTo(canvas.width - 48, 90);
  ctx.stroke();

  drawCanvasText(ctx, "계정", 72, 128, 17, 800, "#506070", "left");
  drawCanvasText(ctx, String(n(setting.accountCount)), 140, 128, 20, 900, "#23324a", "left");
  drawCanvasText(ctx, "목표산정기간", 240, 128, 17, 800, "#506070", "left");
  drawCanvasText(ctx, `${setting.periodStart || "-"} ~ ${setting.periodEnd || "-"}`, 384, 128, 20, 900, "#23324a", "left");
  const operatingPrintMetrics = operatingGoalMetrics(month, totals.overallActual);
  drawCanvasText(
    ctx,
    `운영목표 ${operatingPrintMetrics.rate}%  ${fmtCount(operatingPrintMetrics.operatingGoal)}건  ·  기준목표 ${fmtCount(operatingPrintMetrics.baseGoal)}건`,
    canvas.width - 56,
    128,
    17,
    900,
    "#167046",
    "right"
  );

  const panelX = 48;
  const panelW = canvas.width - 96;

  // KPI summary panel
  const kpiY = 168;
  fillRoundRect(ctx, panelX, kpiY, panelW, 610, 22, "#ffffff", "#d5deea", 2);
  drawCanvasText(ctx, "영업 KPI 요약", panelX + 28, kpiY + 28, 18, 900, "#173558", "left");

  const cols = [200, 140, 140, 140, 468];
  const tableX = panelX + 24;
  const headerY = kpiY + 60;
  const rowH = 76;
  const headerH = 46;
  const headerTitles = ["구분", "목표", "실적", "부족", "달성률"];
  let x = tableX;
  headerTitles.forEach((title, idx) => {
    fillRoundRect(ctx, x, headerY, cols[idx] - 4, headerH, 8, "#0f4c8a");
    drawCanvasText(ctx, title, x + (cols[idx] - 4) / 2, headerY + headerH / 2, 16, 800, "#ffffff", "center");
    x += cols[idx];
  });

  summaryRows.forEach((row, idx) => {
    const y = headerY + headerH + idx * rowH;
    const bg = row.label === "합계" || row.label === "종합달성" ? "#f1f7f3" : "#ffffff";
    x = tableX;
    [row.label, fmtCount(row.goal), fmtCount(row.actual), fmtCount(shortage(row.goal, row.actual)), ""].forEach((value, colIdx) => {
      const w = cols[colIdx] - 4;
      ctx.fillStyle = bg;
      ctx.fillRect(x, y, w, rowH);
      ctx.strokeStyle = "#d9e2ee";
      ctx.strokeRect(x, y, w, rowH);
      if (colIdx < 4) {
        let color = "#1a2d46";
        if (colIdx === 2) color = "#f47a14";
        if (colIdx === 3) color = "#e33c2f";
        drawCanvasText(ctx, value, colIdx === 0 ? x + 18 : x + w / 2, y + rowH / 2, colIdx === 0 ? 16 : 18, 800, color, colIdx === 0 ? "left" : "center");
      }
      x += cols[colIdx];
    });

    const rate = safeRate(row.actual, row.goal);
    const rateX = tableX + cols[0] + cols[1] + cols[2] + cols[3];
    const cellW = cols[4] - 4;
    const barX = rateX + 36;
    const barY = y + rowH / 2 - 7;
    const barW = 250;
    const barH = 14;
    fillRoundRect(ctx, barX, barY, barW, barH, 7, "#e8edf2");
    const fillW = Math.max(12, Math.min(barW, (barW * rate) / 100));
    fillRoundRect(ctx, barX, barY, fillW, barH, 7, row.color);
    drawCanvasText(ctx, fmtPercent(rate, 0), rateX + cellW - 42, y + rowH / 2, 18, 800, row.label === "재약정" ? "#9aa3ad" : row.color, "right");
  });

  // manager table panel
  const managerY = 816;
  fillRoundRect(ctx, panelX, managerY, panelW, 840, 22, "#ffffff", "#d5deea", 2);
  drawCanvasText(ctx, "매니저별 실적현황", panelX + 28, managerY + 28, 18, 900, "#173558", "left");

  const mHeaderY = managerY + 60;
  const mHeaderH = 44;
  const mRowH = 74;
  const mCols = [130, 80, 90, 100, 90, 90, 80, 80, 70, 90, 90, 80, 80];
  const mTitles = ["매니저", "신규", "패키지", "재렌탈", "일시불", "영업", "재약", "오다", "환수", "최종", "목표", "부족", "달성"];
  x = tableX;
  mTitles.forEach((title, idx) => {
    fillRoundRect(ctx, x, mHeaderY, mCols[idx] - 3, mHeaderH, 6, idx === 5 || idx === 9 ? "#1e66b5" : "#0f4c8a");
    drawCanvasText(ctx, title, x + (mCols[idx] - 3) / 2, mHeaderY + mHeaderH / 2, 14, 800, "#ffffff", "center");
    x += mCols[idx];
  });

  const allRows = [...managerRows, totalRow];
  allRows.forEach((row, idx) => {
    const y = mHeaderY + mHeaderH + idx * mRowH;
    const isTotal = idx === allRows.length - 1;
    x = tableX;
    const vals = [
      row.manager,
      row.newCount,
      row.packageCount,
      row.rentalCount,
      row.cashCount,
      row.business,
      row.renewal,
      row.cons,
      row.refund,
      row.final,
      row.goal,
      row.shortage,
      fmtPercent(row.rate, 0)
    ];
    vals.forEach((val, colIdx) => {
      const w = mCols[colIdx] - 3;
      let bg = isTotal ? "#f6f9fc" : "#ffffff";
      if (colIdx === 5 || colIdx === 9) bg = isTotal ? "#eef6ff" : "#f7fbff";
      ctx.fillStyle = bg;
      ctx.fillRect(x, y, w, mRowH);
      ctx.strokeStyle = "#d9e2ee";
      ctx.strokeRect(x, y, w, mRowH);
      let color = "#20344e";
      if (colIdx === 5 || colIdx === 9) color = "#f47a14";
      if (colIdx === 11) color = "#e33c2f";
      if (colIdx === 12) color = "#e33c2f";
      drawCanvasText(ctx, colIdx === 0 ? val : (colIdx === 12 ? val : fmtCount(val)), colIdx === 0 ? x + 12 : x + w / 2, y + mRowH / 2, colIdx === 0 ? 16 : 18, isTotal || colIdx === 0 ? 800 : 700, color, colIdx === 0 ? "left" : "center");
      x += mCols[colIdx];
    });
  });

  return await canvasToPngBlob(canvas);
}

function canvasToPngBlob(canvas) {
  return new Promise((resolve, reject) => {
    try {
      canvas.toBlob((blob) => {
        if (blob) resolve(blob);
        else reject(new Error("이미지 변환에 실패했습니다."));
      }, "image/png");
    } catch (error) {
      reject(error);
    }
  });
}

async function managerShareImageBlob(managerName) {
  const payload = managerDashboardPayload(managerName);
  if (!payload) throw new Error("매니저 정보를 찾을 수 없습니다.");

  const { manager, totals, rate, shortage, periodStart, periodEnd, branchName, teamName } = payload;
  const canvas = document.createElement("canvas");
  canvas.width = 1080;
  canvas.height = 1080;
  const ctx = canvas.getContext("2d");
  if (!ctx) throw new Error("이미지 캔버스를 만들 수 없습니다.");

  const bg = ctx.createLinearGradient(0, 0, 0, 1080);
  bg.addColorStop(0, "#061d49");
  bg.addColorStop(0.42, "#1d4d91");
  bg.addColorStop(0.74, "#e9f5ff");
  bg.addColorStop(1, "#f7fbff");
  ctx.fillStyle = bg;
  ctx.fillRect(0, 0, 1080, 1080);

  fillRoundRect(ctx, 32, 32, 1016, 1016, 34, "rgba(255,255,255,0.08)", "rgba(255,255,255,0.24)", 2);

  // Header: compact
  fillRoundRect(ctx, 72, 70, 330, 70, 10, "rgba(255,255,255,0.10)", "rgba(255,255,255,0.58)", 2);
  drawCanvasText(ctx, `${branchName} ${teamName}`, 237, 106, 34, 900, "#ffffff", "center");
  drawCanvasText(ctx, todayKoreanDateText(), 982, 106, 30, 800, "#e8f2ff", "right");

  drawCanvasText(ctx, `${manager.name} 매니저`, 72, 196, 68, 900, "#ffffff", "left");

  fillRoundRect(ctx, 72, 268, 540, 56, 28, "rgba(255,255,255,0.94)");
  drawCanvasText(ctx, `${periodStart || "-"}  ~  ${periodEnd || "-"}`, 342, 299, 33, 800, "#13305c", "center");

  // KPI panel
  fillRoundRect(ctx, 32, 370, 1016, 322, 24, "#ffffff", "#d7e3f3", 2);

  const kpiItems = [
    { label: "상시목표", value: manager.goal, unit: "건", color: "#062051", icon: "target" },
    { label: "영업실적", value: totals.businessActual, unit: "건", color: "#1268d8", icon: "chart" },
    { label: "최종실적", value: totals.managerFinalActual, unit: "건", color: "#118a43", icon: "trophy" },
    { label: "부족", value: shortage, unit: "건", color: shortage > 0 ? "#e8561d" : "#118a43", icon: "warn" }
  ];

  const kpiY = 398;
  const kpiW = 244;
  const kpiH = 174;
  kpiItems.forEach((item, index) => {
    const x = 54 + index * 244;
    fillRoundRect(ctx, x, kpiY, kpiW, kpiH, 14, "#ffffff", "#d5dfea", 2);
    drawCanvasText(ctx, item.label, x + kpiW / 2, kpiY + 34, 30, 900, "#132742", "center");

    const iconSize = 38;
    const iconX = x + kpiW / 2 - 19;
    const iconY = kpiY + 54;
    if (item.icon === "target") drawShareIconTarget(ctx, iconX + 19, iconY + 19, iconSize, "#e1142f");
    if (item.icon === "chart") drawShareIconChart(ctx, iconX, iconY, iconSize, "#2580e8");
    if (item.icon === "trophy") drawShareIconTrophy(ctx, iconX, iconY, iconSize, "#f0a516");
    if (item.icon === "warn") drawShareIconWarning(ctx, iconX, iconY, iconSize, "#ef4129");

    drawCanvasText(ctx, formatShareNumber(item.value), x + kpiW / 2 - 10, kpiY + 136, 54, 900, item.color, "center");
    drawCanvasText(ctx, item.unit, x + kpiW / 2 + 60, kpiY + 148, 23, 800, "#223954", "left");
  });

  fillRoundRect(ctx, 54, 596, 972, 70, 12, "#fbfdff", "#d5dfea", 2);
  drawCanvasText(ctx, "달성률", 130, 633, 37, 900, "#132742", "left");
  const rateColor = rate >= 100 ? "#118a43" : rate >= 80 ? "#e7780d" : "#de2f2a";
  drawCanvasText(ctx, `${formatShareNumber(rate)}%`, 926, 633, 68, 900, rateColor, "right");

  // Detail panel
  fillRoundRect(ctx, 32, 720, 1016, 264, 24, "#ffffff", "#d7e3f3", 2);

  const details = [
    ["신규", totals.newCount],
    ["패키지", totals.packageCount],
    ["재렌탈", totals.rentalActual],
    ["일시불", totals.cashActual],
    ["재약정", totals.renewalActual],
    ["컨스", totals.orderConsActual],
    ["환수", totals.refundActual]
  ];

  const cellW4 = 244;
  const cellH = 84;
  for (let i = 0; i < 4; i += 1) {
    const x = 54 + i * 244;
    const y = 746;
    fillRoundRect(ctx, x, y, cellW4, cellH, 10, "#fbfdff", "#d7e3f3", 1.5);
    drawCanvasText(ctx, details[i][0], x + cellW4 / 2, y + 30, 28, 900, "#173658", "center");
    drawCanvasText(ctx, `${formatShareNumber(details[i][1])}건`, x + cellW4 / 2, y + 65, 36, 900, "#0d2340", "center");
  }

  const cellW3 = 326;
  for (let i = 4; i < 7; i += 1) {
    const x = 54 + (i - 4) * 326;
    const y = 850;
    fillRoundRect(ctx, x, y, cellW3, cellH, 10, "#fbfdff", "#d7e3f3", 1.5);
    drawCanvasText(ctx, details[i][0], x + cellW3 / 2, y + 30, 28, 900, "#173658", "center");
    drawCanvasText(ctx, `${formatShareNumber(details[i][1])}건`, x + cellW3 / 2, y + 65, 36, 900, "#0d2340", "center");
  }

  // Footer safely inside canvas
  fillRoundRect(ctx, 32, 1004, 1016, 54, 0, "#062051");
  drawShareIconChart(ctx, 84, 1011, 46, "#2591ff");
  drawCanvasText(ctx, "오늘도 최선을 다해주셔서 감사합니다!", 560, 1031, 32, 900, "#ffffff", "center");

  return await canvasToPngBlob(canvas);
}

function openSharePreviewModal(
  blob,
  fileName,
  titleText = "공유 이미지 미리보기",
  primaryAction = "kakao"
) {
  const modal = $("#managerShareModal");
  const img = $("#managerSharePreviewImg");
  const title = $("#managerShareTitle");
  const primaryBtn = $("#managerShareKakaoBtn");
  const guide = modal?.querySelector(".share-preview-guide");
  if (!modal || !img) {
    showToast("미리보기 창을 찾지 못했습니다.");
    return;
  }

  if (currentManagerShareUrl) URL.revokeObjectURL(currentManagerShareUrl);
  currentManagerShareBlob = blob;
  currentManagerShareFileName = fileName;
  currentManagerShareUrl = URL.createObjectURL(blob);

  img.src = currentManagerShareUrl;
  if (title) title.textContent = titleText;

  if (primaryBtn) {
    primaryBtn.dataset.actionMode = primaryAction;
    primaryBtn.textContent = primaryAction === "print" ? "프린트" : "카톡 공유";
  }
  if (guide) {
    guide.textContent = primaryAction === "print"
      ? "이미지를 먼저 확인한 뒤 프린트 버튼을 눌러 영업현황 출력물을 인쇄하세요."
      : "이미지를 먼저 확인한 뒤 카톡 공유 또는 이미지 복사를 사용하세요.";
  }

  modal.hidden = false;
  document.body.classList.add("share-modal-open");
}

function openManagerShareModal(blob, fileName, managerName) {
  openSharePreviewModal(
    blob,
    fileName,
    `${managerName} 매니저 실적 이미지 미리보기`,
    "kakao"
  );
}

function closeManagerShareModal() {
  const modal = $("#managerShareModal");
  const img = $("#managerSharePreviewImg");
  if (modal) modal.hidden = true;
  if (img) img.removeAttribute("src");
  if (currentManagerShareUrl) URL.revokeObjectURL(currentManagerShareUrl);
  currentManagerShareUrl = "";
  currentManagerShareBlob = null;
  currentManagerShareFileName = "";
  document.body.classList.remove("share-modal-open");
}

async function shareManagerKakaoImage(managerName) {
  showToast(`${managerName} 매니저 이미지 미리보기를 준비합니다.`);
  let blob;
  try {
    blob = await managerShareImageBlob(managerName);
  } catch (error) {
    console.error("managerShareImageBlob failed:", error);
    showToast(`이미지 생성이 실패했습니다: ${error.message || "알 수 없는 오류"}`);
    return;
  }

  const safeName = String(managerName || "매니저").replace(/[\\/:*?"<>|]/g, "_");
  const fileName = `${safeName}-매니저-실적현황-${todayIso()}.png`;
  openManagerShareModal(blob, fileName, managerName);
  showToast(`${managerName} 매니저 이미지 미리보기를 열었습니다.`);
}

async function copyCurrentManagerShareImage() {
  if (!currentManagerShareBlob) {
    showToast("복사할 이미지가 없습니다.");
    return false;
  }

  if (navigator.clipboard && window.ClipboardItem) {
    try {
      await navigator.clipboard.write([new ClipboardItem({ "image/png": currentManagerShareBlob })]);
      showToast("이미지를 복사했습니다. 카톡 대화창에 붙여넣기 하세요.");
      return true;
    } catch (error) {
      console.warn(error);
    }
  }

  showToast("이 브라우저에서는 이미지 복사가 제한됩니다. 이미지 저장을 사용해 주세요.");
  return false;
}

function saveCurrentManagerShareImage() {
  if (!currentManagerShareBlob) {
    showToast("저장할 이미지가 없습니다.");
    return;
  }
  const url = URL.createObjectURL(currentManagerShareBlob);
  const link = document.createElement("a");
  link.href = url;
  link.download = currentManagerShareFileName || `매니저-실적현황-${todayIso()}.png`;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 500);
  showToast("공유 이미지를 저장했습니다. 카톡에서 파일로 첨부하면 됩니다.");
}

function printCurrentSharePreview() {
  if (!currentManagerShareBlob) {
    showToast("프린트할 미리보기가 없습니다.");
    return;
  }
  const url = URL.createObjectURL(currentManagerShareBlob);
  const frame = document.createElement("iframe");
  frame.style.position = "fixed";
  frame.style.right = "0";
  frame.style.bottom = "0";
  frame.style.width = "0";
  frame.style.height = "0";
  frame.style.border = "0";
  document.body.appendChild(frame);
  const doc = frame.contentWindow.document;
  doc.open();
  doc.write(`<!doctype html><html lang="ko"><head><meta charset="utf-8"><title>영업현황 프린트</title><style>
    @page { size: A4 landscape; margin: 6mm; }
    html,body{margin:0;padding:0;width:100%;height:100%;background:#fff;}
    body{display:flex;align-items:center;justify-content:center;}
    img{display:block;max-width:100%;max-height:100%;width:auto;height:auto;object-fit:contain;}
  </style></head><body><img id="previewPrintImg" src="${url}" alt="영업현황"></body></html>`);
  doc.close();
  const img = doc.getElementById("previewPrintImg");
  const trigger = () => setTimeout(() => { frame.contentWindow.focus(); frame.contentWindow.print(); }, 180);
  if (img?.complete) trigger(); else img?.addEventListener("load", trigger, { once: true });
  frame.contentWindow.onafterprint = () => setTimeout(() => { URL.revokeObjectURL(url); frame.remove(); }, 500);
}

async function kakaoShareCurrentManagerImage() {
  if (!currentManagerShareBlob) {
    showToast("공유할 이미지가 없습니다.");
    return;
  }

  const file = new File([currentManagerShareBlob], currentManagerShareFileName || `매니저-실적현황-${todayIso()}.png`, { type: "image/png" });

  try {
    if (navigator.canShare?.({ files: [file] })) {
      await navigator.share({ files: [file], title: "매니저 실적현황" });
      return;
    }
  } catch (error) {
    console.warn(error);
  }

  const copied = await copyCurrentManagerShareImage();
  if (copied) {
    showToast("카톡 자동 연결이 제한되어 이미지를 복사했습니다. 카톡 대화창에 붙여넣기 하세요.");
  } else {
    saveCurrentManagerShareImage();
    showToast("카톡 자동 연결이 제한되어 이미지를 저장했습니다. 카톡에서 첨부해 주세요.");
  }
}



async function shareKakaoImage() {
  showToast("영업현황 공유 이미지 미리보기를 준비합니다.");
  let blob;
  try {
    blob = await reportImageBlob();
  } catch (error) {
    console.error(error);
    showToast(`이미지 생성이 실패했습니다: ${error.message || "알 수 없는 오류"}`);
    return;
  }

  openSharePreviewModal(
    blob,
    `영업현황-${todayIso()}.png`,
    "영업현황 엑셀형식 미리보기",
    "print"
  );
  showToast("영업현황 미리보기를 열었습니다. 확인 후 프린트할 수 있습니다.");
}

async function importExcelFile(file) {
  try {
    const importedData = isCsvFile(file) ? await readCsvImport(file) : await readExcelImport(file);
    mergeImportedExcel(importedData);
  } catch (error) {
    console.error(error);
    showToast(`파일을 읽지 못했습니다: ${error.message || "알 수 없는 오류"}`);
  }
}

function isCsvFile(file) {
  const name = String(file?.name || "").toLowerCase();
  const type = String(file?.type || "").toLowerCase();
  return /\.(csv|tsv|txt)$/.test(name) || type.includes("csv") || type.includes("text/plain");
}

async function readCsvImport(file) {
  const text = await readDelimitedTextFile(file);
  const rows = parseCsvText(text);
  if (!rows.length) return { records: [], managers: [], monthSettings: {} };

  const headerIndex = findDelimitedHeaderIndex(rows);
  if (headerIndex < 0) return { records: [], managers: [], monthSettings: {} };

  const header = rows[headerIndex].map((value) => String(value || "").trim());
  const managers = new Set();
  const records = rows.slice(headerIndex + 1).map((row) => recordFromDelimitedRow(row, header)).filter(Boolean);
  records.forEach((record) => { if (record.manager) managers.add(record.manager); });
  return { records, managers: [...managers], monthSettings: {} };
}

async function readDelimitedTextFile(file) {
  const buffer = await file.arrayBuffer();
  const utf8Text = new TextDecoder("utf-8").decode(buffer);
  if (looksLikeSalesCsvText(utf8Text)) return utf8Text;

  for (const encoding of ["euc-kr", "ks_c_5601-1987"]) {
    try {
      const decoded = new TextDecoder(encoding).decode(buffer);
      if (looksLikeSalesCsvText(decoded)) return decoded;
    } catch {
      // Some browser engines do not expose every Korean legacy encoding label.
    }
  }

  return utf8Text;
}

function looksLikeSalesCsvText(text) {
  const compact = String(text || "").replace(/\s+/g, "");
  return compact.includes("매니저") || compact.includes("접수날짜") || compact.includes("접수일") || compact.includes("고객번호") || compact.includes("판매종류");
}

function parseCsvText(text) {
  const source = String(text || "").replace(/^\ufeff/, "");
  const delimiter = detectDelimitedSeparator(source);
  return parseDelimitedText(source, delimiter);
}

function detectDelimitedSeparator(source) {
  const sampleLines = String(source || "").split(/\r?\n/).slice(0, 10);
  const candidates = [",", "\t", ";"];
  const scores = Object.fromEntries(candidates.map((item) => [item, 0]));

  sampleLines.forEach((line) => {
    let quoted = false;
    for (let i = 0; i < line.length; i += 1) {
      const char = line[i];
      const next = line[i + 1];
      if (char === '"' && quoted && next === '"') {
        i += 1;
      } else if (char === '"') {
        quoted = !quoted;
      } else if (!quoted && Object.prototype.hasOwnProperty.call(scores, char)) {
        scores[char] += 1;
      }
    }
  });

  return candidates.sort((a, b) => scores[b] - scores[a])[0] || ",";
}

function parseDelimitedText(text, delimiter = ",") {
  const source = String(text || "").replace(/^\ufeff/, "");
  const rows = [];
  let row = [];
  let cell = "";
  let quoted = false;

  for (let i = 0; i < source.length; i += 1) {
    const char = source[i];
    const next = source[i + 1];
    if (quoted) {
      if (char === '"' && next === '"') {
        cell += '"';
        i += 1;
      } else if (char === '"') {
        quoted = false;
      } else {
        cell += char;
      }
      continue;
    }

    if (char === '"') {
      quoted = true;
    } else if (char === delimiter) {
      row.push(cell);
      cell = "";
    } else if (char === "\n") {
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
    } else if (char !== "\r") {
      cell += char;
    }
  }

  if (cell || row.length) {
    row.push(cell);
    rows.push(row);
  }

  return rows.filter((item) => item.some((value) => String(value || "").trim()));
}

function findDelimitedHeaderIndex(rows) {
  return rows.findIndex((row) => {
    const joined = row.map(normalizeHeaderKey).join("|");
    const hasManager = joined.includes("매니저") || joined.includes("담당자");
    const hasDate = joined.includes("접수날짜") || joined.includes("접수일") || joined.includes("접수일자");
    const hasCustomer = joined.includes("고객번호") || joined.includes("고객명") || joined.includes("연락처");
    return hasManager && (hasDate || hasCustomer);
  });
}

function normalizeHeaderKey(value) {
  return String(value || "")
    .replace(/^\ufeff/, "")
    .replace(/[\s\n\r_()\[\]{}·ㆍ:：\/\-]/g, "")
    .trim();
}

function delimitedCell(row, header, labels) {
  const normalizedLabels = labels.map(normalizeHeaderKey);
  const index = header.findIndex((item) => normalizedLabels.includes(normalizeHeaderKey(item)));
  return index >= 0 ? row[index] : "";
}

function firstDateFromText(value, order = 0) {
  const matches = String(value || "").match(/\d{4}[-/.]\d{1,2}[-/.]\d{1,2}/g) || [];
  return matches[order] ? normalizeImportedDate(matches[order]) : "";
}

function recordFromDelimitedRow(row, header) {
  const value = (...labels) => delimitedCell(row, header, labels);
  const combinedDateCell = value("접수일설치요청일", "접수일/설치요청일", "접수일 설치요청일", "접수일");
  const receivedDate = normalizeImportedDate(value("접수날짜", "접수일자", "접수일")) || firstDateFromText(combinedDateCell, 0);
  const installDate = normalizeImportedDate(value("설치날짜", "설치요청일", "설치일", "설치예정일")) || firstDateFromText(combinedDateCell, 1);
  const manager = String(value("매니저", "담당매니저", "담당자") || "").trim();
  const explicitCategoryValue = value("판매종류", "판매유형", "판매구분", "제품군");
  const genericDistinctionValue = value("구분");
  const category = normalizeCategory(explicitCategoryValue || genericDistinctionValue);
  const activityType = normalizeActivityType(
    value("컨스/지원", "활동구분", "지원구분", "영업구분") || (explicitCategoryValue ? genericDistinctionValue : "")
  );
  const product = String(value("제품명", "상품명", "모델명", "제품") || "").trim();
  const customerName = String(value("고객명", "고객이름", "성명") || "").trim();
  const customerNo = String(value("신규 고객번호", "신규고객번호", "고객번호", "신규번호") || "").trim();
  if (!manager || (!category && !product) || (!customerName && !customerNo && !product)) return null;
  const cashAmount = toNumber(String(value("일시불금액", "일시불 금액", "금액") || "").replaceAll(",", ""));
  const qrValue = value("일시불QR", "일시불 QR", "QR");
  return {
    id: uid("r"),
    status: String(value("완료여부", "상태", "진행상태") || "접수").trim() || "접수",
    receivedDate: receivedDate || todayIso(),
    installDate,
    manager,
    count: toNumber(value("건수", "수량")) || 1,
    previousCustomer: String(value("전 고객번호", "기존 고객번호", "기존고객번호", "이전 고객번호", "이전고객번호") || "").trim(),
    customerNo,
    phone: String(value("연락처", "전화번호", "휴대폰", "핸드폰", "휴대폰번호") || "").trim(),
    customerName,
    category: category || "기타",
    activityType,
    qr: String(qrValue || "").trim(),
    cashAmount: cashAmount > 999 ? cashAmount : 0,
    product,
    seller: String(value("실판매자", "판매자", "접수자") || "").trim(),
    memo: [
      String(value("기타내용", "메모", "비고", "기타", "특이사항") || "").trim(),
      String(value("컨스", "컨스여부") || "").trim() ? `컨스 ${String(value("컨스", "컨스여부") || "").trim()}` : "",
      String(value("오다컨스", "오다컨스여부") || "").trim() ? `컨스 ${String(value("오다컨스", "오다컨스여부") || "").trim()}` : "",
      String(value("지원", "지원여부", "영업지원") || "").trim() ? `지원 ${String(value("지원", "지원여부", "영업지원") || "").trim()}` : ""
    ].filter(Boolean).join(" / "),
    source: "excel"
  };
}

function normalizeImportedDate(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (/^\d{4}-\d{1,2}-\d{1,2}$/.test(text)) {
    const [year, month, day] = text.split("-");
    return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
  }
  if (/^\d{4}\.\d{1,2}\.\d{1,2}\.?$/.test(text)) {
    const [year, month, day] = text.replace(/\.$/, "").split(".");
    return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
  }
  if (/^\d{4}\/\d{1,2}\/\d{1,2}$/.test(text)) {
    const [year, month, day] = text.split("/");
    return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
  }
  const embedded = String(text).match(/\d{4}[-/.]\d{1,2}[-/.]\d{1,2}/);
  if (embedded) return normalizeImportedDate(embedded[0]);
  return excelDate(text);
}

async function readExcelImport(file) {
  // 정적 웹 배포에서도 동일하게 동작하도록 브라우저에서 직접 XLSX를 파싱합니다.
  return parseWorkbookClient(await readXlsx(file));
}

function parseWorkbookClient(workbook) {
  const imported = [];
  const monthSettings = {};
  const managers = new Set();

  workbook.sheets.forEach((sheet) => {
    const rows = sheet.rows;
    const sheetMonth = inferSheetMonth(sheet.name, rows);
    const setting = inferMonthSetting(rows);
    if (sheetMonth && setting) monthSettings[sheetMonth] = setting;

    rows.forEach((row, index) => {
      const headerKind = detectImportHeaderKind(row);
      if (!headerKind) return;
      const header = row;
      for (let i = index + 1; i < rows.length; i += 1) {
        const next = rows[i];
        if (detectImportHeaderKind(next) || cellValue(next, "A") === "맴버쉽 가입현황") break;
        const record = recordFromExcelRow(next, header, sheetMonth, headerKind);
        if (!record) continue;
        imported.push(record);
        if (record.manager) managers.add(record.manager);
      }
    });
  });

  return { records: imported, managers: [...managers], monthSettings };
}

function mergeImportedExcel(importedData) {
  const imported = importedData.records || [];
  const importedManagers = new Set(state.managers.map((manager) => manager.name));

  // 먼저 엑셀에 포함된 월별 목표기간을 반영한 뒤 접수일을 목표월로 환산합니다.
  Object.entries(importedData.monthSettings || {}).forEach(([month, setting]) => {
    state.monthSettings[month] = { ...monthSetting(month), ...setting };
  });
  const importedMonths = new Set(imported.map((record) => recordGoalMonth(record)).filter(Boolean));

  (importedData.managers || []).forEach((name) => {
    if (name && !importedManagers.has(name)) {
      state.managers.push(normalizeManager({
        id: uid("m"),
        name,
        team: defaultTeamName(),
        areas: [],
        goal: 0,
        status: "active",
        joinedMonth: [...importedMonths].sort()[0] || monthIso(),
        teamHistory: [{ team: defaultTeamName(), startMonth: [...importedMonths].sort()[0] || monthIso(), endMonth: "" }],
        createdAt: new Date().toISOString()
      }));
      importedManagers.add(name);
    }
  });

  if (!imported.length) {
    showToast("가져올 접수 내역을 찾지 못했습니다.");
    return;
  }

  state.records = state.records.filter((record) => {
    const month = recordGoalMonth(record);
    const brokenOldImport = record.category === "기타" && /^\d+$/.test(String(record.product || ""));
    return !importedMonths.has(month) || (record.source !== "excel" && !brokenOldImport);
  });
  const existingKeys = new Set(state.records.map(recordKey));
  const uniqueRecords = imported.filter((record) => !existingKeys.has(recordKey(record)));
  state.records = [...uniqueRecords, ...state.records];
  ensureManagerDataIntegrity(state);
  invalidateManagerCaches();
  saveState(`${uniqueRecords.length}건을 엑셀에서 가져왔습니다.`);
}

function recordKey(record) {
  return [record.receivedDate, record.manager, record.customerNo, record.customerName, record.category, record.product].join("|");
}

function inferSheetMonth(sheetName, rows) {
  const nameMatch = sheetName.match(/(\d{2})년\s*(\d{1,2})월/);
  if (nameMatch) return `20${nameMatch[1]}-${nameMatch[2].padStart(2, "0")}`;
  const title = cellValue(rows[0], "A");
  const titleMatch = title.match(/(\d{4})년\s*(\d{1,2})월/);
  if (titleMatch) return `${titleMatch[1]}-${titleMatch[2].padStart(2, "0")}`;
  return "";
}

function inferMonthSetting(rows) {
  const accountCount = toNumber(cellValue(rows[2], "B"));
  const sheetMonth = inferPeriodBaseMonth(rows);
  const newGoal = toNumber(cellValue(rows[4], "B"));
  const row4 = rows[3] || {};
  const rentalColumn = cellValue(row4, "C") === "재탈" ? "C" : "D";
  const renewalColumn = cellValue(row4, "G") === "재약정" ? "G" : "H";
  const rentalTarget = toNumber(cellValue(rows[4], rentalColumn));
  const renewalGoal = toNumber(cellValue(rows[4], renewalColumn));

  if (!accountCount) return null;
  const newIndex = newGoal > 0 ? roundSetting(accountCount * 0.6 / newGoal) : 9.5;
  const rentalIndex = rentalTarget > 0 ? roundSetting(accountCount * 0.3 / rentalTarget) : 5;
  const renewalIndex = renewalGoal > 0 ? roundSetting(accountCount * 0.1 / renewalGoal) : 18;
  return {
    accountCount,
    newWeight: 60,
    rentalWeight: 30,
    renewalWeight: 10,
    newIndex,
    rentalIndex,
    renewalIndex,
    ...inferPeriodFromText(cellValue(rows[2], "C"), sheetMonth)
  };
}

function inferPeriodBaseMonth(rows) {
  const title = String(cellValue(rows[0], "A") || "");
  const match = title.match(/(\d{4})년\s*(\d{1,2})월/);
  return match ? `${match[1]}-${match[2].padStart(2, "0")}` : "";
}

function inferPeriodFromText(text, sheetMonth) {
  const match = String(text || "").match(/(\d{2})\.(\d{1,2})\.(\d{1,2})\s*~\s*(\d{1,2})\.(\d{1,2})/);
  if (!match) return {};
  const startYear = `20${match[1]}`;
  const startMonth = match[2].padStart(2, "0");
  const startDay = match[3].padStart(2, "0");
  let endYear = startYear;
  const endMonth = match[4].padStart(2, "0");
  const endDay = match[5].padStart(2, "0");
  if (sheetMonth) {
    const [sheetYear] = sheetMonth.split("-");
    endYear = sheetYear;
  }
  if (Number(endMonth) < Number(startMonth)) endYear = String(Number(startYear) + 1);
  return {
    periodStart: `${startYear}-${startMonth}-${startDay}`,
    periodEnd: `${endYear}-${endMonth}-${endDay}`
  };
}

function roundSetting(value) {
  return Math.round(value * 10) / 10;
}

function detectImportHeaderKind(row) {
  const labels = Object.values(row || {}).map((value) => normalizeImportHeader(value));
  const has = (name) => labels.includes(normalizeImportHeader(name));
  const newFormat = has("접수일") && has("설치지시일") && has("진행상태") && has("판매자") && has("고객번호") && has("주문자명") && has("상품명");
  if (newFormat) return "order";
  if (has("완료여부") && (has("접수날짜") || has("접수일"))) return "legacy";
  return "";
}

function normalizeImportHeader(value) {
  return String(value || "").replace(/\s+/g, "").trim();
}

function cleanImportedManager(value) {
  return String(value || "").replace(/\s*\([^)]*\)\s*$/, "").trim();
}

function classifyImportedSalesType(product) {
  const text = String(product || "").trim();
  if (!text) return "신규";
  const compact = text.replace(/\s+/g, "");
  const upper = compact.toUpperCase();

  // CAD-CRC 상품은 주문접수에서 접수내역 자체를 생성하지 않습니다.
  if (upper.includes("CAD-CRC")) return "제외";

  // 상품명 안의 판매종류 표시는 /P/, /R/, R/P 형태를 우선 인식합니다.
  // 'CP-' 자체에도 P가 들어가므로 단순히 문자 P/R 포함 여부로 판정하지 않습니다.
  const hasPackage = /(?:^|[\/(])P(?:[\/)]|$)/i.test(compact) || compact.includes("패키지") || upper.includes("R/P");
  const hasRental = /(?:^|[\/(])R(?:[\/)]|$)/i.test(compact) || compact.includes("재렌탈") || compact.includes("재렌탈전용");
  const hasCash = compact.includes("일시불");

  // R/P는 패키지를 우선합니다.
  if (hasPackage) return "패키지";
  if (hasRental) return "재렌탈";
  if (hasCash) return "일시불";
  return "신규";
}

function recordFromExcelRow(row, header, sheetMonth, headerKind = "legacy") {
  if (headerKind === "order") {
    const statusSource = String(cellByHeader(row, header, "상태") || "").trim();
    const progress = String(cellByHeader(row, header, "진행상태") || "").trim();
    if (statusSource === "주문취소") return null;
    if (!["요청", "확인", "완료"].includes(progress)) return null;

    const receivedDate = excelDate(cellByHeader(row, header, "접수일"));
    const installDate = excelDate(cellByHeader(row, header, "설치지시일"));
    const manager = cleanImportedManager(cellByHeader(row, header, "판매자"));
    const customerNo = String(cellByHeader(row, header, "고객번호") || "").trim();
    const customerName = String(cellByHeader(row, header, "주문자명") || "").trim();
    const product = String(cellByHeader(row, header, "상품명") || "").trim();
    const memo = String(cellByHeader(row, header, "특이사항") || "").trim();
    const category = classifyImportedSalesType(product);
    if (category === "제외") return null;
    if (!receivedDate || !manager || !customerNo || !customerName || !product) return null;

    return {
      id: uid("r"),
      status: progress,
      receivedDate,
      installDate,
      manager,
      count: 1,
      previousCustomer: "",
      customerNo,
      phone: "",
      customerName,
      category: category || "기타",
      activityType: "",
      qr: "",
      cashAmount: 0,
      product,
      seller: "",
      memo,
      source: "excel"
    };
  }

  const receivedDate = excelDate(cellByHeader(row, header, "접수날짜"));
  const manager = cleanImportedManager(cellByHeader(row, header, "매니저"));
  const explicitCategoryValue = cellByHeader(row, header, "판매종류");
  const genericDistinctionValue = cellByHeader(row, header, "구분");
  const category = normalizeCategory(explicitCategoryValue || genericDistinctionValue);
  const activityType = normalizeActivityType(
    cellByHeader(row, header, "컨스/지원") || cellByHeader(row, header, "활동구분") || cellByHeader(row, header, "지원구분") || (explicitCategoryValue ? genericDistinctionValue : "")
  );
  const product = String(cellByHeader(row, header, "제품명") || "").trim();
  const customerName = String(cellByHeader(row, header, "고객명") || cellByHeader(row, header, "설치자명") || "").trim();
  const customerNo = String(cellByHeader(row, header, "신규 고객번호") || cellByHeader(row, header, "고객번호") || "").trim();
  if (!manager || (!category && !product) || (!customerName && !customerNo && !product)) return null;

  const qrValue = cellByHeader(row, header, "일시불\nQR") || cellByHeader(row, header, "일시불QR");
  const numericQr = toNumber(String(qrValue).replaceAll(",", ""));
  return {
    id: uid("r"),
    status: String(cellByHeader(row, header, "완료여부") || "접수").trim() || "접수",
    receivedDate: receivedDate || `${sheetMonth || "2026-01"}-01`,
    installDate: excelDate(cellByHeader(row, header, "설치날짜")),
    manager,
    count: toNumber(cellByHeader(row, header, "건수")) || 1,
    previousCustomer: String(cellByHeader(row, header, "전 고객번호") || "").trim(),
    customerNo,
    phone: String(cellByHeader(row, header, "연락처") || "").trim(),
    customerName,
    category: category || "기타",
    activityType,
    qr: String(qrValue || "").trim(),
    cashAmount: numericQr > 999 ? numericQr : 0,
    product,
    seller: String(cellByHeader(row, header, "실판매자") || "").trim(),
    memo: String(cellByHeader(row, header, "기타내용") || "").trim(),
    source: "excel"
  };
}
function normalizeCategory(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (text.includes("재렌탈")) return "재렌탈";
  if (text === "재탈") return "재렌탈";
  if (["멤버십", "멤버쉽", "맴버십", "맴버쉽"].includes(text)) return "맴버쉽";
  if (categories.includes(text)) return text;
  return text;
}

function cellByHeader(row, header, label) {
  if (!row || !header) return "";
  const entry = Object.entries(header).find(([, value]) => String(value).replace(/\s+/g, "") === String(label).replace(/\s+/g, ""));
  return entry ? row[entry[0]] : "";
}

function cellValue(row, column) {
  return row?.[column] ?? "";
}

function excelDate(value) {
  if (!value) return "";
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
  const serial = Number(value);
  if (!Number.isFinite(serial)) return String(value);
  const date = new Date(Date.UTC(1899, 11, 30) + serial * 86400000);
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}-${String(date.getUTCDate()).padStart(2, "0")}`;
}

async function readXlsx(file) {
  const entries = await unzipXlsx(await file.arrayBuffer());
  const parser = new DOMParser();
  const sharedStrings = parseSharedStrings(parser.parseFromString(await zipText(entries, "xl/sharedStrings.xml"), "application/xml"));
  const workbookXml = parser.parseFromString(await zipText(entries, "xl/workbook.xml"), "application/xml");
  const relsXml = parser.parseFromString(await zipText(entries, "xl/_rels/workbook.xml.rels"), "application/xml");
  const rels = Object.fromEntries([...relsXml.querySelectorAll("Relationship")].map((rel) => [rel.getAttribute("Id"), rel.getAttribute("Target")]));
  const sheets = [...workbookXml.querySelectorAll("sheet")].map((sheet) => {
    const rid = sheet.getAttribute("r:id");
    const target = rels[rid] || "";
    const path = target.startsWith("xl/") ? target : `xl/${target}`;
    const xml = parser.parseFromString(entries[path], "application/xml");
    return { name: sheet.getAttribute("name") || "", rows: parseSheetRows(xml, sharedStrings) };
  });
  return { sheets };
}

async function zipText(entries, path) {
  if (!entries[path]) return "";
  return entries[path];
}

function parseSharedStrings(xml) {
  return [...xml.querySelectorAll("si")].map((node) => [...node.querySelectorAll("t")].map((item) => item.textContent).join(""));
}

function parseSheetRows(xml, sharedStrings) {
  const rows = [];
  xml.querySelectorAll("sheetData row").forEach((rowNode) => {
    const row = {};
    rowNode.querySelectorAll("c").forEach((cell) => {
      const ref = cell.getAttribute("r") || "";
      const column = ref.replace(/[0-9]/g, "");
      const type = cell.getAttribute("t");
      let value = cell.querySelector("v")?.textContent ?? "";
      if (type === "s") value = sharedStrings[Number(value)] || "";
      if (type === "inlineStr") value = cell.querySelector("t")?.textContent || "";
      row[column] = value;
    });
    rows[Number(rowNode.getAttribute("r")) - 1] = row;
  });
  return rows;
}

async function unzipXlsx(buffer) {
  const view = new DataView(buffer);
  let eocd = -1;
  for (let i = view.byteLength - 22; i >= 0; i -= 1) {
    if (view.getUint32(i, true) === 0x06054b50) { eocd = i; break; }
  }
  if (eocd < 0) throw new Error("ZIP directory not found");
  const count = view.getUint16(eocd + 10, true);
  let offset = view.getUint32(eocd + 16, true);
  const entries = {};

  for (let i = 0; i < count; i += 1) {
    if (view.getUint32(offset, true) !== 0x02014b50) throw new Error("Invalid ZIP entry");
    const method = view.getUint16(offset + 10, true);
    const compressedSize = view.getUint32(offset + 20, true);
    const nameLength = view.getUint16(offset + 28, true);
    const extraLength = view.getUint16(offset + 30, true);
    const commentLength = view.getUint16(offset + 32, true);
    const localOffset = view.getUint32(offset + 42, true);
    const name = new TextDecoder().decode(new Uint8Array(buffer, offset + 46, nameLength));
    const localNameLength = view.getUint16(localOffset + 26, true);
    const localExtraLength = view.getUint16(localOffset + 28, true);
    const dataStart = localOffset + 30 + localNameLength + localExtraLength;
    const compressed = buffer.slice(dataStart, dataStart + compressedSize);
    const data = method === 0 ? compressed : await inflateRaw(compressed);
    entries[name] = new TextDecoder("utf-8").decode(data);
    offset += 46 + nameLength + extraLength + commentLength;
  }
  return entries;
}

async function inflateRaw(buffer) {
  if (!("DecompressionStream" in window)) throw new Error("This browser cannot read compressed xlsx files");
  try {
    const stream = new Blob([buffer]).stream().pipeThrough(new DecompressionStream("deflate-raw"));
    return await new Response(stream).arrayBuffer();
  } catch {
    const stream = new Blob([buffer]).stream().pipeThrough(new DecompressionStream("deflate"));
    return await new Response(stream).arrayBuffer();
  }
}



function renewalGuideSelectedMonth() {
  const input = $("#renewalGuideMonthInput");
  let value = String(input?.value || $("#monthFilter")?.value || monthIso()).trim();
  if (!/^\d{4}-\d{2}$/.test(value)) value = monthIso();
  if (input && input.value !== value) input.value = value;
  return value;
}

function renewalGuideMonthTitle(month) {
  const [year, rawMonth] = String(month || monthIso()).split("-").map((item) => Number(item));
  return `${year}년 ${rawMonth}월 기준 재렌탈 · 재약정 한눈에 보기`;
}

function renewalGuideSubTitle() {
  return "개월차별 가능여부와 조건을 쉽게 확인하는 안내표";
}

function renewalGuideOffsetMonth(baseMonth, offset) {
  const [year, rawMonth] = String(baseMonth).split("-").map((item) => Number(item));
  const date = new Date(year, rawMonth - 1, 1);
  date.setMonth(date.getMonth() - offset);
  return `${date.getFullYear()}년 ${date.getMonth() + 1}월`;
}

function renewalGuideGroups(month) {
  const defs = [
    { start: 36, end: 36, tone: "special", title: "재탈 가능", headline: "약정 / 36의무 / 12XY 재탈", bullets: [] },
    { start: 37, end: 37, tone: "blocked", title: "아무것도 안됨", headline: "별도 가능 조건 없음", bullets: [] },
    { start: 38, end: 47, tone: "half", title: "재탈 가능", headline: "의무 60 / 72 / 84 포함", bullets: ["비데는 의무 60만 가능", "재렌탈시 0.5건 인정"] },
    { start: 48, end: 49, tone: "contract", title: "재약정 가능", subTitle: "(48개월차와 동일)", headline: "재렌탈시 0.5건 인정", bullets: [] },
    { start: 50, end: 50, tone: "green", title: "재탈 가능", headline: "48의무 재탈 가능", bullets: [] },
    { start: 51, end: 54, tone: "green", title: "재탈 가능", headline: "약정 끝난 고객 재탈 가능", bullets: [] },
    { start: 55, end: 55, tone: "amber", title: "재탈 가능", headline: "약정 중 재탈 가능", bullets: [] },
    { start: 56, end: 57, tone: "purple", title: "TS / SS 전용", headline: "60의무 재탈 가능", bullets: ["재렌탈시 1건 인정"] },
    { start: 58, end: 59, tone: "teal", title: "재탈 가능", headline: "60개월 의무 모든 정수기 재탈 가능", bullets: ["재렌탈시 1건 인정"] },
    { start: 60, end: 62, tone: "cyan", title: "재탈 가능", headline: "재렌탈시 1건 인정", bullets: [] }
  ];

  return defs.map((group) => ({
    ...group,
    rows: Array.from({ length: group.end - group.start + 1 }, (_, index) => {
      const cycleMonth = group.start + index;
      return {
        cycleMonth,
        yearMonth: renewalGuideOffsetMonth(month, cycleMonth)
      };
    })
  }));
}

function renewalGuideRowClass(group, row, isFirstRow) {
  const classes = ["renewal-row", `renewal-tone-${group.tone}`];
  if (row.cycleMonth === group.start) classes.push("group-start");
  if (row.cycleMonth === group.end) classes.push("group-end");
  if (isFirstRow) classes.push("group-anchor");
  return classes.join(" ");
}

function renewalGuideMark(group) {
  const marks = {
    special: "✓",
    blocked: "－",
    half: "✓",
    contract: "🤝",
    green: "✓",
    amber: "✓",
    purple: "★",
    teal: "🛡",
    cyan: "✓"
  };
  return marks[group.tone] || "✓";
}

function renderRenewalGuideGroupCell(group) {
  const bullets = Array.isArray(group.bullets) ? group.bullets.filter(Boolean) : [];
  return `
    <div class="guide-block tone-${group.tone}">
      <div class="guide-head">
        <span class="guide-mark">${renewalGuideMark(group)}</span>
        <div class="guide-title-wrap">
          <div class="guide-title">${escapeHtml(group.title || "")}${group.subTitle ? `<small>${escapeHtml(group.subTitle)}</small>` : ""}</div>
          ${group.headline ? `<div class="guide-headline">${escapeHtml(group.headline)}</div>` : ""}
        </div>
      </div>
      ${bullets.length ? `<ul class="guide-bullets">${bullets.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>` : ""}
    </div>`;
}

function renderRenewalGuideSheet(month, compact = false) {
  const groups = renewalGuideGroups(month);
  const bodyRows = groups.map((group) => group.rows.map((row, index) => `
    <tr class="${renewalGuideRowClass(group, row, index === 0)}">
      <td class="cycle-cell"><span class="cycle-num">${row.cycleMonth}</span><span class="cycle-unit">개월차</span></td>
      <td class="ym-cell">${row.yearMonth}</td>
      ${index === 0 ? `<td class="guide-cell tone-${group.tone}" rowspan="${group.rows.length}">${renderRenewalGuideGroupCell(group)}</td>` : ""}
    </tr>`).join("")).join("");

  return `
    <div class="renewal-sheet ${compact ? "compact" : ""}">
      <div class="renewal-sheet-header">
        <div class="renewal-sheet-title">${renewalGuideMonthTitle(month)}</div>
        <div class="renewal-sheet-subtitle">${renewalGuideSubTitle()}</div>
      </div>
      <div class="renewal-sheet-table-wrap">
        <table class="renewal-sheet-table renewal-guide-visual-table">
          <colgroup>
            <col class="col-cycle">
            <col class="col-month">
            <col class="col-note">
          </colgroup>
          <thead>
            <tr>
              <th>개월차</th>
              <th>년월</th>
              <th>안내</th>
            </tr>
          </thead>
          <tbody>${bodyRows}</tbody>
        </table>
      </div>
      <div class="renewal-sheet-footer">※ 실무 참고용 요약표 / 세부 운영정책은 내부 기준 확인</div>
    </div>`;
}

function renderRenewalGuide() {
  const month = renewalGuideSelectedMonth();
  const titleNode = $("#renewalGuidePreviewTitle");
  if (titleNode) titleNode.textContent = renewalGuideMonthTitle(month);
  const preview = $("#renewalGuidePreview");
  if (preview) preview.innerHTML = renderRenewalGuideSheet(month, false);
}

function printRenewalGuide() {
  const month = renewalGuideSelectedMonth();
  const selectedCount = toNumber($("#renewalGuidePrintCount")?.value);
  const copyCount = selectedCount === 3 ? 3 : 2;
  const copies = Array.from({ length: copyCount }, (_, index) => `
    <div class="renewal-print-copy${index < copyCount - 1 ? " has-cutline" : ""}">
      ${renderRenewalGuideSheet(month, true)}
    </div>`).join("");

  const iframe = document.createElement("iframe");
  iframe.style.position = "fixed";
  iframe.style.right = "0";
  iframe.style.bottom = "0";
  iframe.style.width = "0";
  iframe.style.height = "0";
  iframe.style.border = "0";
  document.body.appendChild(iframe);

  const doc = iframe.contentWindow.document;
  const stylesheetHref = document.querySelector('link[rel="stylesheet"]')?.href || './styles.css';
  const isThree = copyCount === 3;
  doc.open();
  doc.write(`<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>${renewalGuideMonthTitle(month)}</title>
<link rel="stylesheet" href="${stylesheetHref}">
<style>
  @page { size: A4 landscape; margin: 3mm; }
  * { box-sizing: border-box; }
  html, body {
    width: 291mm;
    height: 204mm;
    margin: 0;
    padding: 0;
    overflow: hidden;
    background: #fff;
    font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', sans-serif;
    color: #18352a;
  }
  .renewal-print-layout {
    width: 291mm;
    height: 204mm;
    display: grid;
    grid-template-columns: repeat(${copyCount}, minmax(0, 1fr));
    gap: 0;
    align-items: stretch;
    overflow: hidden;
  }
  .renewal-print-copy {
    position: relative;
    width: 100%;
    height: 204mm;
    min-width: 0;
    padding: ${isThree ? "2mm 2.2mm" : "2mm 3mm"};
    overflow: hidden;
    display: flex;
    justify-content: center;
    align-items: flex-start;
  }
  .renewal-print-copy.has-cutline::after {
    content: '';
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    border-right: 0.35mm dashed #8f969a;
    pointer-events: none;
  }
  .renewal-print-copy > .renewal-sheet {
    width: 100% !important;
    max-width: none !important;
    min-width: 0 !important;
    margin: 0 !important;
    box-shadow: none !important;
    border-radius: ${isThree ? "10px" : "13px"} !important;
    border-width: 1.4px !important;
    transform: none !important;
  }
  .renewal-sheet-table-wrap { overflow: hidden !important; }
  .renewal-sheet.compact .renewal-sheet-header { padding: ${isThree ? "5px 5px 4px" : "7px 7px 5px"}; }
  .renewal-sheet.compact .renewal-sheet-title { font-size: ${isThree ? "9.2pt" : "13pt"}; line-height: 1.12; }
  .renewal-sheet.compact .renewal-sheet-subtitle { margin-top: 2px; font-size: ${isThree ? "5.4pt" : "7.4pt"}; }
  .renewal-sheet.compact .renewal-sheet-table th,
  .renewal-sheet.compact .renewal-sheet-table td { padding: ${isThree ? "1.4px 2px" : "2.4px 3px"}; }
  .renewal-sheet.compact .renewal-sheet-table th { font-size: ${isThree ? "5.4pt" : "7.1pt"}; }
  .renewal-sheet.compact .cycle-num { font-size: ${isThree ? "6.8pt" : "9.2pt"}; }
  .renewal-sheet.compact .cycle-unit { font-size: ${isThree ? "4.5pt" : "5.8pt"}; }
  .renewal-sheet.compact .ym-cell { font-size: ${isThree ? "5.1pt" : "7pt"}; }
  .renewal-sheet.compact .guide-block { padding: ${isThree ? "3px 4px" : "5px 6px"}; }
  .renewal-sheet.compact .guide-head { gap: ${isThree ? "3px" : "5px"}; }
  .renewal-sheet.compact .guide-mark { width: ${isThree ? "12px" : "15px"}; height: ${isThree ? "12px" : "15px"}; flex-basis: ${isThree ? "12px" : "15px"}; font-size: ${isThree ? "5.4pt" : "7pt"}; }
  .renewal-sheet.compact .guide-title { font-size: ${isThree ? "5.5pt" : "7.3pt"}; line-height: 1.15; }
  .renewal-sheet.compact .guide-title small { margin-left: 2px; }
  .renewal-sheet.compact .guide-headline { margin-top: 1px; font-size: ${isThree ? "4.8pt" : "6.4pt"}; line-height: 1.2; }
  .renewal-sheet.compact .guide-bullets { margin: ${isThree ? "2px 0 0 14px" : "3px 0 0 19px"}; padding-left: ${isThree ? "7px" : "9px"}; font-size: ${isThree ? "4.35pt" : "5.7pt"}; line-height: 1.2; }
  .renewal-sheet.compact .guide-bullets li + li { margin-top: 1px; }
  .renewal-sheet.compact .renewal-sheet-footer { padding: ${isThree ? "3px 3px 4px" : "4px 4px 5px"}; font-size: ${isThree ? "4.3pt" : "5.6pt"}; }
  .renewal-guide-visual-table .col-cycle { width: ${isThree ? "18%" : "19%"} !important; }
  .renewal-guide-visual-table .col-month { width: ${isThree ? "25%" : "27%"} !important; }
  .renewal-guide-visual-table .col-note { width: auto !important; }
</style>
</head>
<body>
  <div class="renewal-print-layout">${copies}</div>
  <script>
    window.addEventListener('load', () => {
      requestAnimationFrame(() => requestAnimationFrame(() => {
        setTimeout(() => { window.focus(); window.print(); }, 180);
      }));
    });
  <\/script>
</body>
</html>`);
  doc.close();

  const cleanup = () => setTimeout(() => iframe.remove(), 800);
  iframe.contentWindow.onafterprint = cleanup;
}

function attachEvents() {
  $$("#printManagerStats [data-manager-performance-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      const nextMode = button.dataset.managerPerformanceTab === "actual" ? "actual" : "assigned";
      if (managerPerformanceMode === nextMode) return;
      managerPerformanceMode = nextMode;
      renderDashboard();
    });
  });

  const sidebarRefreshBtn = $("#sidebarRefreshBtn");
  sidebarRefreshBtn?.addEventListener("click", () => window.location.reload());
  sidebarRefreshBtn?.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      window.location.reload();
    }
  });
  ["membershipStatusFilter", "membershipManagerFilter", "membershipContactFilter"].forEach((id) => {
    const control = $(`#${id}`);
    if (control) control.addEventListener("change", () => { renderMembershipRecords(); });
  });


  $("#evaluationMonthInput")?.addEventListener("change", renderManagementEvaluation);
  $("#renewalGuideMonthInput")?.addEventListener("change", renderRenewalGuide);
  $("#renewalGuideCurrentBtn")?.addEventListener("click", () => {
    const input = $("#renewalGuideMonthInput");
    if (input) input.value = monthIso();
    renderRenewalGuide();
  });
  $("#renewalGuidePrintBtn")?.addEventListener("click", printRenewalGuide);
  $("#evaluationPolicySettings")?.addEventListener("input", (event) => {
    const titleInput = event.target.closest(".evaluation-policy-title");
    if (!titleInput) return;
    const card = titleInput.closest(".evaluation-policy-card");
    const title = card?.querySelector(".evaluation-policy-card-title > strong");
    if (title) title.textContent = titleInput.value.trim() || "항목명을 입력하세요";
  });
  $("#evaluationSaveBtn")?.addEventListener("click", saveManagementEvaluationInput);
  $("#evaluationPrintBtn")?.addEventListener("click", printManagementEvaluation);
  $("#evaluationSavePolicyBtn")?.addEventListener("click", () => {
    collectManagementEvaluationPolicySettings();
    persistState();
    renderManagementEvaluation();
    showToast("선택한 월의 경영평가 기준을 저장했습니다.");
  });
  $("#evaluationCopyPreviousPolicyBtn")?.addEventListener("click", () => {
    const month = managementEvaluationMonth();
    const previousMonth = Object.keys(state.managementEvaluationPolicies || {})
      .filter((item) => /^\d{4}-\d{2}$/.test(item) && item < month)
      .sort()
      .pop();
    if (previousMonth) state.managementEvaluationPolicies[month] = normalizeManagementEvaluationPolicy(structuredClone(state.managementEvaluationPolicies[previousMonth]), month);
    else state.managementEvaluationPolicies[month] = defaultManagementEvaluationPolicy(month);
    persistState();
    renderManagementEvaluation();
    showToast(previousMonth ? `${formatMonthLabel(previousMonth)} 기준을 복사했습니다.` : "기본 기준을 불러왔습니다.");
  });
  $("#evaluationPolicySettings")?.addEventListener("click", (event) => {
    const month = managementEvaluationMonth();
    const addPolicy = event.target.closest("#addEvaluationPolicyItemBtn");
    const removePolicy = event.target.closest(".remove-evaluation-policy-item");
    const addProduct = event.target.closest("[data-add-evaluation-product-rule]");
    const removeProduct = event.target.closest(".remove-evaluation-product-rule");
    if (!addPolicy && !removePolicy && !addProduct && !removeProduct) return;
    const policy = collectManagementEvaluationPolicySettings();
    if (addPolicy) policy.policyItems.push(defaultManagementEvaluationPolicyItem("count"));
    if (removePolicy) {
      const id = removePolicy.closest("[data-evaluation-policy-id]")?.dataset.evaluationPolicyId;
      policy.policyItems = policy.policyItems.filter((item) => item.id !== id);
    }
    if (addProduct) {
      const type = addProduct.dataset.addEvaluationProductRule === "high" ? "high" : "primary";
      policy[type === "high" ? "highValueProducts" : "primaryProducts"].push(normalizeManagementEvaluationProductRule({ title: "제품군", keywords: [] }, type));
    }
    if (removeProduct) {
      const row = removeProduct.closest("[data-evaluation-product-id]");
      const type = row?.dataset.evaluationProductKind === "high" ? "high" : "primary";
      const key = type === "high" ? "highValueProducts" : "primaryProducts";
      policy[key] = policy[key].filter((item) => item.id !== row?.dataset.evaluationProductId);
    }
    state.managementEvaluationPolicies[month] = normalizeManagementEvaluationPolicy(policy, month);
    renderManagementEvaluation();
  });

  $$("[data-analysis-months]").forEach((button) => button.addEventListener("click", () => setAnalyticsRangePreset(button.dataset.analysisMonths)));
  ["analyticsStartMonth", "analyticsEndMonth", "analyticsManagerFilter"].forEach((id) => {
    $("#" + id)?.addEventListener("change", () => {
      if (id === "analyticsManagerFilter") analyticsSelectedManager = $("#analyticsManagerFilter")?.value || "";
      $$("[data-analysis-months]").forEach((button) => button.classList.remove("active"));
      renderAnalytics();
    });
  });
  $$("[data-analysis-tab]").forEach((button) => button.addEventListener("click", () => setAnalyticsTab(button.dataset.analysisTab)));
  $("#analyticsDetailManagerSelect")?.addEventListener("change", renderAnalytics);
  $("#analyticsView")?.addEventListener("click", (event) => {
    const combinedOpenButton = event.target.closest("#analyticsCombinedOpenBtn");
    const printButton = event.target.closest("[data-analytics-print]");
    const managerButton = event.target.closest("[data-analytics-manager-select]");
    const managerRow = event.target.closest("[data-analytics-manager-row]");
    const taskButton = event.target.closest("[data-analytics-task]");
    if (combinedOpenButton) {
      const managerName = $("#analyticsManagerFilter")?.value || "";
      if (!managerName) {
        showToast("통합보고서를 보려면 실제 판매자를 먼저 선택해 주세요.");
        return;
      }
      setAnalyticsTab("combined");
      renderAnalytics();
      return;
    }
    if (printButton) {
      if (printButton.disabled) return;
      printAnalyticsPanel(printButton.dataset.analyticsPrint, printButton.dataset.printTitle || "영업분석");
      return;
    }
    if (taskButton) {
      analyticsAddRecommendationTask(taskButton.dataset.analyticsTask);
      return;
    }
    const managerName = managerButton?.dataset.analyticsManagerSelect || managerRow?.dataset.analyticsManagerRow;
    if (managerName) {
      analyticsSelectedManager = managerName;
      if ($("#analyticsManagerFilter")) $("#analyticsManagerFilter").value = managerName;
      if ($("#analyticsDetailManagerSelect")) $("#analyticsDetailManagerSelect").value = managerName;
      setAnalyticsTab("managers");
      renderAnalytics();
    }
  });
  window.addEventListener("resize", () => {
    if (currentView === "analytics") window.requestAnimationFrame(drawAnalyticsTrendChart);
    syncOperatingGoalMobilePanel();
    syncDashboardSummaryMode();
    syncEvaluationPolicySettingsVisibility();
  });

  $$(".nav-item").forEach((item) => item.addEventListener("click", () => switchView(item.dataset.view)));
  attachMobileAppEvents();
  attachMobileFullMenuEvents();
  setMobileRecordTab($("#recordsView")?.dataset.mobileRecordTab || "main");
  syncMobileAppNav(currentView);
  syncMobileMenuVisibility();
  $("#payrollImportInput")?.addEventListener("change", (event) => { importPayrollFile(event.target.files?.[0]); event.target.value = ""; });
  $("#payrollSellerFilter")?.addEventListener("change", renderPayroll);
  $("#payrollManagerInput")?.addEventListener("change", (event) => { state.payrollManager = String(event.target.value || "").trim(); persistState({ immediateServer: true }); renderPayroll(); });
  $("#payrollMonthInput")?.addEventListener("change", (event) => { state.payrollMonth = String(event.target.value || "").trim(); persistState({ immediateServer: true }); renderPayroll(); });
  $("#payrollExportBtn")?.addEventListener("click", exportPayrollExcel);
  $("#payrollSaveBtn")?.addEventListener("click", savePayrollArchive);
  $("#payrollClearBtn")?.addEventListener("click", clearPayrollData);
  $("#payrollArchivesList")?.addEventListener("click", (event) => {
    const loadButton = event.target.closest(".payroll-archive-load");
    if (loadButton) { loadPayrollArchive(loadButton.dataset.payrollArchiveId); return; }
    const deleteButton = event.target.closest(".payroll-archive-delete");
    if (deleteButton) deletePayrollArchive(deleteButton.dataset.payrollArchiveId);
  });
  $("#checklistForm")?.addEventListener("submit", saveChecklistFromForm);
  $("#resetChecklistFormBtn")?.addEventListener("click", resetChecklistForm);
  $("#deleteChecklistBtn")?.addEventListener("click", deleteChecklistItem);
  $("#checklistTimeInput")?.addEventListener("input", setChecklistReminderEnabled);
  $("#checklistPrevMonthBtn")?.addEventListener("click", () => changeChecklistMonth(-1));
  $("#checklistNextMonthBtn")?.addEventListener("click", () => changeChecklistMonth(1));
  $("#checklistMonthInput")?.addEventListener("input", (event) => {
    if (/^\d{4}-\d{2}$/.test(event.target.value)) {
      checklistMonth = event.target.value;
      closeChecklistStatusModal();
      resetChecklistForm();
      renderChecklist();
    }
  });
  ["checklistSearchInput", "checklistStatusFilter", "checklistPriorityFilter"].forEach((id) => {
    $("#" + id)?.addEventListener(id === "checklistSearchInput" ? "input" : "change", renderChecklist);
  });
  $("#checklistSummaryGrid")?.addEventListener("click", (event) => {
    const card = event.target.closest("[data-checklist-summary]");
    if (card) openChecklistStatusModal(card.dataset.checklistSummary || "전체");
  });
  $("#checklistSummaryGrid")?.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    const card = event.target.closest("[data-checklist-summary]");
    if (card) {
      event.preventDefault();
      openChecklistStatusModal(card.dataset.checklistSummary || "전체");
    }
  });
  $("#closeChecklistStatusModalBtn")?.addEventListener("click", closeChecklistStatusModal);
  $("#closeChecklistStatusModalBtn2")?.addEventListener("click", closeChecklistStatusModal);
  $("#checklistStatusModal")?.addEventListener("click", (event) => {
    if (event.target?.id === "checklistStatusModal") closeChecklistStatusModal();
    const completeBtn = event.target.closest("[data-checklist-popup-complete]");
    if (completeBtn) updateChecklistStatus(completeBtn.dataset.checklistPopupComplete, "완료");
  });
  $("#checklistStatusModal")?.addEventListener("change", (event) => {
    const select = event.target.closest("[data-checklist-popup-status-id]");
    if (select) updateChecklistStatus(select.dataset.checklistPopupStatusId, select.value);
  });
  $("#closeChecklistAlarmBtn")?.addEventListener("click", closeChecklistAlarmModal);
  $("#completeChecklistAlarmBtn")?.addEventListener("click", completeChecklistAlarmItem);
  $("#checklistAlarmModal")?.addEventListener("click", (event) => {
    if (event.target?.id === "checklistAlarmModal") closeChecklistAlarmModal();
  });
  $("#checklistList")?.addEventListener("click", (event) => {
    const toggle = event.target.closest("[data-checklist-toggle]");
    if (toggle && !event.target.closest("button,select,input")) {
      const id = toggle.dataset.checklistToggle;
      const detail = document.querySelector(`[data-checklist-detail="${CSS.escape(id)}"]`);
      if (detail) detail.hidden = !detail.hidden;
      toggle.closest(".checklist-compact-item")?.classList.toggle("is-expanded", detail && !detail.hidden);
      return;
    }
    const editBtn = event.target.closest("[data-checklist-edit]");
    if (editBtn) {
      const item = state.checklistItems.find((entry) => entry.id === editBtn.dataset.checklistEdit);
      fillChecklistForm(item);
      return;
    }
    const completeBtn = event.target.closest("[data-checklist-complete]");
    if (completeBtn) updateChecklistStatus(completeBtn.dataset.checklistComplete, "완료");
  });
  $("#checklistList")?.addEventListener("change", (event) => {
    const select = event.target.closest("[data-checklist-status-id]");
    if (select) updateChecklistStatus(select.dataset.checklistStatusId, select.value);
  });
  $("#contactRequestForm")?.addEventListener("submit", saveContactRequestFromForm);
  $("#resetContactRequestFormBtn")?.addEventListener("click", resetContactRequestForm);
  $("#deleteContactRequestBtn")?.addEventListener("click", deleteContactRequest);
  $("#addContactRequestProgressBtn")?.addEventListener("click", () => addContactRequestProgressRow({date:todayIso(),content:""}));
  $("#contactRequestProgressRows")?.addEventListener("click", (event) => { const btn=event.target.closest(".contact-request-progress-remove"); if(!btn) return; btn.closest(".contact-progress-row")?.remove(); if(!$("#contactRequestProgressRows")?.children.length) addContactRequestProgressRow({date:todayIso(),content:""}); });
  $("#contactRequestList")?.addEventListener("click", (event) => {
    const edit=event.target.closest("[data-contact-request-edit]"); if(edit){ fillContactRequestForm((state.contactRequests||[]).find(x=>x.id===edit.dataset.contactRequestEdit)); return; }
    const hist=event.target.closest("[data-contact-request-history]"); if(hist){ const box=document.querySelector(`[data-contact-request-history-box="${CSS.escape(hist.dataset.contactRequestHistory)}"]`); if(box) box.hidden=!box.hidden; }
  });

  $("#contactNoteForm")?.addEventListener("submit", saveContactNoteFromForm);
  $("#resetContactNoteFormBtn")?.addEventListener("click", resetContactNoteForm);
  $("#deleteContactNoteBtn")?.addEventListener("click", deleteContactNote);
  $("#newContactNoteBtn")?.addEventListener("click", () => $("#contactNoteImportInput")?.click());
  $("#addContactProgressBtn")?.addEventListener("click", () => addContactProgressRow({ date: todayIso(), content: "" }));
  $("#contactProgressRows")?.addEventListener("click", (event) => {
    const btn = event.target.closest(".contact-progress-remove"); if (!btn) return;
    btn.closest(".contact-progress-row")?.remove();
    if (!$("#contactProgressRows")?.children.length) addContactProgressRow({ date: todayIso(), content: "" });
  });
  ["contactNoteStatusFilter","contactNoteManagerFilter"].forEach(id => $("#"+id)?.addEventListener(id === "contactNoteSearchInput" ? "input" : "change", renderContactNotes));
  ["contactHeaderStatusFilter","contactHeaderManagerFilter","contactHeaderCustomerNoFilter","contactHeaderProductFilter","contactHeaderCustomerFilter","contactHeaderContactPhoneFilter","contactHeaderAddressFilter","contactHeaderMemoFilter","contactHeaderProgressFilter"].forEach(id => $("#"+id)?.addEventListener(id.includes("Status") || id.includes("Manager") ? "change" : "input", renderContactNotes));
  $("#contactNoteTableBody")?.addEventListener("click", (event) => {
    const saveProgressBtn = event.target.closest("[data-contact-progress-save]");
    if (saveProgressBtn) { event.stopPropagation(); addInlineContactProgress(saveProgressBtn.dataset.contactProgressSave); return; }
    const historyBtn = event.target.closest("[data-contact-history-toggle]");
    if (historyBtn) { event.stopPropagation(); const detail=document.querySelector(`[data-contact-note-detail="${CSS.escape(historyBtn.dataset.contactHistoryToggle)}"]`); if(detail) detail.hidden=!detail.hidden; return; }
    const editBtn = event.target.closest("[data-contact-edit]");
    if (editBtn) { const note = state.contactNotes.find(item => item.id === editBtn.dataset.contactEdit); fillContactNoteForm(note); return; }
    if (event.target.closest("input, textarea, select, button")) return;
    const row = event.target.closest(".contact-note-main-row"); if (!row) return;
    const detail = document.querySelector(`[data-contact-note-detail="${CSS.escape(row.dataset.contactNoteId)}"]`); if (detail) detail.hidden = !detail.hidden;
  });
  $("#contactNoteTableBody")?.addEventListener("change", (event) => {
    const statusSelect = event.target.closest("[data-contact-status-id]");
    if (statusSelect) { saveInlineContactStatus(statusSelect.dataset.contactStatusId, statusSelect.value); return; }
    const phoneInput = event.target.closest("[data-contact-phone-id]");
    if (phoneInput) { saveInlineContactPhone(phoneInput.dataset.contactPhoneId, phoneInput.value); return; }
    const memoInput = event.target.closest("[data-contact-memo-id]");
    if (memoInput) { saveInlineContactMemo(memoInput.dataset.contactMemoId, memoInput.value); return; }
  });
  $("#contactNoteImportInput")?.addEventListener("change", async (event) => { const file = event.target.files?.[0]; await importContactNoteFile(file); event.target.value = ""; });

  $("#openNewRecord")?.addEventListener("click", () => {
    resetRecordForm();
    switchView("records");
  });
  $("#operatingGoalPanel")?.addEventListener("click", (event) => {
    const rateButton = event.target.closest("[data-operating-goal-rate]");
    if (rateButton) {
      const rate = Number(rateButton.dataset.operatingGoalRate);
      if ($("#operatingGoalRateInput")) $("#operatingGoalRateInput").value = rate;
      updateOperatingGoalPreview();
      return;
    }
  });
  $("#operatingGoalRateInput")?.addEventListener("input", updateOperatingGoalPreview);
  $("#saveOperatingGoalBtn")?.addEventListener("click", () => saveOperatingGoal());
  $("#resetOperatingGoalBtn")?.addEventListener("click", () => {
    if ($("#operatingGoalRateInput")) $("#operatingGoalRateInput").value = 100;
    saveOperatingGoal(100, "100% 기준목표로 복원");
  });
  $("#shareKakaoBtn")?.addEventListener("click", shareKakaoImage);
  $("#managerPerformancePrintBtn")?.addEventListener("click", printCurrentManagerPerformance);
  $("#printDashboardBtn").addEventListener("click", printDashboard);
  $("#saveCustomCardsBtn")?.addEventListener("click", () => {
    collectCustomDashboardCards();
    persistState();
    renderDashboard();
    renderCustomDashboardCardSettings();
    showToast("대시보드 조건카드를 저장했습니다.");
  });
  $("#resetCustomCardsBtn")?.addEventListener("click", resetCustomDashboardCards);
  $("#saveAnalyticsSettingsBtn")?.addEventListener("click", saveAnalyticsSettings);
  $("#saveMenuVisibilityBtn")?.addEventListener("click", saveMenuVisibilitySettings);
  $$('input[name="analyticsStartMode"], input[name="analyticsMonthStatusMode"]').forEach((node) => node.addEventListener("change", () => {
    syncAnalyticsGuidedControls();
    if (node.name === "analyticsMonthStatusMode") renderAnalyticsMonthStatusRows(analyticsSettings());
  }));
  $("#analyticsBranchStartMonthInput")?.addEventListener("change", () => {
    if ((document.querySelector('input[name="analyticsMonthStatusMode"]:checked')?.value || "auto") === "manual") {
      const temp = { ...analyticsSettings(), branchStartMode: "manual", branchStartMonth: $("#analyticsBranchStartMonthInput")?.value || "" };
      renderAnalyticsMonthStatusRows(temp);
    }
  });
  $("#addAnalyticsAliasRuleBtn")?.addEventListener("click", () => {
    const settings = analyticsSettings();
    settings.sellerAliases = [...settings.sellerAliases, { source: "", target: "", startMonth: monthIso() }];
    renderAnalyticsAliasRows(settings);
  });
  $("#analyticsSellerAliasRows")?.addEventListener("click", (event) => {
    const button = event.target.closest(".analytics-remove-alias");
    if (!button) return;
    const row = button.closest(".analytics-alias-row");
    if (row) row.remove();
    if (!$("#analyticsSellerAliasRows")?.querySelector(".analytics-alias-row")) $("#analyticsSellerAliasRows").innerHTML = `<div class="analytics-empty-choice">통합할 판매자가 없으면 그대로 두면 됩니다.</div>`;
  });
  $("#analyticsSellerStartRows")?.addEventListener("change", (event) => {
    if (!event.target.matches(".analytics-seller-start-mode")) return;
    const row = event.target.closest(".analytics-seller-start-row");
    const monthInput = row?.querySelector(".analytics-seller-start-month");
    if (monthInput) {
      monthInput.disabled = event.target.value !== "manual";
      if (event.target.value === "manual" && !monthInput.value) monthInput.value = monthIso();
    }
  });
  $("#toggleAnalyticsProductAddBtn")?.addEventListener("click", () => {
    const panel = $("#analyticsProductAddPanel");
    if (panel) panel.hidden = !panel.hidden;
  });
  $("#analyticsProductAddPanel")?.addEventListener("click", (event) => {
    const templateButton = event.target.closest("[data-product-template]");
    if (templateButton) {
      const family = templateButton.dataset.productTemplate || "";
      const rule = analyticsProductCatalogRule(family);
      if (rule) appendAnalyticsProductRule({ family: rule.family, terms: [...rule.terms] });
      return;
    }
    if (event.target.closest("#addAnalyticsCustomProductBtn")) {
      const family = String($("#analyticsCustomProductFamilyInput")?.value || "").trim();
      const term = String($("#analyticsCustomProductTermInput")?.value || "").trim();
      if (!family || !term) {
        showToast("제품군 이름과 첫 분류기준을 입력해 주세요.");
        return;
      }
      appendAnalyticsProductRule({ family, terms: [term] });
      if ($("#analyticsCustomProductFamilyInput")) $("#analyticsCustomProductFamilyInput").value = "";
      if ($("#analyticsCustomProductTermInput")) $("#analyticsCustomProductTermInput").value = "";
    }
  });
  $("#analyticsProductRuleChoices")?.addEventListener("click", (event) => {
    const row = event.target.closest(".analytics-product-rule-row");
    if (!row) return;
    if (event.target.closest(".analytics-product-delete-btn")) {
      row.remove();
      if (!$("#analyticsProductRuleChoices")?.querySelector(".analytics-product-rule-row")) {
        $("#analyticsProductRuleChoices").innerHTML = `<div class="analytics-empty-choice analytics-product-empty">사용할 제품군을 위의 <b>제품군 추가</b>에서 선택해 주세요.</div>`;
      }
      return;
    }
    if (event.target.closest(".analytics-product-edit-btn")) {
      const editor = row.querySelector(".analytics-product-rule-editor");
      if (editor) editor.hidden = false;
      return;
    }
    if (event.target.closest(".analytics-product-edit-done-btn")) {
      updateAnalyticsProductRuleSummary(row);
      const editor = row.querySelector(".analytics-product-rule-editor");
      if (editor) editor.hidden = true;
      return;
    }
    if (event.target.closest(".analytics-product-add-term-btn")) {
      const input = row.querySelector(".analytics-product-new-term");
      const value = String(input?.value || "").trim();
      if (!value) return;
      const options = row.querySelector(".analytics-product-term-options");
      const exists = [...(options?.querySelectorAll("input[type='checkbox']") || [])].some((node) => String(node.value || "").trim().toUpperCase() === value.toUpperCase());
      if (!exists && options) options.insertAdjacentHTML("beforeend", `<label class="analytics-check-chip"><input type="checkbox" value="${escapeHtml(value)}" checked><span>${escapeHtml(value)}</span></label>`);
      if (input) input.value = "";
    }
  });
  $("#printManagerStats")?.addEventListener("click", (event) => {
    const button = event.target.closest("[data-manager-share]");
    if (!button) return;
    event.preventDefault();
    event.stopPropagation();
    shareManagerKakaoImage(button.dataset.managerShare);
  });
  $("#managerShareCloseBtn")?.addEventListener("click", closeManagerShareModal);
  $("#managerShareCancelBtn")?.addEventListener("click", closeManagerShareModal);
  $("#managerShareCopyBtn")?.addEventListener("click", copyCurrentManagerShareImage);
  $("#managerShareSaveBtn")?.addEventListener("click", saveCurrentManagerShareImage);
  $("#managerShareKakaoBtn")?.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    const button = event.currentTarget;
    if (button?.dataset.actionMode === "print") printCurrentSharePreview();
    else kakaoShareCurrentManagerImage();
  });
  $("#managerShareModal")?.addEventListener("click", (event) => {
    if (event.target?.id === "managerShareModal") closeManagerShareModal();
  });
  $("#exportBackupBtn")?.addEventListener("click", exportFullBackup);
  $("#completeResetBtn")?.addEventListener("click", openCompleteResetModal);
  $("#completeResetCancelBtn")?.addEventListener("click", closeCompleteResetModal);
  $("#completeResetBackupBtn")?.addEventListener("click", exportFullBackup);
  $("#completeResetBackupConfirm")?.addEventListener("change", (event) => { const btn = $("#completeResetExecuteBtn"); if (btn) btn.disabled = !event.target.checked; });
  $("#completeResetExecuteBtn")?.addEventListener("click", executeCompleteReset);
  $("#completeResetModal")?.addEventListener("click", (event) => { if (event.target.id === "completeResetModal") closeCompleteResetModal(); });
  // 전체 백업 불러오기는 native label/input 방식으로 처리합니다.
  attachDbManagementEvents();

  ["startDateFilter", "endDateFilter", "managerFilter"].forEach((id) => {
    const control = $(`#${id}`);
    if (control) control.addEventListener("input", render);
  });
  $("#globalSearch")?.addEventListener("input", debounce(render, 120));

  const applyDashboardMonth = (month) => {
    $("#monthFilter").value = month;
  const renewalGuideMonthInput = $("#renewalGuideMonthInput");
  if (renewalGuideMonthInput) renewalGuideMonthInput.value = month;
    const period = monthPeriod(month);
    setDashboardRange(period.start, dashboardDefaultEnd(period));
    const dayFilter = $("#dayFilter");
    if (dayFilter) dayFilter.value = "";
    if (!settingsEditMode.goal) renderGoalSettingsForMonth(month);
    manualStatsForMonth(month);
    render();
  };

  $("#monthFilter").addEventListener("input", () => {
    applyDashboardMonth($("#monthFilter").value);
  });

  $("#calendarPrevMonthBtn")?.addEventListener("click", () => {
    applyDashboardMonth(shiftMonth($("#monthFilter").value || monthIso(), -1));
  });

  $("#calendarNextMonthBtn")?.addEventListener("click", () => {
    applyDashboardMonth(shiftMonth($("#monthFilter").value || monthIso(), 1));
  });

  const dayFilter = $("#dayFilter");
  if (dayFilter) {
    dayFilter.addEventListener("input", () => {
      const day = dayFilter.value;
      if (!day) return;
      $("#startDateFilter").value = day;
      $("#endDateFilter").value = day;
      render();
    });
  }

  const dashboardCalendar = $("#dashboardCalendar");
  if (dashboardCalendar) {
    const selectCalendarDate = (date) => {
      setDashboardRange(date, date);
      render();
    };
    const selectCalendarRange = (start, end) => {
      const from = start < end ? start : end;
      const to = start < end ? end : start;
      setDashboardRange(from, to);
      render();
    };
    dashboardCalendar.addEventListener("pointerdown", (event) => {
      const button = event.target.closest("[data-date]");
      if (!button) return;
      calendarDragStart = button.dataset.date;
      calendarDragEnd = button.dataset.date;
      dashboardCalendar.setPointerCapture?.(event.pointerId);
      event.preventDefault();
    });
    const dateFromPointer = (event) => document.elementFromPoint(event.clientX, event.clientY)?.closest?.("[data-date]")?.dataset.date || "";
    dashboardCalendar.addEventListener("pointermove", (event) => {
      if (!calendarDragStart) return;
      calendarDragEnd = dateFromPointer(event) || calendarDragEnd;
    });
    dashboardCalendar.addEventListener("pointerup", (event) => {
      calendarDragEnd = dateFromPointer(event) || calendarDragEnd;
      if (!calendarDragStart) return;
      if (calendarDragStart === calendarDragEnd) selectCalendarDate(calendarDragStart);
      else selectCalendarRange(calendarDragStart, calendarDragEnd);
      calendarDragStart = "";
      calendarDragEnd = "";
      dashboardCalendar.releasePointerCapture?.(event.pointerId);
    });
    dashboardCalendar.addEventListener("pointercancel", () => {
      calendarDragStart = "";
      calendarDragEnd = "";
    });
    dashboardCalendar.addEventListener("mousedown", (event) => {
      const button = event.target.closest("[data-date]");
      if (!button) return;
      calendarDragStart = button.dataset.date;
      calendarDragEnd = button.dataset.date;
      event.preventDefault();
    });
    dashboardCalendar.addEventListener("mouseover", (event) => {
      if (!calendarDragStart) return;
      const button = event.target.closest("[data-date]");
      if (button) calendarDragEnd = button.dataset.date;
    });
    window.addEventListener("mouseup", () => {
      if (!calendarDragStart) return;
      if (calendarDragStart === calendarDragEnd) selectCalendarDate(calendarDragStart);
      else selectCalendarRange(calendarDragStart, calendarDragEnd);
      calendarDragStart = "";
      calendarDragEnd = "";
    });
  }

  $("#currentRangeBtn").addEventListener("click", () => {
    const period = monthPeriod($("#monthFilter").value);
    setDashboardRange(period.start, dashboardDefaultEnd(period));
    render();
  });

  $("#fullRangeBtn").addEventListener("click", () => {
    const period = monthPeriod($("#monthFilter").value);
    setDashboardRange(period.start, period.end);
    render();
  });

  $("#editUserSettingsBtn").addEventListener("click", () => {
    unlockSettingsSection("user");
    showToast("사용자 설정을 수정할 수 있습니다.");
  });

  $("#saveUserSettingsBtn").addEventListener("click", () => {
    collectUserSettings();
    lockSettingsSection("user");
    saveState("사용자 설정을 저장했습니다.");
  });

  $("#editGoalSettingsBtn").addEventListener("click", () => {
    unlockSettingsSection("goal");
    showToast("월 목표지수를 수정할 수 있습니다.");
  });

  $("#goalMonthInput")?.addEventListener("change", () => {
    renderGoalSettingsForMonth($("#goalMonthInput").value || currentDashboardMonth());
    render();
  });

  $("#periodEndInput")?.addEventListener("change", () => {
    syncGoalMonthFromPeriodEnd();
  });

  $("#saveGoalSettingsBtn").addEventListener("click", () => {
    const savedMonth = goalSettingsMonth();
    collectGoalSettings();
    lockSettingsSection("goal");
    saveState(`${savedMonth} 월 목표지수를 저장했습니다. 기존 월 실적은 유지됩니다.`);
  });

  $("#teamOperationEffectiveMonth")?.addEventListener("change", () => {
    renderTeamOperationSettings();
    renderSettings();
  });
  $("#teamOperationSingleBtn")?.addEventListener("click", () => {
    setTeamOperationMode("1", teamOperationSettingsMonth());
  });
  $("#teamOperationDualBtn")?.addEventListener("click", () => {
    const effectiveMonth = teamOperationSettingsMonth();
    setTeamOperationMode("2", effectiveMonth);
    const userTeamMonth = $("#masterTeamEffectiveMonth");
    if (userTeamMonth) userTeamMonth.value = effectiveMonth;
  });


  $("#receivedDateInput")?.addEventListener("change", () => {
    const selectedManager = $("#managerInput")?.value || "";
    const selectedSeller = $("#sellerInput")?.value || "";
    refreshRecordManagerOptions(
      goalMonthForDate($("#receivedDateInput").value || "", $("#monthFilter")?.value || monthIso()),
      selectedManager,
      Boolean($("#recordId")?.value)
    );
    updateSellerInputOptions(selectedSeller);
    showToast(`${recordEntryMonth()} 적용 매니저 목록으로 변경했습니다.`);
  });

  $("#categoryInput").addEventListener("change", () => {
    updateSellerInputOptions("");
  });

  $("#printManagerStats")?.addEventListener("click", (event) => {
    const button = event.target.closest("[data-cons-manage]");
    if (!button) return;
    openConsPaymentManager(button.dataset.consManage || "");
  });

  $("#printManagerStats")?.addEventListener("change", (event) => {
    const input = event.target.closest(".manager-inline-input");
    if (!input) return;
    const managerName = input.dataset.manager;
    const field = input.dataset.field;
    const stat = manualStatFor(managerName);
    const labels = { renewal: "재약정", consPaid: "컨스 지급완료", consMemo: "컨스 지급메모", refund: "환수" };

    if (field === "consMemo") {
      stat.consMemo = String(input.value || "");
      persistState();
      renderDashboard();
      showToast(`${managerName} ${labels[field]}을 저장했습니다.`);
      return;
    }

    const value = input.value === "" ? 0 : Number(input.value);
    if (!Number.isFinite(value) || value < 0) {
      showToast("0 이상의 숫자만 입력할 수 있습니다.");
      input.value = stat[field] ? stat[field] : "";
      return;
    }

    stat[field] = value;
    persistState();
    renderDashboard();
    showToast(`${managerName} ${labels[field]} 값을 저장했습니다.`);
  });

  $("#recordForm").addEventListener("submit", (event) => {
    event.preventDefault();
    const currentRecordId = $("#recordId").value;
    const existingRecord = state.records.find((item) => item.id === currentRecordId);
    const selectedManager = String($("#managerInput")?.value || "").trim();
    const receivedMonth = recordEntryMonth();
    const activeManagers = activeTeamManagerNames(receivedMonth);
    const preservesHistoricalManager = Boolean(existingRecord && existingRecord.manager === selectedManager);

    if (!selectedManager || (!activeManagers.includes(selectedManager) && !preservesHistoricalManager)) {
      refreshRecordManagerOptions(receivedMonth, selectedManager, preservesHistoricalManager);
      showToast(`${receivedMonth} 적용 매니저를 선택해 주세요.`);
      return;
    }

    const id = currentRecordId || uid("r");
    const nowIso = new Date().toISOString();
    const record = {
      id,
      status: $("#statusInput").value,
      receivedDate: $("#receivedDateInput").value,
      installDate: $("#installDateInput").value,
      manager: $("#managerInput").value,
      managerId: managerByName($("#managerInput").value)?.id || existingRecord?.managerId || "",
      managerNameAtRecord: existingRecord?.managerNameAtRecord || $("#managerInput").value,
      managerTeamAtRecord: existingRecord?.managerTeamAtRecord || managerTeamForMonth($("#managerInput").value, receivedMonth),
      managerStatusAtRecord: existingRecord?.managerStatusAtRecord || managerStatusForMonth($("#managerInput").value, receivedMonth),
      count: toNumber($("#countInput").value),
      previousCustomer: $("#previousCustomerInput").value.trim(),
      customerNo: $("#customerInput").value.trim(),
      phone: formatPhoneNumber($("#phoneInput").value),
      customerName: $("#customerNameInput").value.trim(),
      category: $("#categoryInput").value,
      activityType: normalizeActivityType($("#activityTypeInput")?.value || ""),
      qr: $("#qrInput").value.trim(),
      cashAmount: toNumber($("#cashAmountInput").value),
      product: $("#productInput").value.trim(),
      seller: $("#sellerInput").value,
      sellerId: managerByName($("#sellerInput").value)?.id || existingRecord?.sellerId || "",
      sellerNameAtRecord: existingRecord?.sellerNameAtRecord || $("#sellerInput").value,
      memo: $("#memoInput").value.trim(),
      createdAt: existingRecord?.createdAt || nowIso,
      updatedAt: nowIso
    };
    refreshEditedRecordReferences(record, existingRecord);
    const index = state.records.findIndex((item) => item.id === id);
    if (index >= 0) state.records[index] = record;
    else state.records.unshift(record);
    resetRecordForm();
    saveState("접수 내역을 저장했습니다.");
  });

  $("#resetRecordForm").addEventListener("click", resetRecordForm);
  $("#deleteRecordBtn").addEventListener("click", () => {
    const id = $("#recordId").value;
    if (!id) return;
    state.records = state.records.filter((record) => record.id !== id);
    resetRecordForm();
    saveState("접수 내역을 삭제했습니다.");
  });

  $("#recordTableBody").addEventListener("click", (event) => {
    const checkbox = event.target.closest(".record-select-checkbox");
    if (checkbox) {
      event.stopPropagation();
      return;
    }
    const row = event.target.closest("[data-record-id]");
    if (!row) return;

    const seqButton = event.target.closest(".seq-number-button");
    if (seqButton) {
      $$("#recordTableBody tr").forEach((item) => {
        if (item !== row) item.classList.remove("show-row-move");
      });
      row.classList.toggle("show-row-move");
      selectedRecordId = row.dataset.recordId;
      $$("#recordTableBody tr").forEach((item) => item.classList.remove("selected-record-row"));
      row.classList.add("selected-record-row");
      const record = state.records.find((item) => item.id === row.dataset.recordId);
      if (record) fillRecordForm(record);
      event.stopPropagation();
      return;
    }

    const moveButton = event.target.closest("[data-move]");
    if (moveButton) {
      moveRecordInCurrentView(row.dataset.recordId, moveButton.dataset.move === "up" ? -1 : 1);
      event.stopPropagation();
      return;
    }

    selectedRecordId = row.dataset.recordId;
    $$("#recordTableBody tr").forEach((item) => item.classList.remove("selected-record-row"));
    row.classList.add("selected-record-row");
    const record = state.records.find((item) => item.id === row.dataset.recordId);
    if (record) fillRecordForm(record);

    const cell = event.target.closest("td[data-edit-type]");
    if (!cell) return;
    if (event.target.closest("input, select, textarea, button")) return;
    enterRecordCellEdit(cell);
  });

  $("#recordTableBody").addEventListener("change", (event) => {
    const checkbox = event.target.closest(".record-select-checkbox");
    if (!checkbox) return;
    const key = String(checkbox.dataset.recordSelect || "");
    if (!key) return;
    if (checkbox.checked) selectedRecordIds.add(key);
    else selectedRecordIds.delete(key);
    updateBulkRecordDeleteUI(visibleRecordsForCurrentFilters());
  });

  $("#selectAllRecords")?.addEventListener("change", (event) => {
    const visibleRecords = visibleRecordsForCurrentFilters();
    visibleRecords.forEach((record) => {
      const key = recordSelectionKey(record);
      if (event.target.checked) selectedRecordIds.add(key);
      else selectedRecordIds.delete(key);
    });
    renderRecords();
  });

  $("#bulkDeleteRecordsBtn")?.addEventListener("click", deleteSelectedRecords);

  $("#recordTableBody").addEventListener("keydown", (event) => {
    const cell = event.target.closest("td.editing-cell");
    if (!cell) return;
    if (event.key === "Escape") {
      cancelRecordCellEdit(cell);
      renderRecords();
      event.preventDefault();
      return;
    }
    if (event.key === "Enter" && !event.shiftKey && event.target.tagName !== "TEXTAREA") {
      saveRecordCellEdit(cell);
      event.preventDefault();
    }
  });

  $("#recordTableBody").addEventListener("change", (event) => {
    const cell = event.target.closest("td.editing-cell");
    if (!cell) return;
    if (event.target.tagName === "SELECT") saveRecordCellEdit(cell);
  });

  $("#recordTableBody").addEventListener("focusout", (event) => {
    const cell = event.target.closest("td.editing-cell");
    if (!cell) return;
    window.setTimeout(() => {
      if (!cell.contains(document.activeElement)) saveRecordCellEdit(cell);
    }, 0);
  });


  $("#membershipTableBody")?.addEventListener("click", (event) => {
    const row = event.target.closest("[data-record-id]");
    if (!row) return;

    const seqButton = event.target.closest(".seq-number-button");
    if (seqButton) {
      $$("#membershipTableBody tr").forEach((item) => {
        if (item !== row) item.classList.remove("show-row-move");
      });
      row.classList.toggle("show-row-move");
      event.stopPropagation();
      return;
    }

    const moveButton = event.target.closest("[data-move]");
    if (moveButton) {
      moveMembershipRecordInCurrentView(row.dataset.recordId, moveButton.dataset.move === "up" ? -1 : 1);
      event.stopPropagation();
      return;
    }

    const record = state.records.find((item) => item.id === row.dataset.recordId || promoRecordKey(item) === row.dataset.recordId);
    if (!record) return;
    selectedRecordId = "";
    fillRecordForm(record);

    const cell = event.target.closest("td[data-edit-type]");
    if (!cell) return;
    if (event.target.closest("input, select, textarea, button")) return;
    enterRecordCellEdit(cell);
  });

  $("#membershipTableBody")?.addEventListener("keydown", (event) => {
    const cell = event.target.closest("td.editing-cell");
    if (!cell) return;
    if (event.key === "Escape") {
      cancelRecordCellEdit(cell);
      renderMembershipRecords();
      event.preventDefault();
      return;
    }
    if (event.key === "Enter" && !event.shiftKey && event.target.tagName !== "TEXTAREA") {
      saveRecordCellEdit(cell);
      event.preventDefault();
    }
  });

  $("#membershipTableBody")?.addEventListener("change", (event) => {
    const cell = event.target.closest("td.editing-cell");
    if (!cell) return;
    if (event.target.tagName === "SELECT") saveRecordCellEdit(cell);
  });

  $("#membershipTableBody")?.addEventListener("focusout", (event) => {
    const cell = event.target.closest("td.editing-cell");
    if (!cell) return;
    window.setTimeout(() => {
      if (!cell.contains(document.activeElement)) saveRecordCellEdit(cell);
    }, 0);
  });

  $("#promoForm").addEventListener("submit", (event) => {
    event.preventDefault();
    syncPromoDateMode();
    const id = $("#promoId").value || uid("p");
    const promo = normalizePromotion({
      id,
      name: $("#promoNameInput").value.trim(),
      startDate: $("#promoStartInput").value,
      endDate: $("#promoEndInput").value,
      type: $("#promoTypeInput").value || "count",
      countRules: collectCountRuleRows(),
      scoreRules: collectScoreRuleRows(),
      scoreRewardRules: collectScoreRewardRows(),
      productRules: collectProductRuleRows(),
      memo: $("#promoMemoInput").value.trim(),
      includePendingRecords: normalizePromotion(state.promotions.find((item) => item.id === id) || {}).includePendingRecords || {}
    });
    const index = state.promotions.findIndex((item) => item.id === id);
    if (index >= 0) state.promotions[index] = promo;
    else state.promotions.unshift(promo);
    fillPromoForm(promo);
    saveState("프로모션을 저장했습니다.");
  });

  $("#newPromotionBtn")?.addEventListener("click", resetPromoForm);
  $("#promoMonthFilter")?.addEventListener("change", () => {
    promoListFilter = "all";
    $$(".promo-filter-tab").forEach((node) => node.classList.toggle("active", node.dataset.promoFilter === "all"));
    renderPromotions();
    showToast(`프로모션 조회월을 ${formatMonthLabel(currentPromotionMonth())}로 변경했습니다.`);
  });
  $("#printPromotionMonthlyReportBtn")?.addEventListener("click", printPromotionMonthlyReport);
  $$(".promo-filter-tab").forEach((button) => {
    button.addEventListener("click", () => {
      promoListFilter = button.dataset.promoFilter || "all";
      $$(".promo-filter-tab").forEach((node) => node.classList.toggle("active", node === button));
      renderPromotions();
    });
  });
  $$(".promo-type-btn").forEach((button) => {
    button.addEventListener("click", () => setPromoType(button.dataset.promoType || "count"));
  });
  $$("input[name='promoDateMode']").forEach((input) => input.addEventListener("change", syncPromoDateMode));
  $("#promoStartInput")?.addEventListener("change", syncPromoDateMode);
  $("#addCountRuleBtn")?.addEventListener("click", () => renderCountRuleRows([...collectCountRuleRows(), { threshold: 1, reward: "", quantity: 1 }]));
  $("#addScoreRuleBtn")?.addEventListener("click", () => renderScoreRuleRows([...collectScoreRuleRows(), { title: "", keyword: "", keywords: [], excludeKeyword: "", excludeKeywords: [], score: 1 }]));
  $("#addScoreRewardBtn")?.addEventListener("click", () => renderScoreRewardRows([...collectScoreRewardRows(), { threshold: 1, reward: "", quantity: 1 }]));
  $("#addProductRuleBtn")?.addEventListener("click", () => renderProductRuleRows([...collectProductRuleRows(), { title: "", keyword: "", keywords: [], reward: "", quantity: 1 }]));
  ["#promoCountRuleRows", "#promoScoreRuleRows", "#promoScoreRewardRows", "#promoProductRuleRows"].forEach((selector) => {
    const node = $(selector);
    if (!node) return;
    node.addEventListener("click", (event) => {
      if (!event.target.classList.contains("remove-promo-rule")) return;
      event.target.closest(".promo-rule-row")?.remove();
      renderPromotionDetail();
    });
    node.addEventListener("input", renderPromotionDetail);
    node.addEventListener("change", renderPromotionDetail);
  });
  $("#promoDetailRecordBody")?.addEventListener("change", (event) => {
    const target = event.target.closest?.(".promo-record-include-toggle");
    if (!target) return;
    const promo = activePromotion();
    const recordId = target.dataset.recordId;
    if (!promo.id || !recordId) {
      showToast("인정/미인정 저장에 필요한 접수건 식별값을 찾지 못했습니다.");
      return;
    }
    const selectedManager = $("#promoDetailManagerSelect")?.value || "";
    const index = state.promotions.findIndex((item) => item.id === promo.id);
    if (index < 0) return;
    state.promotions[index] = normalizePromotion(state.promotions[index]);
    state.promotions[index].includePendingRecords = state.promotions[index].includePendingRecords || {};
    state.promotions[index].includePendingRecords[recordId] = target.value === "yes";
    persistState();
    renderPromotions();
    const managerSelect = $("#promoDetailManagerSelect");
    if (managerSelect && selectedManager) {
      managerSelect.value = selectedManager;
      renderPromotionManagerDetail(activePromotion());
    }
    showToast("인정/미인정 선택을 저장했습니다.");
  });
  $("#promoDetailManagerSelect")?.addEventListener("change", () => renderPromotionManagerDetail());


  $("#resetPromoForm").addEventListener("click", resetPromoForm);
  $("#deletePromoBtn").addEventListener("click", () => {
    const id = $("#promoId").value;
    if (!id) return;
    state.promotions = state.promotions.filter((promo) => promo.id !== id);
    resetPromoForm();
    saveState("프로모션을 삭제했습니다.");
  });

  $("#promoList").addEventListener("click", (event) => {
    const card = event.target.closest("[data-promo-id]");
    if (!card) return;
    const promo = state.promotions.find((item) => item.id === card.dataset.promoId);
    if (promo) fillPromoForm(promo);
  });

  attachManagerTimelineEvents();

  $("#editManagerSettingsBtn").addEventListener("click", () => {
    unlockSettingsSection("manager");
    refreshManagerOrderNumbers();
    showToast("매니저 정보와 노출순번을 수정할 수 있습니다.");
  });

  $("#addManagerBtn").addEventListener("click", () => {
    if (!settingsEditMode.manager) return;
    const list = $("#managerSettings");
    const targetMonth = $("#goalMonthInput")?.value || $("#monthFilter").value || monthIso();
    const manager = normalizeManager({
      id: uid("m"),
      name: "",
      team: defaultTeamName(),
      goal: 0,
      status: "active",
      joinedMonth: targetMonth,
      teamHistory: [{ team: defaultTeamName(), startMonth: targetMonth, endMonth: "" }]
    });
    manager.displayOrder = list.querySelectorAll(".manager-row").length + 1;
    list.insertAdjacentHTML("beforeend", managerSettingsRowMarkup(manager, targetMonth, true));
    setSettingsSectionEditable("manager", true);
    refreshManagerOrderNumbers();
    list.lastElementChild?.querySelector(".manager-name")?.focus();
  });

  $("#saveManagerSettingsBtn").addEventListener("click", () => {
    if (!collectManagerSettings()) return;
    lockSettingsSection("manager");
    invalidateManagerCaches();
    saveState("매니저 기본정보와 재직·소속 이력을 저장했습니다.");
  });

  $("#managerSettings").addEventListener("change", (event) => {
    const row = event.target.closest(".manager-row");
    if (!row || !settingsEditMode.manager) return;
    const type = row.querySelector(".manager-change-type");
    const status = row.querySelector(".manager-status");
    if (!type || !status) return;
    if (event.target === type) {
      if (type.value === "leave" || type.value === "pause") status.value = "inactive";
      if (type.value === "return") status.value = "active";
    } else if (event.target.classList.contains("manager-team") && type.value === "none" && row.dataset.isNew !== "true") {
      type.value = "move";
    } else if (event.target === status && type.value === "none" && row.dataset.isNew !== "true") {
      type.value = status.value === "active" ? "return" : "pause";
    }
  });

  $("#managerSettings").addEventListener("click", (event) => {
    const editButton = event.target.closest(".edit-manager-row");
    if (editButton) {
      unlockSettingsSection("manager");
      const row = editButton.closest("[data-manager-id]");
      row?.querySelector(".manager-name")?.focus();
      return;
    }

    const orderButton = event.target.closest(".manager-order-button");
    if (orderButton) {
      moveManagerSettingsRow(
        orderButton.closest("[data-manager-id]"),
        orderButton.classList.contains("manager-order-up") ? -1 : 1
      );
      return;
    }

    const removeButton = event.target.closest(".remove-manager");
    if (removeButton) {
      if (!settingsEditMode.manager) return;
      const row = removeButton.closest("[data-manager-id]");
      const name = String(row?.querySelector(".manager-name")?.value || "매니저").trim();
      const registered = managerById(row?.dataset.managerId);
      if (registered && managerHasStoredReferences(registered)) {
        showToast("접수·목표·수기실적이 연결된 매니저는 등록 삭제할 수 없습니다. 재직상태 이력에 퇴사·휴직을 추가하세요."); return;
      }
      if (!row || !window.confirm(`${name} 매니저를 등록 목록에서 삭제할까요?\n기존 접수·목표·수기실적은 보존됩니다.`)) return;
      managerSettingsDeletedIds.add(row.dataset.managerId);
      row.remove();
      refreshManagerOrderNumbers();
      return;
    }

    const cancelButton = event.target.closest(".cancel-new-manager");
    if (!cancelButton || !settingsEditMode.manager) return;
    const row = cancelButton.closest("[data-manager-id]");
    if (row?.dataset.isNew === "true") {
      row.remove();
      refreshManagerOrderNumbers();
    }
  });


  // 표시 정렬만 전환합니다. state.records 및 백업 데이터 순서는 변경하지 않습니다.
  $("#recordDateSortBtn")?.addEventListener("click", () => {
    recordSequenceSort = recordSequenceSort === "asc" ? "desc" : "asc";
    renderRecords();
  });
  $("#mobileRecordDateSortBtn")?.addEventListener("click", () => {
    recordSequenceSort = recordSequenceSort === "asc" ? "desc" : "asc";
    renderRecords();
  });
  $("#membershipDateSortBtn")?.addEventListener("click", () => {
    membershipRecordDateSort = membershipRecordDateSort === "asc" ? "desc" : "asc";
    renderMembershipRecords();
  });

  ["#recordSimpleSearch", "#recordDateBasisFilter", "#recordMonthFilter", "#recordStartDateFilter", "#recordEndDateFilter", "#recordStatusFilter", "#recordManagerFilter", "#recordCategoryFilter", "#recordSellerFilter"].forEach((selector) => {
    const node = $(selector);
    if (node) node.addEventListener("change", () => {
      if (selector === "#recordMonthFilter") {
        applyRecordMonthPeriod(node.value);
        const basis = $("#recordDateBasisFilter");
        if (basis) basis.value = "receivedDate";
        showToast(`접수리스트 조회월을 ${formatMonthLabel(node.value)}로 변경했습니다.`);
      }
      renderRecords();
      renderMembershipFilterOptions();
      renderMembershipRecords();
    });
  });

  $("#recordTodayViewBtn")?.addEventListener("click", () => {
    const today = todayIso();
    const monthInput = $("#recordMonthFilter");
    if (monthInput) monthInput.value = today.slice(0, 7);
    const startInput = $("#recordStartDateFilter");
    const endInput = $("#recordEndDateFilter");
    if (startInput) startInput.value = today;
    if (endInput) endInput.value = today;
    renderRecords();
    showToast("오늘 접수내역만 표시합니다.");
  });

  $("#recordGoalPeriodBtn")?.addEventListener("click", () => {
    // 접수리스트의 '목표월 기준'은 항상 해당 월의 목표시작일~목표종료일을 사용합니다.
    // 접수일(receivedDate) 기준으로 다시 맞추고, 임의 기간 조회값을 초기화합니다.
    const month = $("#recordMonthFilter")?.value || currentDashboardMonth() || monthIso();
    applyRecordMonthPeriod(month);
    const basis = $("#recordDateBasisFilter");
    if (basis) basis.value = "receivedDate";
    renderRecords();
    showToast(`목표월 기준으로 조회합니다. ${monthPeriod(month).start} ~ ${monthPeriod(month).end}`);
  });

  $("#recordAllViewBtn").addEventListener("click", () => {
    ["#recordMonthFilter", "#recordStartDateFilter", "#recordEndDateFilter", "#recordManagerFilter"].forEach((selector) => {
      const node = $(selector);
      if (node) node.value = "";
    });
    renderRecords();
    showToast("전체 접수내역을 접수일 기준으로 표시합니다.");
  });

  $("#clearRecordFiltersBtn").addEventListener("click", () => {
    ["#recordSimpleSearch", "#recordDateBasisFilter", "#recordMonthFilter", "#recordStartDateFilter", "#recordEndDateFilter", "#recordStatusFilter", "#recordManagerFilter", "#recordCategoryFilter", "#recordSellerFilter"].forEach((selector) => {
      const node = $(selector);
      if (node) node.value = "";
    });
    renderRecords();
  });

  $("#printRecordsBtn")?.addEventListener("click", printRecordList);
  $("#exportCsvBtn")?.addEventListener("click", exportCsv);
  $("#viewTodayInstallRecords")?.addEventListener("click", applyTodayInstallFilter);
  $("#viewYesterdayInstallRecords")?.addEventListener("click", applyYesterdayInstallFilter);
  $("#installTodayModal")?.addEventListener("click", (event) => {
    const btn = event.target.closest("[data-install-complete]");
    if (!btn) return;
    completeInstallFromAlert(btn.dataset.installComplete);
  });
  $("#closeInstallTodayModal")?.addEventListener("click", closeInstallTodayModal);
  $("#closeInstallTodayModal2")?.addEventListener("click", closeInstallTodayModal);
  $$("[data-install-modal-close]").forEach((node) => node.addEventListener("click", closeInstallTodayModal));

  $("#importExcelInput")?.addEventListener("change", async (event) => {
    const file = event.target.files?.[0];
    if (file) await importExcelFile(file);
    event.target.value = "";
  });

}


document.addEventListener("click", (event) => {
  const closeBtn = event.target.closest?.("#managerShareCloseBtn, #managerShareCancelBtn");
  if (closeBtn) {
    closeManagerShareModal();
    return;
  }
  if (event.target.closest?.("#managerShareCopyBtn")) {
    copyCurrentManagerShareImage();
    return;
  }
  if (event.target.closest?.("#managerShareSaveBtn")) {
    saveCurrentManagerShareImage();
    return;
  }
  const primaryShareButton = event.target.closest?.("#managerShareKakaoBtn");
  if (primaryShareButton) {
    if (primaryShareButton.dataset.actionMode === "print") {
      printCurrentSharePreview();
    } else {
      kakaoShareCurrentManagerImage();
    }
    return;
  }
  if (event.target?.id === "managerShareModal") closeManagerShareModal();
});




// === V11.28 라이선스 관리 (내부 WEB 전용) ================================
// 영업 데이터/DB관리 상태와 완전히 분리된 관리도구입니다.
// Apps Script URL만 localStorage에, 관리자 인증키는 sessionStorage에 보관합니다.
const LICENSE_ADMIN_URL_KEY = "sk_sales_license_server_url_v1";
const LICENSE_ADMIN_SECRET_KEY = "sk_sales_license_admin_secret_v1";
let licenseAdminRows = [];
let licenseUiBound = false;

function licenseAdminServerUrl() {
  return String(localStorage.getItem(LICENSE_ADMIN_URL_KEY) || "").trim();
}
function licenseAdminSecret() {
  return String(sessionStorage.getItem(LICENSE_ADMIN_SECRET_KEY) || "").trim();
}
function normalizeLicenseServerUrl(value = "") {
  const text = String(value || "").trim();
  if (!text) return "";
  try {
    const url = new URL(text);
    if (url.protocol !== "https:" || !/^(script\.google\.com|script\.googleusercontent\.com)$/i.test(url.hostname)) return "";
    return url.toString();
  } catch { return ""; }
}
function licenseJsonpRequest(action, payload = {}, timeoutMs = 12000) {
  const serverUrl = licenseAdminServerUrl();
  const secret = licenseAdminSecret();
  if (!serverUrl) return Promise.reject(new Error("Apps Script Web App URL을 먼저 입력해 주세요."));
  if (!secret) return Promise.reject(new Error("관리자 인증키를 입력해 주세요."));
  return new Promise((resolve, reject) => {
    const callback = `__skLicenseCb_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    const script = document.createElement("script");
    const timer = window.setTimeout(() => finish(new Error("라이선스 서버 응답 시간이 초과되었습니다.")), timeoutMs);
    const finish = (error, result) => {
      window.clearTimeout(timer);
      try { delete window[callback]; } catch {}
      script.remove();
      if (error) reject(error); else resolve(result);
    };
    window[callback] = (result) => finish(null, result);
    const url = new URL(serverUrl);
    const params = { action, adminSecret: secret, callback, _ts: Date.now(), ...payload };
    Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, String(value ?? "")));
    script.onerror = () => finish(new Error("Google 라이선스 서버에 연결하지 못했습니다."));
    script.src = url.toString();
    document.head.appendChild(script);
  });
}
function licenseStatusLabel(status = "") {
  const key = String(status || "READY").toUpperCase();
  return ({ READY: "미등록", ACTIVE: "사용중", PAUSED: "사용중지" })[key] || key;
}
function licenseDeviceLabel(row = {}) {
  const id = String(row.deviceId || "").trim();
  if (!id) return "미등록";
  return id.length > 14 ? `${id.slice(0, 7)}…${id.slice(-6)}` : id;
}
function licenseDisplayDate(value = "") {
  if (!value) return "-";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString("ko-KR", { year:"2-digit", month:"2-digit", day:"2-digit", hour:"2-digit", minute:"2-digit" });
}
function updateLicenseSummary(rows = licenseAdminRows) {
  const counts = { total: rows.length, active: 0, ready: 0, paused: 0 };
  rows.forEach((row) => {
    const status = String(row.status || "READY").toUpperCase();
    if (status === "ACTIVE") counts.active += 1;
    else if (status === "PAUSED") counts.paused += 1;
    else counts.ready += 1;
  });
  if ($("#licenseTotalCount")) $("#licenseTotalCount").textContent = counts.total;
  if ($("#licenseActiveCount")) $("#licenseActiveCount").textContent = counts.active;
  if ($("#licenseReadyCount")) $("#licenseReadyCount").textContent = counts.ready;
  if ($("#licensePausedCount")) $("#licensePausedCount").textContent = counts.paused;
}
function renderLicenseRows(rows = licenseAdminRows) {
  const query = String($("#licenseSearchInput")?.value || "").trim().toLowerCase();
  const filtered = !query ? rows : rows.filter((row) => [row.branchName, row.managerName, row.position, row.licenseKey, row.contact, row.note].some((v) => String(v || "").toLowerCase().includes(query)));
  const body = $("#licenseTableBody");
  const mobile = $("#licenseMobileList");
  if (!filtered.length) {
    if (body) body.innerHTML = `<tr><td colspan="9" class="empty-state">표시할 라이선스가 없습니다.</td></tr>`;
    if (mobile) mobile.innerHTML = `<div class="license-mobile-empty">표시할 라이선스가 없습니다.</div>`;
    return;
  }
  if (body) body.innerHTML = filtered.map((row) => {
    const status = String(row.status || "READY").toUpperCase();
    const managerName = String(row.managerName || "").trim();
    const position = String(row.position || "").trim();
    const legacyContact = String(row.contact || "").trim();
    return `<tr>
      <td><strong>${escapeHtml(row.branchName || "-")}</strong>${row.note ? `<small>${escapeHtml(row.note)}</small>` : ""}</td>
      <td><strong>${escapeHtml(managerName || "미설정")}</strong>${!managerName && legacyContact ? `<small>기존 메모: ${escapeHtml(legacyContact)}</small>` : ""}</td>
      <td>${escapeHtml(position || "미설정")}</td>
      <td><code>${escapeHtml(row.licenseKey || "-")}</code><button class="license-mini-btn" data-license-copy="${escapeHtml(row.licenseKey || "")}" type="button">복사</button></td>
      <td><span class="license-status-badge ${status.toLowerCase()}">${licenseStatusLabel(status)}</span></td>
      <td>${escapeHtml(licenseDeviceLabel(row))}</td>
      <td>${escapeHtml(licenseDisplayDate(row.activatedAt))}</td>
      <td>${escapeHtml(licenseDisplayDate(row.lastValidatedAt))}</td>
      <td class="license-row-actions">
        <button class="license-mini-btn" data-license-toggle="${escapeHtml(row.id || row.licenseKey || "")}" data-next-status="${status === "PAUSED" ? "ACTIVE" : "PAUSED"}" type="button">${status === "PAUSED" ? "사용허용" : "사용중지"}</button>
        <button class="license-mini-btn" data-license-reset-device="${escapeHtml(row.id || row.licenseKey || "")}" type="button">PC초기화</button>
        <button class="license-mini-btn danger" data-license-delete="${escapeHtml(row.id || row.licenseKey || "")}" type="button">삭제</button>
      </td>
    </tr>`;
  }).join("");
  if (mobile) mobile.innerHTML = filtered.map((row) => {
    const status = String(row.status || "READY").toUpperCase();
    const managerName = String(row.managerName || "").trim() || "미설정";
    const position = String(row.position || "").trim() || "미설정";
    return `<article class="license-mobile-card"><div><strong>${escapeHtml(row.branchName || "-")}</strong><span class="license-status-badge ${status.toLowerCase()}">${licenseStatusLabel(status)}</span></div><p class="license-mobile-person">담당자 ${escapeHtml(managerName)} · ${escapeHtml(position)}</p><code>${escapeHtml(row.licenseKey || "-")}</code><p>PC ${escapeHtml(licenseDeviceLabel(row))} · 최근 ${escapeHtml(licenseDisplayDate(row.lastValidatedAt))}</p><div><button class="license-mini-btn" data-license-copy="${escapeHtml(row.licenseKey || "")}" type="button">키 복사</button><button class="license-mini-btn" data-license-toggle="${escapeHtml(row.id || row.licenseKey || "")}" data-next-status="${status === "PAUSED" ? "ACTIVE" : "PAUSED"}" type="button">${status === "PAUSED" ? "사용허용" : "사용중지"}</button><button class="license-mini-btn" data-license-reset-device="${escapeHtml(row.id || row.licenseKey || "")}" type="button">PC초기화</button><button class="license-mini-btn danger" data-license-delete="${escapeHtml(row.id || row.licenseKey || "")}" type="button">삭제</button></div></article>`;
  }).join("");
}
function setLicenseServerState(ok, message = "") {
  const pill = $("#licenseServerStatePill");
  if (!pill) return;
  pill.textContent = ok ? "연결됨" : (message || "미연결");
  pill.classList.toggle("ok", Boolean(ok));
}
async function refreshLicenseList({ silent = false } = {}) {
  try {
    const result = await licenseJsonpRequest("adminList");
    if (!result?.ok) throw new Error(result?.error || "라이선스 목록을 불러오지 못했습니다.");
    licenseAdminRows = Array.isArray(result.licenses) ? result.licenses : [];
    updateLicenseSummary();
    renderLicenseRows();
    setLicenseServerState(true);
    if (!silent) showToast("라이선스 목록을 새로고침했습니다.");
  } catch (error) {
    setLicenseServerState(false, "연결 확인 필요");
    if (!silent) showToast(error.message || "라이선스 서버 연결에 실패했습니다.");
  }
}
async function testLicenseServerConnection() {
  const url = normalizeLicenseServerUrl($("#licenseServerUrlInput")?.value);
  const secret = String($("#licenseAdminSecretInput")?.value || "").trim();
  if (!url) { showToast("올바른 Apps Script Web App URL을 입력해 주세요."); return; }
  if (!secret) { showToast("관리자 인증키를 입력해 주세요."); return; }
  localStorage.setItem(LICENSE_ADMIN_URL_KEY, url);
  sessionStorage.setItem(LICENSE_ADMIN_SECRET_KEY, secret);
  try {
    const result = await licenseJsonpRequest("adminPing");
    if (!result?.ok) throw new Error(result?.error || "관리자 인증에 실패했습니다.");
    setLicenseServerState(true);
    showToast("Google 라이선스 서버에 연결했습니다.");
    await refreshLicenseList({ silent: true });
  } catch (error) {
    setLicenseServerState(false, "인증 실패");
    showToast(error.message || "라이선스 서버 연결에 실패했습니다.");
  }
}
async function issueLicenseFromAdmin() {
  const branchName = String($("#licenseBranchNameInput")?.value || "").trim();
  const managerName = String($("#licenseManagerNameInput")?.value || "").trim();
  const position = String($("#licensePositionInput")?.value || "").trim();
  const note = String($("#licenseNoteInput")?.value || "").trim();
  if (!branchName) { showToast("지국명을 입력해 주세요."); return; }
  if (!managerName) { showToast("담당자 이름을 입력해 주세요."); return; }
  if (!position) { showToast("직책을 입력해 주세요."); return; }
  try {
    const result = await licenseJsonpRequest("adminCreate", { branchName, managerName, position, note, allowedPc: 1 });
    if (!result?.ok) throw new Error(result?.error || "라이선스를 발급하지 못했습니다.");
    const key = String(result.license?.licenseKey || result.licenseKey || "");
    const wrap = $("#licenseIssuedResult");
    if (wrap) wrap.hidden = false;
    if ($("#licenseIssuedKey")) $("#licenseIssuedKey").textContent = key;
    if ($("#licenseBranchNameInput")) $("#licenseBranchNameInput").value = "";
    if ($("#licenseManagerNameInput")) $("#licenseManagerNameInput").value = "";
    if ($("#licensePositionInput")) $("#licensePositionInput").value = "";
    if ($("#licenseNoteInput")) $("#licenseNoteInput").value = "";
    showToast(`${branchName} · ${managerName} ${position} 라이선스를 발급했습니다.`);
    await refreshLicenseList({ silent: true });
  } catch (error) { showToast(error.message || "라이선스 발급에 실패했습니다."); }
}
async function updateLicenseAdminState(id, status) {
  try {
    const result = await licenseJsonpRequest("adminSetStatus", { id, status });
    if (!result?.ok) throw new Error(result?.error || "라이선스 상태 변경에 실패했습니다.");
    await refreshLicenseList({ silent: true });
    showToast(status === "PAUSED" ? "라이선스를 사용중지했습니다." : "라이선스를 다시 허용했습니다.");
  } catch (error) { showToast(error.message || "라이선스 상태 변경에 실패했습니다."); }
}
async function resetLicenseDevice(id) {
  if (!window.confirm("이 라이선스의 등록 PC를 초기화할까요? 기존 PC는 다음 인증부터 사용할 수 없고 새 PC에서 다시 인증할 수 있습니다.")) return;
  try {
    const result = await licenseJsonpRequest("adminResetDevice", { id });
    if (!result?.ok) throw new Error(result?.error || "PC 등록 초기화에 실패했습니다.");
    await refreshLicenseList({ silent: true });
    showToast("등록 PC를 초기화했습니다.");
  } catch (error) { showToast(error.message || "PC 등록 초기화에 실패했습니다."); }
}
async function deleteLicenseFromAdmin(id) {
  const row = licenseAdminRows.find((item) => String(item.id || item.licenseKey || "") === String(id || ""));
  const label = row ? `${row.branchName || "지국명 미상"} / ${row.managerName || "담당자 미설정"} ${row.position || ""}`.trim() : "선택한 라이선스";
  const activeWarning = String(row?.status || "").toUpperCase() === "ACTIVE" ? `\n\n현재 사용중인 라이선스입니다. 삭제하면 등록 PC 정보와 인증 이력도 함께 삭제됩니다.` : "";
  if (!window.confirm(`${label} 라이선스를 삭제할까요?${activeWarning}

삭제한 라이선스는 되돌릴 수 없습니다.`)) return;
  try {
    const result = await licenseJsonpRequest("adminDelete", { id });
    if (!result?.ok) throw new Error(result?.error || "라이선스 삭제에 실패했습니다.");
    await refreshLicenseList({ silent: true });
    showToast("라이선스를 삭제했습니다.");
  } catch (error) { showToast(error.message || "라이선스 삭제에 실패했습니다."); }
}
function bindLicenseManagementEvents() {
  if (licenseUiBound) return;
  licenseUiBound = true;
  $("#licenseServerConnectBtn")?.addEventListener("click", testLicenseServerConnection);
  $("#licenseRefreshBtn")?.addEventListener("click", () => refreshLicenseList());
  $("#licenseIssueBtn")?.addEventListener("click", issueLicenseFromAdmin);
  $("#licenseSearchInput")?.addEventListener("input", () => renderLicenseRows());
  $("#licenseIssuedCopyBtn")?.addEventListener("click", async () => {
    const key = String($("#licenseIssuedKey")?.textContent || "").trim();
    if (!key) return;
    try { await navigator.clipboard.writeText(key); showToast("라이선스 키를 복사했습니다."); }
    catch { showToast("복사하지 못했습니다. 키를 직접 선택해 주세요."); }
  });
  const clickHandler = async (event) => {
    const copy = event.target.closest("[data-license-copy]");
    if (copy) {
      try { await navigator.clipboard.writeText(copy.dataset.licenseCopy || ""); showToast("라이선스 키를 복사했습니다."); } catch { showToast("키 복사에 실패했습니다."); }
      return;
    }
    const toggle = event.target.closest("[data-license-toggle]");
    if (toggle) { await updateLicenseAdminState(toggle.dataset.licenseToggle, toggle.dataset.nextStatus); return; }
    const reset = event.target.closest("[data-license-reset-device]");
    if (reset) { await resetLicenseDevice(reset.dataset.licenseResetDevice); return; }
    const remove = event.target.closest("[data-license-delete]");
    if (remove) await deleteLicenseFromAdmin(remove.dataset.licenseDelete);
  };
  $("#licenseTableBody")?.addEventListener("click", clickHandler);
  $("#licenseMobileList")?.addEventListener("click", clickHandler);
}
function renderLicenseManagement() {
  bindLicenseManagementEvents();
  if ($("#licenseServerUrlInput") && !$("#licenseServerUrlInput").value) $("#licenseServerUrlInput").value = licenseAdminServerUrl();
  if ($("#licenseAdminSecretInput") && !$("#licenseAdminSecretInput").value) $("#licenseAdminSecretInput").value = licenseAdminSecret();
  updateLicenseSummary();
  renderLicenseRows();
  if (licenseAdminServerUrl() && licenseAdminSecret() && !licenseAdminRows.length) void refreshLicenseList({ silent: true });
}
// ========================================================================

const APP_VERSION = "v1.03";
const STATE_SCHEMA_VERSION = 4;

function normalizeVersionText(version = "") {
  return String(version || "").trim().replace(/^v/i, "");
}

function versionLabelForDisplay(version = APP_VERSION) {
  const normalized = normalizeVersionText(version);
  return normalized ? `V${normalized}` : APP_VERSION.toUpperCase();
}

function applyProgramVersionToStaticLabels() {
  const label = versionLabelForDisplay(APP_VERSION);
  const startup = $("#startupVersionLabel");
  if (startup) startup.textContent = label;
  const meta = document.querySelector('meta[name="app-version"]');
  if (meta) meta.content = label;
}

function openCompleteResetModal() {
  const modal = $("#completeResetModal");
  const check = $("#completeResetBackupConfirm");
  const execute = $("#completeResetExecuteBtn");
  if (!modal) return;
  if (check) check.checked = false;
  if (execute) execute.disabled = true;
  modal.hidden = false;
  document.body.classList.add("complete-reset-open");
}
function closeCompleteResetModal() {
  const modal = $("#completeResetModal");
  if (modal) modal.hidden = true;
  document.body.classList.remove("complete-reset-open");
}
async function executeCompleteReset() {
  const check = $("#completeResetBackupConfirm");
  if (!check?.checked) return;
  if (!window.confirm("정말 현재 프로그램 데이터를 초기화할까요? 초기화 직전 안전백업을 먼저 생성합니다.")) return;
  const safetyBackup = await createDbBackup("before-reset", { silent: true });
  if (stateDataCount(state) > 0 && !safetyBackup) {
    if (!window.confirm("초기화 전 안전백업 생성에 실패했습니다. 그래도 초기화를 계속할까요?")) return;
  }
  const previousPersistRevision = Number(state?.appMeta?.persistRevision || 0);
  const fresh = normalizeState({ ...sampleState, records: [], managers: [], promotions: [], checklistItems: [], contactNotes: [], contactRequests: [], todos: [], todosByDate: {}, managerManualStats: {}, managerManualOrder: {}, managerMonthlyGoals: {}, operatingGoals: {}, managementEvaluationInputs: {}, managementEvaluationPolicies: {}, payrollRecords: [], payrollUnmatchedRecords: [], payrollArchives: [], appMeta: { ...sampleState.appMeta } });
  state = fresh;
  state.appMeta = state.appMeta && typeof state.appMeta === "object" ? state.appMeta : {};
  state.appMeta.persistRevision = Number.isFinite(previousPersistRevision) && previousPersistRevision > 0 ? previousPersistRevision : 0;
  invalidateManagerCaches();
  try {
    const resetSaveResult = await persistState({ ensureManagers: false, immediateServer: true, allowEmptyServer: true });
    if (resetSaveResult?.ok === false) throw new Error("초기화 데이터 저장에 실패했습니다.");
    localStorage.removeItem(LOCAL_BACKUP_KEY);
    localStorage.removeItem(LOCAL_BACKUP_INDEX_KEY);
    // Keep the committed empty snapshot as the newest reset state.
  } catch (error) {
    console.error("[COMPLETE RESET] save failed", error);
    showToast("초기화 데이터를 저장하지 못했습니다. 다시 시도해 주세요.");
    return;
  }
  closeCompleteResetModal();
  renderNow();
  showToast("모든 프로그램 데이터를 초기화했습니다.");
}
function initStartupIntro() {
  const intro = $("#startupIntro");
  if (!intro) return;
  const status = $("#startupIntroStatus");
  const progress = $("#startupIntroProgress");
  const messages = ["업무 화면을 구성하고 있습니다", "저장 데이터를 확인하고 있습니다", "영업현황을 불러오는 중입니다"];
  let idx = 0;
  const timer = setInterval(() => {
    idx += 1;
    if (status) status.textContent = messages[Math.min(idx, messages.length - 1)];
    if (progress) progress.style.width = `${Math.min(100, 30 + idx * 35)}%`;
    if (idx >= 2) { clearInterval(timer); setTimeout(() => { intro.classList.add("startup-intro-hidden"); setTimeout(() => intro.remove(), 420); }, 420); }
  }, 430);
}

async function init() {
  initStartupIntro();
  startSidebarClock();
  if (!IS_PC_APP && window.SKCloud?.enabled?.()) {
    try {
      await window.SKCloud.ensureSignedIn();
    } catch (error) {
      console.error("[SK CLOUD] login init failed", error);
      window.alert("SK 클라우드 로그인 준비에 실패했습니다. 페이지를 새로고침해 주세요.");
      return;
    }
  }
  try {
    await loadPersistedState();
  } catch (error) {
    console.warn("[STARTUP] state load failed", error);
    window.alert(error?.message || "저장 데이터를 불러오지 못했습니다.");
    return;
  }
  // 프로그램을 새로 열 때는 저장된 과거 월이나 테스트용 고정 날짜가 아니라
  // 실제 PC의 현재 날짜 기준 월로 대시보드를 시작한다.
  const month = monthIso();
  const period = monthPeriod(month);
  $("#monthFilter").value = month;
  const renewalGuideMonthInput = $("#renewalGuideMonthInput");
  if (renewalGuideMonthInput) renewalGuideMonthInput.value = month;
  const recordMonthFilter = $("#recordMonthFilter");
  if (recordMonthFilter) recordMonthFilter.value = month;
  const promoMonthFilter = $("#promoMonthFilter");
  if (promoMonthFilter) promoMonthFilter.value = month;
  const recordStartDateFilter = $("#recordStartDateFilter");
  if (recordStartDateFilter) recordStartDateFilter.value = period.start;
  const recordEndDateFilter = $("#recordEndDateFilter");
  if (recordEndDateFilter) recordEndDateFilter.value = dashboardDefaultEnd(period);
  applyRecordMonthPeriod(month);
  const dayFilter = $("#dayFilter");
  if (dayFilter) dayFilter.value = "";
  setDashboardRange(period.start, dashboardDefaultEnd(period));
  attachEvents();
  document.body.dataset.view = currentView;
  const recordsView = $("#recordsView");
  if (recordsView && !recordsView.dataset.mobileRecordTab) recordsView.dataset.mobileRecordTab = "main";
  resetRecordForm();
  renderNow();
  queueDbAutoBackup();
  window.setInterval(() => { if (currentView === "db") refreshDbManagement(); }, 15000);
  window.setTimeout(openInstallTodayModalIfNeeded, 400);
  window.setTimeout(startChecklistAlarmWatcher, 550);
}


window.shareManagerKakaoImage = shareManagerKakaoImage;
window.openManagerShareModal = openManagerShareModal;
window.kakaoShareCurrentManagerImage = kakaoShareCurrentManagerImage;
window.copyCurrentManagerShareImage = copyCurrentManagerShareImage;
window.saveCurrentManagerShareImage = saveCurrentManagerShareImage;


// Saved changes are committed by persistState(); pagehide must not overwrite another window.

window.SK_SALES_VERSION = APP_VERSION;
window.SK_SALES_SCHEMA_VERSION = STATE_SCHEMA_VERSION;
window.reportImageBlob = reportImageBlob;
window.shareKakaoImage = shareKakaoImage;

// 한 번의 동기 화면 계산 안에서만 재사용합니다. 저장/조회월 변경 뒤에는 항상 새로 계산합니다.
let calculationScope = null;
function withCalculationScope(callback) {
  if (calculationScope) return callback();
  calculationScope = new Map();
  try { return callback(); } finally { calculationScope = null; }
}
function memoizeCalculation(fn, objectArgument = false) {
  return function (...args) {
    if (!calculationScope) return fn.apply(this, args);
    let cache = calculationScope.get(fn);
    if (!cache) { cache = objectArgument ? new WeakMap() : new Map(); calculationScope.set(fn, cache); }
    const key = objectArgument ? args[0] : JSON.stringify(args);
    if (objectArgument && (!key || typeof key !== "object")) return fn.apply(this, args);
    if (!cache.has(key)) cache.set(key, fn.apply(this, args));
    const result = cache.get(key);
    // 호출부에서 정렬해도 저장된 목록 순서에 영향을 주지 않습니다.
    return Array.isArray(result) ? result.slice() : result;
  };
}
function scopedCalculation(fn) {
  return function (...args) { return withCalculationScope(() => fn.apply(this, args)); };
}
function installCalculationScopes() {
  teamManagers = memoizeCalculation(teamManagers);
  teamManagerNames = memoizeCalculation(teamManagerNames);
  analyticsSettings = memoizeCalculation(analyticsSettings);
  analyticsAutoStartMonth = memoizeCalculation(analyticsAutoStartMonth);
  analyticsCanonicalPersonName = memoizeCalculation(analyticsCanonicalPersonName);
  analyticsResolveAliasName = memoizeCalculation(analyticsResolveAliasName);
  analyticsBaseRecordsForMonth = memoizeCalculation(analyticsBaseRecordsForMonth);
  analyticsActualEntityNames = memoizeCalculation(analyticsActualEntityNames);
  analyticsAliasSourcesForTarget = memoizeCalculation(analyticsAliasSourcesForTarget);
  analyticsReportedRecords = memoizeCalculation(analyticsReportedRecords);
  analyticsActualRecords = memoizeCalculation(analyticsActualRecords);
  analyticsActivityRecords = memoizeCalculation(analyticsActivityRecords);
  analyticsManualStat = memoizeCalculation(analyticsManualStat);
  analyticsGoalFor = memoizeCalculation(analyticsGoalFor);
  analyticsMonthHasData = memoizeCalculation(analyticsMonthHasData);
  analyticsMonthStatus = memoizeCalculation(analyticsMonthStatus);
  analyticsIsCompletedMonth = memoizeCalculation(analyticsIsCompletedMonth);
  analyticsMonthlyMetrics = memoizeCalculation(analyticsMonthlyMetrics);
  analyticsProductStats = memoizeCalculation(analyticsProductStats);
  analyticsManagerSummary = memoizeCalculation(analyticsManagerSummary);
  analyticsResolveSellerName = memoizeCalculation(analyticsResolveSellerName, true);
  analyticsReportedManagerName = memoizeCalculation(analyticsReportedManagerName, true);
  promoManagerStats = memoizeCalculation(promoManagerStats);
  renderAnalytics = scopedCalculation(renderAnalytics);
  drawAnalyticsTrendChart = scopedCalculation(drawAnalyticsTrendChart);
  renderPromotions = scopedCalculation(renderPromotions);
  renderPromotionDetail = scopedCalculation(renderPromotionDetail);
  renderPromotionManagerDetail = scopedCalculation(renderPromotionManagerDetail);
  printPromotionMonthlyReport = scopedCalculation(printPromotionMonthlyReport);
}

// 재직/소속의 적용월과 기본정보 수정을 분리합니다. 편집 중에는 state를 바꾸지 않습니다.
function managerHistoryFields(manager) {
  return { joinedMonth: manager.joinedMonth, status: manager.status, team: manager.team,
    inactiveMonth: manager.inactiveMonth, statusHistory: manager.statusHistory,
    teamHistory: manager.teamHistory, changeHistory: manager.changeHistory };
}
function managerHistoryStatusValue(manager, entry) {
  if (entry.status !== "inactive") return "active";
  return manager.changeHistory?.some((item) => item.month === entry.startMonth && item.type === "leave") ? "leave" : "pause";
}
function managerTimelineRowMarkup(kind, entry, value, teams, isBase = false) {
  const options = kind === "team" ? [["", "소속팀 선택"], ...teams.map((team) => [team, team])] : [["", "변경할 상태 선택"], ["active", "재직·복귀"], ["pause", "휴직·비활성"], ["leave", "퇴사"]];
  return `<div class="manager-timeline-entry">
    <label>시작월<input class="history-start" type="month" value="${escapeHtml(entry.startMonth || "")}" aria-label="${kind === "team" ? "소속" : "재직"} 시작월"></label>
    <label>${kind === "team" ? "소속팀" : "재직상태"}<select class="history-value">${options.map(([key, label]) => `<option value="${escapeHtml(key)}"${value === key ? " selected" : ""}>${escapeHtml(label)}</option>`).join("")}</select></label>
    <span class="history-range">${escapeHtml(entry.startMonth ? `${entry.startMonth}-01부터` : "시작월 미상 · 이전부터")}<br>${escapeHtml(entry.endMonth ? `${entry.endMonth} 말일까지` : "이후 계속")}</span>
    ${isBase ? `<span class="history-base">최초 이력</span>` : `<button type="button" class="ghost-button small history-remove">이력 삭제</button>`}
  </div>`;
}
function managerTimelineMarkup(manager) {
  const teams = [...new Set([...configuredTeamNames(), ...manager.teamHistory.map((item) => item.team)])];
  return `<details class="manager-history-editor"><summary>재직·소속 이력 확인 / 수정</summary>
    <p>시작월 <strong>1일</strong>부터 적용합니다. 다음 이력 전월 말일까지 이어지며, 이력을 삭제하면 앞 이력이 이어집니다. 최초 이력은 삭제 대신 수정하세요. 첫 이력의 빈 시작월은 ‘이전부터 적용(시작월 미상)’입니다.</p>
    ${["status", "team"].map((kind) => `<section class="manager-timeline" data-history-kind="${kind}" data-dirty="false">
      <h4>${kind === "team" ? "소속팀 이력" : "재직상태 이력"}</h4>
      <div class="manager-timeline-rows">${manager[kind === "team" ? "teamHistory" : "statusHistory"].map((entry, index) => managerTimelineRowMarkup(kind, entry, kind === "team" ? entry.team : managerHistoryStatusValue(manager, entry), teams, index === 0)).join("")}</div>
      <button type="button" class="ghost-button small history-add">${kind === "team" ? "팀 이동 추가" : "퇴사·휴직·복귀 추가"}</button>
    </section>`).join("")}
    <p class="manager-history-preview" aria-live="polite">저장된 이력입니다. 수정하려면 ‘매니저 설정 수정’을 누르세요.</p>
    <small>접수일의 달력월로 소속을 판단하고, 실적은 목표산정기간으로 집계합니다. 예: 10월 목표기간이 9월 28일부터라면, 10월 퇴사여도 9월 28~30일 접수는 10월 실적에 남습니다. 과거 이력을 정정하면 해당 기간의 팀별 조회·통계·평가도 다시 계산됩니다.</small>
    ${manager.historyEdits?.length ? `<details class="manager-history-audit"><summary>이력 정정 기록 (${manager.historyEdits.length}회)</summary>${manager.historyEdits.slice().reverse().map((edit) => `<p>${escapeHtml(new Date(edit.at).toLocaleString("ko-KR"))}<br>변경 전: ${escapeHtml(managerHistoryDescription(edit.before))}<br>변경 후: ${escapeHtml(managerHistoryDescription(edit.after))}</p>`).join("")}</details>` : ""}
  </details>`;
}
function managerHistoryDescription(fields) {
  return ["statusHistory", "teamHistory"].map((key) => (fields?.[key] || []).map((entry) => `${entry.startMonth || "이전부터"}~${entry.endMonth || "계속"} ${entry.team || (entry.status === "inactive" ? ((fields.changeHistory || []).some((reason) => reason.month === entry.startMonth && reason.type === "leave") ? "퇴사" : "휴직·비활성") : "재직")}`).join(", ")).join(" / ");
}
function compileManagerTimeline(entries, kind) {
  if (!entries.length) throw new Error("첫 이력은 남겨 주세요. 잘못된 첫 이력은 시작월과 내용을 수정하세요.");
  const sorted = entries.map((entry) => ({ startMonth: String(entry.startMonth || ""), value: String(entry.value || "") })).sort((a, b) => a.startMonth.localeCompare(b.startMonth));
  const seen = new Set();
  sorted.forEach((entry, index) => {
    if (entry.startMonth && !/^\d{4}-(0[1-9]|1[0-2])$/.test(entry.startMonth)) throw new Error("시작월을 정확하게 입력해 주세요.");
    if (seen.has(entry.startMonth)) throw new Error("같은 시작월의 이력이 중복됩니다. 기존 이력을 수정하거나 중복 행을 삭제하세요.");
    if (!entry.startMonth && index !== 0) throw new Error("시작월 미상은 첫 이력에만 사용할 수 있습니다.");
    if (!entry.value || (kind === "status" && !["active", "pause", "leave"].includes(entry.value))) throw new Error("이력의 상태 또는 소속팀을 선택해 주세요.");
    seen.add(entry.startMonth);
  });
  return sorted.map((entry, index) => ({ startMonth: entry.startMonth,
    endMonth: sorted[index + 1] ? shiftMonth(sorted[index + 1].startMonth, -1) : "",
    [kind]: kind === "status" ? (entry.value === "active" ? "active" : "inactive") : entry.value }));
}
function managerTimelineDraft(existing, drafts) {
  const next = { ...existing };
  let reasons = (existing.changeHistory || []).slice();
  for (const kind of ["status", "team"]) {
    if (!drafts[kind]) continue;
    const history = compileManagerTimeline(drafts[kind], kind);
    if (drafts[kind][0].startMonth !== history[0].startMonth) throw new Error("최초 이력의 시작월은 이후 변경월보다 빨라야 합니다. 최초 이력부터 수정해 주세요.");
    next[kind === "team" ? "teamHistory" : "statusHistory"] = history;
    if (kind === "status") {
      next.joinedMonth = history[0].startMonth;
      reasons = reasons.filter((item) => !["leave", "pause", "return"].includes(item.type));
      for (const entry of drafts.status) {
        if (entry.startMonth) reasons.push({ month: entry.startMonth, type: entry.value === "active" ? "return" : entry.value, status: entry.value === "active" ? "active" : "inactive", team: "" });
      }
    } else {
      reasons = reasons.filter((item) => item.type !== "move");
      history.slice(1).forEach((entry) => reasons.push({ month: entry.startMonth, type: "move", team: entry.team, status: "active" }));
    }
  }
  next.status = managerStatusForMonth(next, monthIso());
  next.team = managerTeamForMonth(next, monthIso());
  next.inactiveMonth = next.status === "inactive" ? (historyEntryForMonth(next.statusHistory, monthIso(), "status")?.startMonth || "") : "";
  next.changeHistory = reasons.map((entry) => ({ ...entry, team: managerTeamForMonth(next, entry.month), status: managerStatusForMonth(next, entry.month) }));
  return next;
}
function readManagerTimelineDraft(row, existing) {
  const drafts = {};
  row.querySelectorAll('.manager-timeline[data-dirty="true"]').forEach((section) => {
    drafts[section.dataset.historyKind] = [...section.querySelectorAll(".manager-timeline-entry")].map((entry) => ({startMonth: entry.querySelector(".history-start").value, value: entry.querySelector(".history-value").value}));
  });
  return Object.keys(drafts).length ? managerTimelineDraft(existing, drafts) : existing;
}
function updateManagerTimelinePreview(row) {
  const existing = managerById(row.dataset.managerId);
  const preview = row.querySelector(".manager-history-preview");
  if (!existing || !preview) return;
  try {
    const next = readManagerTimelineDraft(row, existing);
    row.querySelectorAll('.manager-timeline[data-dirty="true"]').forEach((section) => {
      const history = next[section.dataset.historyKind === "team" ? "teamHistory" : "statusHistory"];
      section.querySelectorAll(".manager-timeline-entry").forEach((entry) => {
        const start = entry.querySelector(".history-start").value;
        const item = history.find((item) => item.startMonth === start);
        entry.querySelector(".history-range").textContent = `${start ? start + "-01부터" : "시작월 미상 · 이전부터"} / ${item?.endMonth ? item.endMonth + " 말일까지" : "이후 계속"}`;
      });
    });
    let affected = 0;
    for (const record of state.records || []) {
      if (record.managerId !== existing.id && record.manager !== existing.name && record.sellerId !== existing.id && record.seller !== existing.name) continue;
      const month = organizationMonthForDate(record.receivedDate || record.installDate, currentDashboardMonth());
      if (managerTeamForMonth(existing, month) !== managerTeamForMonth(next, month) || managerStatusForMonth(existing, month) !== managerStatusForMonth(next, month)) affected++;
    }
    preview.classList.remove("history-error");
    preview.textContent = `저장 대기 · 적용 상태/소속이 달라지는 기존 접수 ${affected}건. 접수 원본은 보존됩니다. 아래 ‘매니저 설정 저장’을 누르면 확정됩니다.`;
  } catch (error) { preview.classList.add("history-error"); preview.textContent = error.message; }
}
function managerHasStoredReferences(manager) {
  return (state.records || []).some((record) => record.managerId === manager.id || record.sellerId === manager.id || record.manager === manager.name || record.seller === manager.name)
    || Object.values(state.managerMonthlyGoals || {}).some((bucket) => toNumber(bucket?.[manager.name]) !== 0)
    || Object.values(state.managerManualStats || {}).some((bucket) => Object.values(bucket?.[manager.name] || {}).some((value) => typeof value === "string" ? Boolean(value.trim()) && value.trim() !== "0" : Boolean(value)));
}
function attachManagerTimelineEvents() {
  const list = $("#managerSettings");
  if (!list) return;
  list.addEventListener("change", (event) => {
    const section = event.target.closest(".manager-timeline");
    if (!section || !settingsEditMode.manager) return;
    section.dataset.dirty = "true";
    updateManagerTimelinePreview(section.closest(".manager-row"));
  });
  list.addEventListener("click", (event) => {
    const button = event.target.closest(".history-add, .history-remove");
    if (!button || !settingsEditMode.manager) return;
    const section = button.closest(".manager-timeline");
    const row = section.closest(".manager-row");
    if (button.classList.contains("history-remove")) {
      if (section.querySelectorAll(".manager-timeline-entry").length <= 1) { showToast("첫 이력은 삭제 대신 시작월과 내용을 수정하세요."); return; }
      button.closest(".manager-timeline-entry").remove();
    } else {
      const kind = section.dataset.historyKind;
      const values = [...section.querySelectorAll(".history-start")].map((input) => input.value).filter(Boolean).sort();
      const last = values.at(-1);
      const month = last && last >= monthIso() ? shiftMonth(last, 1) : monthIso();
      const manager = managerById(row.dataset.managerId);
      const teams = [...new Set([...configuredTeamNames(), ...manager.teamHistory.map((item) => item.team)])];
      section.querySelector(".manager-timeline-rows").insertAdjacentHTML("beforeend", managerTimelineRowMarkup(kind, {startMonth: month, endMonth: ""}, "", teams));
      section.querySelector(".manager-timeline-entry:last-child .history-start")?.focus();
    }
    section.dataset.dirty = "true";
    updateManagerTimelinePreview(row);
  });
  $("#cancelManagerSettingsBtn")?.addEventListener("click", () => {
    lockSettingsSection("manager");
    managerSettingsDeletedIds.clear();
    const month = $("#goalMonthInput")?.value || currentDashboardMonth();
    list.innerHTML = sortManagersByDisplayOrder(state.managers).map((manager) => managerSettingsRowMarkup(manager, month)).join("");
    refreshManagerOrderNumbers();
    setSettingsSectionEditable("manager", false);
    showToast("저장하지 않은 매니저 수정을 취소했습니다.");
  });
}

installCalculationScopes();
init();

