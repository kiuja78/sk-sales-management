(() => {
  "use strict";

  const config = window.SK_SUPABASE_CONFIG || {};
  let client = null;
  let currentUser = null;
  let cloudHasRow = false;
  let cloudBaselineRevision = 0;
  let loginPromise = null;

  function enabled() {
    return Boolean(config.url && config.publishableKey);
  }

  function ensureClient() {
    if (client) return client;
    if (!enabled()) throw new Error("Supabase 연결 설정이 없습니다.");
    if (!window.supabase?.createClient) throw new Error("Supabase 라이브러리를 불러오지 못했습니다.");
    client = window.supabase.createClient(config.url, config.publishableKey, {
      auth: {
        persistSession: true,
        autoRefreshToken: true,
        detectSessionInUrl: true
      }
    });
    return client;
  }

  function injectUiStyle() {
    if (document.getElementById("skCloudStyle")) return;
    const style = document.createElement("style");
    style.id = "skCloudStyle";
    style.textContent = `
      .sk-cloud-login-layer{position:fixed;inset:0;z-index:2147483000;display:grid;place-items:center;background:rgba(10,24,19,.72);backdrop-filter:blur(8px);padding:22px}
      .sk-cloud-login-card{width:min(430px,100%);background:#fff;border-radius:24px;padding:28px;box-shadow:0 30px 90px rgba(0,0,0,.28);font-family:inherit;color:#18362b}
      .sk-cloud-login-brand{font-size:12px;font-weight:900;letter-spacing:.12em;color:#39755f;margin-bottom:8px}
      .sk-cloud-login-card h2{margin:0 0 8px;font-size:25px}
      .sk-cloud-login-desc{margin:0 0 22px;color:#61736c;font-size:14px;line-height:1.55}
      .sk-cloud-field{display:grid;gap:7px;margin:14px 0;font-size:13px;font-weight:800}
      .sk-cloud-field input{width:100%;box-sizing:border-box;border:1px solid #cfdad5;border-radius:12px;padding:12px 13px;font:inherit;outline:none}
      .sk-cloud-field input:focus{border-color:#4f8c74;box-shadow:0 0 0 3px rgba(79,140,116,.12)}
      .sk-cloud-login-button{width:100%;border:0;border-radius:12px;padding:13px 16px;margin-top:8px;background:#2f6b56;color:#fff;font-weight:900;cursor:pointer}
      .sk-cloud-login-button:disabled{opacity:.55;cursor:wait}
      .sk-cloud-login-error{min-height:20px;margin:10px 0 0;color:#b42318;font-size:12px;font-weight:700}
      .sk-cloud-login-help{margin-top:15px;color:#71827b;font-size:11px;line-height:1.5}
      .sk-cloud-status{position:fixed;right:14px;bottom:14px;z-index:10020;border:1px solid rgba(47,107,86,.22);background:rgba(255,255,255,.94);backdrop-filter:blur(8px);border-radius:999px;padding:8px 12px;box-shadow:0 8px 24px rgba(0,0,0,.12);font:700 11px/1.2 inherit;color:#2f6b56;cursor:pointer}
      .sk-cloud-status::before{content:"";display:inline-block;width:7px;height:7px;border-radius:50%;background:#25a36f;margin-right:7px;vertical-align:1px}
      @media(max-width:700px){.sk-cloud-status{bottom:76px;right:10px}.sk-cloud-login-card{padding:22px;border-radius:20px}}
    `;
    document.head.appendChild(style);
  }

  function renderStatus() {
    injectUiStyle();
    let badge = document.getElementById("skCloudStatus");
    if (!badge) {
      badge = document.createElement("button");
      badge.type = "button";
      badge.id = "skCloudStatus";
      badge.className = "sk-cloud-status";
      badge.title = "클릭하면 로그아웃할 수 있습니다.";
      badge.addEventListener("click", async () => {
        if (!window.confirm("SK 영업관리 시스템에서 로그아웃할까요?")) return;
        try { await ensureClient().auth.signOut(); } catch (_) {}
        location.reload();
      });
      document.body.appendChild(badge);
    }
    badge.textContent = currentUser ? "클라우드 연결됨" : "로그인 필요";
  }

  function loginUi() {
    injectUiStyle();
    let layer = document.getElementById("skCloudLoginLayer");
    if (layer) return layer;
    layer = document.createElement("div");
    layer.id = "skCloudLoginLayer";
    layer.className = "sk-cloud-login-layer";
    layer.innerHTML = `
      <section class="sk-cloud-login-card" role="dialog" aria-modal="true" aria-labelledby="skCloudLoginTitle">
        <div class="sk-cloud-login-brand">SK SALES MANAGEMENT</div>
        <h2 id="skCloudLoginTitle">로그인</h2>
        <p class="sk-cloud-login-desc">어느 PC에서 접속해도 동일한 SK 영업 데이터를 사용하기 위해 Supabase 계정으로 로그인합니다.</p>
        <label class="sk-cloud-field">이메일
          <input id="skCloudEmail" type="email" autocomplete="username" placeholder="이메일 주소">
        </label>
        <label class="sk-cloud-field">비밀번호
          <input id="skCloudPassword" type="password" autocomplete="current-password" placeholder="비밀번호">
        </label>
        <button class="sk-cloud-login-button" type="button" id="skCloudLoginButton">로그인</button>
        <div class="sk-cloud-login-error" id="skCloudLoginError"></div>
        <div class="sk-cloud-login-help">Supabase Authentication에 등록한 계정을 사용합니다. 비밀번호는 GitHub나 프로그램 코드에 저장되지 않습니다.</div>
      </section>
    `;
    document.body.appendChild(layer);
    return layer;
  }

  async function showLogin() {
    if (loginPromise) return loginPromise;
    loginPromise = new Promise((resolve, reject) => {
      const layer = loginUi();
      const email = layer.querySelector("#skCloudEmail");
      const password = layer.querySelector("#skCloudPassword");
      const button = layer.querySelector("#skCloudLoginButton");
      const errorBox = layer.querySelector("#skCloudLoginError");

      const submit = async () => {
        const emailValue = String(email.value || "").trim();
        const passwordValue = String(password.value || "");
        if (!emailValue || !passwordValue) {
          errorBox.textContent = "이메일과 비밀번호를 입력해 주세요.";
          return;
        }
        button.disabled = true;
        button.textContent = "로그인 중...";
        errorBox.textContent = "";
        try {
          const { data, error } = await ensureClient().auth.signInWithPassword({
            email: emailValue,
            password: passwordValue
          });
          if (error) throw error;
          currentUser = data?.user || data?.session?.user || null;
          if (!currentUser) throw new Error("로그인 정보를 확인하지 못했습니다.");
          layer.remove();
          renderStatus();
          resolve(currentUser);
        } catch (error) {
          console.error("[SK CLOUD] sign-in failed", error);
          errorBox.textContent = error?.message === "Invalid login credentials"
            ? "이메일 또는 비밀번호가 맞지 않습니다."
            : "로그인에 실패했습니다. 잠시 후 다시 시도해 주세요.";
          button.disabled = false;
          button.textContent = "로그인";
        }
      };

      button.addEventListener("click", submit);
      password.addEventListener("keydown", (event) => {
        if (event.key === "Enter") submit();
      });
      window.setTimeout(() => email.focus(), 0);
    });
    return loginPromise;
  }

  async function ensureSignedIn() {
    const supabaseClient = ensureClient();
    const { data, error } = await supabaseClient.auth.getSession();
    if (error) console.warn("[SK CLOUD] session read failed", error);
    currentUser = data?.session?.user || null;
    if (!currentUser) currentUser = await showLogin();
    renderStatus();
    return currentUser;
  }

  async function loadState() {
    await ensureSignedIn();
    const { data, error } = await ensureClient()
      .from("sk_app_state")
      .select("data, revision, updated_at")
      .eq("user_id", currentUser.id)
      .maybeSingle();
    if (error) throw error;
    if (!data) {
      cloudHasRow = false;
      cloudBaselineRevision = 0;
      return { exists: false, data: null, revision: 0, updatedAt: "" };
    }
    cloudHasRow = true;
    cloudBaselineRevision = Number(data.revision || 0);
    return {
      exists: true,
      data: data.data && typeof data.data === "object" ? data.data : null,
      revision: cloudBaselineRevision,
      updatedAt: String(data.updated_at || "")
    };
  }

  async function saveState(snapshot) {
    await ensureSignedIn();
    const nextRevision = Number(snapshot?.appMeta?.persistRevision || 0);
    const payload = {
      user_id: currentUser.id,
      data: snapshot,
      revision: nextRevision,
      updated_at: new Date().toISOString()
    };

    if (!cloudHasRow) {
      const { data, error } = await ensureClient()
        .from("sk_app_state")
        .insert(payload)
        .select("revision")
        .single();
      if (error) {
        if (error.code === "23505") return { ok: false, conflict: true };
        throw error;
      }
      cloudHasRow = true;
      cloudBaselineRevision = Number(data?.revision ?? nextRevision);
      return { ok: true, conflict: false, revision: cloudBaselineRevision };
    }

    const { data, error } = await ensureClient()
      .from("sk_app_state")
      .update({
        data: snapshot,
        revision: nextRevision,
        updated_at: payload.updated_at
      })
      .eq("user_id", currentUser.id)
      .eq("revision", cloudBaselineRevision)
      .select("revision");

    if (error) throw error;
    if (!Array.isArray(data) || data.length === 0) return { ok: false, conflict: true };
    cloudBaselineRevision = Number(data[0]?.revision ?? nextRevision);
    return { ok: true, conflict: false, revision: cloudBaselineRevision };
  }

  async function seedIfMissing(snapshot) {
    if (cloudHasRow) return { ok: true, skipped: true };
    return saveState(snapshot);
  }

  async function getStatus() {
    await ensureSignedIn();
    const { data, error } = await ensureClient()
      .from("sk_app_state")
      .select("revision, updated_at")
      .eq("user_id", currentUser.id)
      .maybeSingle();
    if (error) throw error;
    return {
      connected: true,
      exists: Boolean(data),
      revision: Number(data?.revision || 0),
      updatedAt: String(data?.updated_at || ""),
      userEmail: String(currentUser?.email || "")
    };
  }

  async function createBackup(snapshot, meta = {}) {
    await ensureSignedIn();
    const payload = {
      user_id: currentUser.id,
      data: snapshot,
      revision: Number(snapshot?.appMeta?.persistRevision || meta.revision || 0),
      reason: String(meta.reason || "manual"),
      reason_label: String(meta.reasonLabel || "수동 백업"),
      record_count: Number(meta.recordCount || 0),
      manager_count: Number(meta.managerCount || 0),
      data_count: Number(meta.dataCount || 0)
    };
    const { data, error } = await ensureClient()
      .from("sk_app_backups")
      .insert(payload)
      .select("id, created_at, revision")
      .single();
    if (error) throw error;
    return {
      id: String(data?.id || ""),
      createdAt: String(data?.created_at || ""),
      revision: Number(data?.revision || payload.revision)
    };
  }

  async function listBackups(limit = 200) {
    await ensureSignedIn();
    const safeLimit = Math.max(1, Math.min(1000, Number(limit || 200)));
    const { data, error } = await ensureClient()
      .from("sk_app_backups")
      .select("id, revision, reason, reason_label, record_count, manager_count, data_count, created_at")
      .eq("user_id", currentUser.id)
      .order("created_at", { ascending: false })
      .limit(safeLimit);
    if (error) throw error;
    return (Array.isArray(data) ? data : []).map((row) => ({
      id: String(row.id || ""),
      revision: Number(row.revision || 0),
      reason: String(row.reason || "manual"),
      reasonLabel: String(row.reason_label || "백업"),
      recordCount: Number(row.record_count || 0),
      managerCount: Number(row.manager_count || 0),
      dataCount: Number(row.data_count || 0),
      createdAt: String(row.created_at || "")
    }));
  }

  async function getBackup(id) {
    await ensureSignedIn();
    const { data, error } = await ensureClient()
      .from("sk_app_backups")
      .select("id, data, revision, reason, reason_label, record_count, manager_count, data_count, created_at")
      .eq("user_id", currentUser.id)
      .eq("id", id)
      .maybeSingle();
    if (error) throw error;
    if (!data) return null;
    return {
      id: String(data.id || ""),
      data: data.data && typeof data.data === "object" ? data.data : null,
      revision: Number(data.revision || 0),
      reason: String(data.reason || "manual"),
      reasonLabel: String(data.reason_label || "백업"),
      recordCount: Number(data.record_count || 0),
      managerCount: Number(data.manager_count || 0),
      dataCount: Number(data.data_count || 0),
      createdAt: String(data.created_at || "")
    };
  }

  async function cleanupBackups(retentionDays = 30, maxBackups = 100) {
    await ensureSignedIn();
    const rows = await listBackups(1000);
    const cutoff = Date.now() - Math.max(1, Number(retentionDays || 30)) * 86400000;
    const keepMax = Math.max(1, Number(maxBackups || 100));
    const removeIds = rows
      .filter((row, index) => {
        const t = Date.parse(row.createdAt || "");
        return index >= keepMax || (Number.isFinite(t) && t < cutoff);
      })
      .map((row) => row.id)
      .filter(Boolean);
    if (!removeIds.length) return 0;
    const { error } = await ensureClient()
      .from("sk_app_backups")
      .delete()
      .eq("user_id", currentUser.id)
      .in("id", removeIds);
    if (error) throw error;
    return removeIds.length;
  }

  async function signOut() {
    if (!client) return;
    await client.auth.signOut();
    currentUser = null;
    cloudHasRow = false;
    cloudBaselineRevision = 0;
  }

  window.SKCloud = Object.freeze({
    enabled,
    ensureSignedIn,
    loadState,
    saveState,
    seedIfMissing,
    getStatus,
    createBackup,
    listBackups,
    getBackup,
    cleanupBackups,
    signOut,
    user: () => currentUser
  });
})();
