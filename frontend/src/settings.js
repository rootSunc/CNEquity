// Lake switches. A preview writes nothing; apply updates the config file only.
import { driftHeldNote } from "./drift.js";
import { ds, pick, tr } from "./i18n.js";
export async function renderSettings(ctx, home, alive, flash = "") {
  const { api, esc } = ctx;
  const host = document.getElementById("ops-settings");
  if (!host) return;
  const revision = (host.settingsRevision || 0) + 1;
  host.settingsRevision = revision;
  const pageAlive = alive;
  alive = () => pageAlive() && host.settingsRevision === revision;
  const held = driftHeldNote(home, tr("取数设置", "Fetch settings"));
  if (held) {
    host.innerHTML = held;
    return;
  }
  let value;
  try {
    value = await api("/api/ops/settings");
  } catch (err) {
    if (alive()) host.innerHTML = `<p class="panel-note err">${tr("取数设置", "Fetch settings")}: ${esc(err.message)}</p>`;
    return;
  }
  if (!alive()) return;
  const writable = home.mode.ops_enabled;
  const disabled = writable ? "" : "disabled";
  const sources = home.occupancy?.sources;
  const sections = (value.sections || [])
    .map((section) => {
      const lit = SOURCE_SECTIONS.has(section.id);
      const fields = (section.settings || [])
        .map((spec) => (lit
          ? `<div class="ops-source-row">${field(spec, disabled, esc)}<span class="ops-source-status" id="source-state-${esc(spec.id)}">${sourceState(spec, sources, esc, writable)}</span></div>`
          : field(spec, disabled, esc)))
        .join("");
      return `<h3>${esc(pick(section, "title"))}</h3>${lit ? `<div class="ops-source-list">${fields}</div>` : fields}`;
    })
    .join("");
  host.innerHTML = `<div class="panel-header"><h2>${tr("数据源与取数设置", "Sources and fetch settings")}</h2></div>
    <p class="panel-note">${esc(pick(value, "note") || "")}</p>
    ${value.push2_env_note ? `<p class="panel-note">${esc(pick(value, "push2_env_note"))}</p>` : ""}
    ${writable ? "" : `<p class="panel-note">${tr("当前模式不能修改这些设置。", "This mode cannot change these settings.")}</p>`}
    ${flash ? `<p class="panel-note" id="settings-saved">${esc(flash)}</p>` : `<p class="panel-note" id="settings-saved" hidden></p>`}
    <form id="settings-fields" class="ops-fields">${sections}
      <div class="action-row"><button class="button button-primary" type="submit" ${disabled}>${tr("预览取数设置", "Preview fetch settings")}</button></div>
    </form><div id="settings-preview"></div>`;
  const form = document.getElementById("settings-fields");
  const target = document.getElementById("settings-preview");
  const specById = Object.fromEntries((value.sections || []).flatMap((section) => section.settings || []).map((spec) => [spec.id, spec]));
  const repaint = (source) => {
    const slot = document.getElementById(`source-state-${source}`);
    if (slot && specById[source]) slot.innerHTML = sourceState(specById[source], sources, esc, writable);
  };
  form.onkeydown = (event) => {
    // Enter in a key box saves that key; it must not submit the settings preview.
    const input = event.target?.closest?.("[data-key-input]");
    if (input && event.key === "Enter") {
      event.preventDefault();
      document.querySelector(`[data-save-key="${input.dataset.keyInput}"]`)?.click();
    }
  };
  form.onclick = async (event) => {
    const hit = event.target.closest?.("[data-check-key], [data-edit-key], [data-save-key], [data-remove-key]");
    const data = hit?.dataset || {};
    const edit = data.editKey ? hit : null;
    if (edit && writable) {
      const editor = document.getElementById(`key-editor-${edit.dataset.editKey}`);
      if (editor) {
        editor.hidden = !editor.hidden;
        if (!editor.hidden) editor.querySelector("input")?.focus();
      }
      return;
    }
    const save = data.saveKey ? hit : null;
    if (save && writable) {
      const source = save.dataset.saveKey;
      const input = document.querySelector(`[data-key-input="${source}"]`);
      const note = document.getElementById(`key-note-${source}`);
      const key = (input?.value || "").trim();
      if (!key) return;
      save.disabled = true;
      if (note) note.textContent = tr("正在保存并检测…", "Saving and checking…");
      try {
        const saved = await api(`/api/ops/sources/${encodeURIComponent(source)}/key`, {
          method: "PUT",
          headers: { "Content-Type": "application/json", "X-CNE-CSRF": home.csrf_token },
          body: JSON.stringify({ key }),
        });
        if (input) input.value = "";
        if (!alive() || !sources) return;
        sources.keys = { ...(sources.keys || {}), [source]: true };
        sources.key_origins = { ...(sources.key_origins || {}), [source]: saved.overridden_by_env ? "env" : "file" };
        sources.key_hints = { ...(sources.key_hints || {}), [source]: key.length >= 12 ? `…${key.slice(-4)}` : "…" };
        sources.key_checks = { ...(sources.key_checks || {}), [source]: saved.check };
        repaint(source);
        repaint("tushare_daily");
        const after = document.getElementById(`key-note-${source}`);
        if (after) {
          after.textContent = saved.overridden_by_env
            ? tr(`已写入 ${saved.key_file}；但环境变量 ${saved.env} 仍然优先生效。`, `Written to ${saved.key_file}, but the environment variable ${saved.env} still takes precedence.`)
            : tr(`已写入 ${saved.key_file}（仅本机用户可读），配置文件只记录这个路径。`, `Written to ${saved.key_file} (readable by you only); the config file records only that path.`);
        }
      } catch (err) {
        if (note && alive()) note.textContent = err.message;
      } finally {
        save.disabled = false;
      }
      return;
    }
    const remove = data.removeKey ? hit : null;
    if (remove && writable) {
      const source = remove.dataset.removeKey;
      remove.disabled = true;
      try {
        const removed = await api(`/api/ops/sources/${encodeURIComponent(source)}/key`, {
          method: "DELETE",
          headers: { "X-CNE-CSRF": home.csrf_token },
        });
        if (!alive() || !sources) return;
        const status = await api("/api/ops/status");
        if (!alive()) return;
        Object.assign(sources, status.occupancy?.sources || {});
        repaint(source);
        repaint("tushare_daily");
        const note = document.getElementById(`key-note-${source}`);
        if (note && removed.overridden_by_env) note.textContent = tr(`环境变量 ${removed.env} 仍提供密钥。`, `The environment variable ${removed.env} still provides a key.`);
      } catch (err) {
        const note = document.getElementById(`key-note-${source}`);
        if (note && alive()) note.textContent = err.message;
        remove.disabled = false;
      }
      return;
    }
    const button = data.checkKey ? hit : null;
    if (!button || !writable) return;
    const source = button.dataset.checkKey;
    button.disabled = true;
    button.textContent = tr("正在检测…", "Checking…");
    let result;
    try {
      result = await api(`/api/ops/sources/${encodeURIComponent(source)}/check-key`, {
        method: "POST",
        headers: { "X-CNE-CSRF": home.csrf_token },
      });
    } catch (err) {
      result = { state: "error", message: err.message, checked_at: null };
    }
    if (!alive()) return;
    if (sources) sources.key_checks = { ...(sources.key_checks || {}), [source]: result };
    repaint(source);
    repaint("tushare_daily");
  };
  const specs = (value.sections || []).flatMap((section) => section.settings || []);
  let previewGeneration = 0;
  form.oninput = (event) => {
    // Typing a key is not a settings edit; it must not drop the preview.
    if (event?.target?.closest?.("[data-key-input]")) return;
    previewGeneration += 1;
    target.innerHTML = "";
  };
  form.onsubmit = async (event) => {
    event.preventDefault();
    if (!writable) return;
    const current = ++previewGeneration;
    target.innerHTML = `<p class="panel-note">${tr("正在预览…", "Previewing…")}</p>`;
    const values = {};
    for (const spec of specs) {
      const input = document.getElementById(`setting-${spec.id}`);
      values[spec.id] = spec.kind === "bool" ? Boolean(input.checked) : input.value;
    }
    try {
      const preview = await api("/api/ops/settings/preview", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CNE-CSRF": home.csrf_token },
        body: JSON.stringify({ values }),
      });
      if (!alive() || current !== previewGeneration) return;
      if (!preview.token) {
        target.innerHTML = `<p class="panel-note">${esc(pick(preview, "note") || tr("没有需要保存的改动。", "Nothing to save."))}</p>`;
        return;
      }
      const items = (preview.changes || [])
        .map((change) => `<li>${esc(pick(change, "label"))}${tr("：", ": ")}${esc(pick(change, "before_label"))} → ${esc(pick(change, "after_label"))}</li>`)
        .join("");
      const count = (preview.changes || []).length;
      target.innerHTML = `<p class="panel-note">${esc(pick(preview, "note") || "")}</p>
        ${preview.push2_env_note ? `<p class="panel-note">${esc(preview.push2_env_note)}</p>` : ""}
        <ul class="ops-list ops-changes">${items}</ul>
        <p class="panel-note">${esc(pick(preview, "acknowledgement"))}</p>
        <div class="action-row"><button class="button button-primary" id="settings-apply" type="button">${tr(`保存这 ${count} 项改动`, `Save these ${count} changes`)}</button></div>
        <p class="panel-note" id="settings-note"></p>`;
      const button = document.getElementById("settings-apply");
      button.onclick = async () => {
        if (!alive() || current !== previewGeneration) return;
        button.disabled = true;
        try {
          const saved = await api("/api/ops/settings/apply", {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-CNE-CSRF": home.csrf_token },
            body: JSON.stringify({ token: preview.token, acknowledged: true }),
          });
          if (alive()) {
            await renderSettings(
              ctx,
              home,
              pageAlive,
              tr(
                `已保存。原文件备份为 ${saved.backup_name}。这次没有启动取数。`,
                `Saved. The original file was backed up as ${saved.backup_name}. No fetching was started.`,
              ),
            );
          }
        } catch (err) {
          if (alive()) document.getElementById("settings-note").textContent = tr(`${err.message} 请重新预览。`, `${err.message} Preview again.`);
        }
      };
    } catch (err) {
      if (alive() && current === previewGeneration) target.innerHTML = `<p class="panel-note err">${esc(err.message)}</p>`;
    }
  };
}

const SOURCE_SECTIONS = new Set(["sources", "licensed"]);
const PAID = { tushare: true, ths_official: true };

// The registry source each switch controls; today's failures are counted by it.
const SWITCH_SOURCE = {
  tdx: "tdx_protocol",
  eastmoney: "eastmoney",
  sina_bars: "sina",
  baostock: "baostock",
  ths: "ths",
  cninfo: "cninfo",
  tushare: "tushare",
  ths_official: "ths_official",
};

// A paid source: its switch, whether a key is configured, and the last check
// of that key. The check sends one small request; it never runs by itself.
export function paidSourceState(spec, sources, esc, writable = false) {
  const configured = Boolean(sources?.keys?.[spec.id]);
  const check = sources?.key_checks?.[spec.id];
  const when = check?.checked_at ? checkedAt(check.checked_at) : "";
  let tone = "muted";
  let word;
  if (!configured) {
    tone = spec.value ? "bad" : "muted";
    word = spec.value ? tr("未配置密钥，不会发请求", "No key; no request is sent") : tr("关闭 · 未配置密钥", "Off · no key");
  } else if (!check) {
    tone = spec.value ? "warn" : "muted";
    word = spec.value ? tr("开启 · 密钥未检测", "On · key not checked") : tr("关闭 · 密钥未检测", "Off · key not checked");
  } else {
    const prefix = spec.value ? tr("开启", "On") : tr("关闭", "Off");
    const verdict = {
      ok: [tr("密钥可用", "Key accepted"), "ok"],
      limited: [tr("密钥有效，套餐不含检测接口", "Key valid; the probe is outside its plan"), "ok"],
      rejected: [tr("密钥被拒绝", "Key rejected"), "bad"],
      error: [tr("检测没有结果", "Check inconclusive"), "warn"],
    }[check.state] || [tr("检测没有结果", "Check inconclusive"), "warn"];
    word = `${prefix} · ${verdict[0]}`;
    tone = spec.value || check.state === "rejected" ? verdict[1] : "muted";
  }
  const facts = [];
  if (check && when) facts.push(tr(`检测于 ${when}`, `checked ${when}`));
  if (check && check.state !== "ok" && check.message) facts.push(check.message);
  if (configured && check?.state === "rejected" && spec.id === "tushare") {
    facts.push(tr("它的数据集不计入落后", "Its datasets are not counted as stale"));
  }
  const failed = sources?.failures?.[SWITCH_SOURCE[spec.id]];
  if (failed?.batches) {
    const names = failed.datasets.slice(0, 3).map((name) => ds(name)).join(tr("、", ", "));
    facts.push(tr(`今日失败 ${failed.batches} 批：${names}`, `${failed.batches} failed ${failed.batches === 1 ? "batch" : "batches"} today: ${names}`));
  }
  if (configured) {
    const origin = sources?.key_origins?.[spec.id];
    const hint = sources?.key_hints?.[spec.id];
    const where = {
      env: tr("来自环境变量", "from an environment variable"),
      file: tr("来自密钥文件", "from a key file"),
      inline: tr("明文写在配置文件里，建议改存密钥文件", "written inline in the config file; a key file is safer"),
    }[origin];
    if (where) facts.unshift(hint ? `${hint} · ${where}` : where);
  }
  const buttons = [];
  if (configured && writable) {
    buttons.push(`<button class="button button-ghost" type="button" data-check-key="${esc(spec.id)}">${check ? tr("重新检测", "Check again") : tr("检测密钥", "Check key")}</button>`);
  }
  if (writable) {
    buttons.push(`<button class="button button-ghost" type="button" data-edit-key="${esc(spec.id)}">${configured ? tr("更换密钥", "Replace key") : tr("填写密钥", "Enter key")}</button>`);
    if (configured && sources?.key_origins?.[spec.id] === "file") {
      buttons.push(`<button class="button button-ghost" type="button" data-remove-key="${esc(spec.id)}">${tr("移除", "Remove")}</button>`);
    }
  }
  const editor = writable
    ? `<span class="ops-key-editor" id="key-editor-${esc(spec.id)}" hidden><input type="password" autocomplete="off" spellcheck="false" data-key-input="${esc(spec.id)}" aria-label="${esc(tr("密钥", "Key"))}" placeholder="${esc(tr("粘贴密钥；只保存在本机密钥文件", "Paste the key; kept only in a local key file"))}"><button class="button button-primary" type="button" data-save-key="${esc(spec.id)}">${tr("保存并检测", "Save and check")}</button></span>`
    : "";
  const detail = facts.length ? `<small class="ops-source-facts">${esc(facts.join(tr("；", "; ")))}</small>` : "";
  return `<span class="ops-source-state"><i class="ops-dot tone-${tone}" aria-hidden="true"></i>${esc(word)}${buttons.join("")}</span>${detail}${editor}<small class="ops-source-facts" id="key-note-${esc(spec.id)}"></small>`;
}

// Tushare's daily switch: asked for, and whether the key lets it happen today.
function tushareDailyState(spec, sources, esc) {
  const check = sources?.key_checks?.tushare;
  const usable = check && (check.state === "ok" || check.state === "limited");
  let tone = "muted";
  let word = tr("关闭 · 只用于回填", "Off · backfill only");
  if (spec.value && usable) {
    tone = "ok";
    word = tr("开启 · 日更包含 Tushare 数据集", "On · the daily update includes Tushare datasets");
  } else if (spec.value) {
    tone = "warn";
    word = tr("已开启，但密钥未通过检测，日更暂不包含", "On, but the key has not passed its check; not in the daily update yet");
  }
  return `<span class="ops-source-state"><i class="ops-dot tone-${tone}" aria-hidden="true"></i>${esc(word)}</span>`;
}

function checkedAt(iso) {
  const at = new Date(iso);
  if (Number.isNaN(at.getTime())) return "";
  const text = at.toLocaleString("sv-SE", { timeZone: "Asia/Shanghai" });
  return text.slice(5, 16);
}

// Each source switch with what it is doing today: off, paused, tripped, missing
// its key, or on — plus today's requests and failed batches where known.
function sourceState(spec, sources, esc, writable = false) {
  if (spec.kind !== "bool") return "";
  if (spec.id === "tushare_daily") return tushareDailyState(spec, sources, esc);
  if (spec.id in PAID) return paidSourceState(spec, sources, esc, writable);
  const breakers = sources?.breakers || [];
  let tone = "ok";
  let word = tr("开启", "On");
  if (spec.id === "push2_paused") {
    tone = spec.value ? "warn" : "ok";
    word = spec.value ? tr("已暂停", "Paused") : tr("可用", "Available");
  } else if (!spec.value) {
    tone = "muted";
    word = tr("关闭", "Off");
  } else if (spec.id === "eastmoney" && breakers.length) {
    tone = "bad";
    word = tr(`今日熔断：${breakers.join("、")}`, `Tripped today: ${breakers.join(", ")}`);
  } else if (spec.id === "eastmoney" && sources?.push2_paused) {
    tone = "warn";
    word = tr("push2 暂停，其余可用", "push2 paused, the rest available");
  }
  const facts = [];
  if (spec.id === "eastmoney" || spec.id === "push2_paused") {
    const hosts = Object.entries(sources?.requests || {}).filter(([name]) => (spec.id === "push2_paused" ? name === "push2" : name !== "push2"));
    for (const [name, item] of hosts) {
      facts.push(item.budget
        ? tr(`${name} 今日请求 ${item.requests}/${item.budget}`, `${name} ${item.requests}/${item.budget} requests today`)
        : tr(`${name} 今日请求 ${item.requests}`, `${name} ${item.requests} requests today`));
    }
  }
  const failed = sources?.failures?.[SWITCH_SOURCE[spec.id]];
  if (failed?.batches) {
    const names = failed.datasets.slice(0, 3).map((name) => ds(name)).join(tr("、", ", "));
    facts.push(tr(`今日失败 ${failed.batches} 批：${names}`, `${failed.batches} failed ${failed.batches === 1 ? "batch" : "batches"} today: ${names}`));
    if (tone === "ok") tone = "warn";
  }
  const detail = facts.length ? `<small class="ops-source-facts">${esc(facts.join(tr("；", "; ")))}</small>` : "";
  return `<span class="ops-source-state"><i class="ops-dot tone-${tone}" aria-hidden="true"></i>${esc(word)}</span>${detail}`;
}

function field(spec, disabled, esc) {
  const scope = spec.scope ? `<small class="muted">${esc(pick(spec, "scope"))}</small>` : "";
  const help = spec.help ? `<small class="muted">${esc(pick(spec, "help"))}</small>` : "";
  if (spec.kind === "bool") {
    return `<label class="ops-check"><input id="setting-${esc(spec.id)}" type="checkbox" ${spec.value ? "checked" : ""} ${disabled}> ${esc(pick(spec, "label"))}</label>${help}${scope}`;
  }
  const options = (spec.choices || [])
    .map((choice) => `<option value="${esc(choice.value)}" ${choice.value === spec.value ? "selected" : ""}>${esc(pick(choice, "label"))}</option>`)
    .join("");
  return `<label>${esc(pick(spec, "label"))}<select id="setting-${esc(spec.id)}" ${disabled}>${options}</select></label>${help}${scope}`;
}
