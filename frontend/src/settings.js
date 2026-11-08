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
          ? `<div class="ops-source-row">${field(spec, disabled, esc)}${sourceState(spec, sources, esc)}</div>`
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
  const specs = (value.sections || []).flatMap((section) => section.settings || []);
  let previewGeneration = 0;
  form.oninput = () => {
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

// Each source switch with what it is doing today: off, paused, tripped, missing
// its key, or on — plus today's requests and failed batches where known.
function sourceState(spec, sources, esc) {
  if (spec.kind !== "bool") return "";
  const breakers = sources?.breakers || [];
  const keys = sources?.keys || {};
  let tone = "ok";
  let word = tr("开启", "On");
  if (spec.id === "push2_paused") {
    tone = spec.value ? "warn" : "ok";
    word = spec.value ? tr("已暂停", "Paused") : tr("可用", "Available");
  } else if (!spec.value) {
    tone = "muted";
    word = tr("关闭", "Off");
  } else if (spec.id in keys && !keys[spec.id]) {
    tone = "bad";
    word = tr("缺少密钥，不会发请求", "No key; no request is sent");
  } else if (spec.id === "eastmoney" && breakers.length) {
    tone = "bad";
    word = tr(`今日熔断：${breakers.join("、")}`, `Tripped today: ${breakers.join(", ")}`);
  } else if (spec.id === "eastmoney" && sources?.push2_paused) {
    tone = "warn";
    word = tr("push2 暂停，其余可用", "push2 paused, the rest available");
  } else if (spec.id in keys) {
    word = tr("开启，密钥已配置", "On, key configured");
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
