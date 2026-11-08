// Current-user OS schedules, with a separate preview before registration.
import { driftHeldNote } from "./drift.js";
import { dsWithCode, locale, pick, tr } from "./i18n.js";
// "11-08 22:29" in Beijing time, or the raw text when it is not a timestamp.
function tickText(raw) {
  const at = Date.parse(raw || "");
  if (!Number.isFinite(at)) return raw || "";
  return new Intl.DateTimeFormat(locale(), {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
    timeZone: "Asia/Shanghai",
  }).format(new Date(at));
}

function paintSchedulerStatus(value, esc) {
  const el = document.getElementById("ops-status-scheduler");
  if (!el || !value) return;
  const native = value.native || {};
  let title = tr("已暂停 / 未启用", "Paused / not enabled");
  let detail = tr("尚无调度检查记录。", "No scheduler checks yet.");
  let tone = "muted";
  const last = value.last_tick?.at ? tr(`最近检查 ${tickText(value.last_tick.at)}`, `Last check ${tickText(value.last_tick.at)}`) : "";
  if (value.held) {
    title = tr("已锁定", "Locked");
    detail = tr("配置不一致，先处理页面顶部的提示。", "Config drift is holding this. See the notice at the top of the page.");
    tone = "warn";
  } else if (native.error) {
    title = tr("系统状态未知", "System status unknown");
    detail = native.error;
    tone = "bad";
  } else if (value.enabled) {
    title = tr("已启用", "Enabled");
    tone = "ok";
    if (value.timer_health === "stale") {
      detail = tr("超过 20 分钟未收到系统触发。", "No system trigger for over 20 minutes.");
      tone = "warn";
    } else if (last) detail = last;
    else detail = tr("已注册，等待系统触发。", "Registered; waiting for a system trigger.");
  } else if (value.timer_health === "stale") {
    detail = tr("超过 20 分钟未收到系统触发。", "No system trigger for over 20 minutes.");
  } else if (last) {
    detail = last;
  }
  el.innerHTML = `<span class="metric-label"><i class="ops-dot tone-${tone}" aria-hidden="true"></i>${tr("定时任务", "Schedule")}</span><strong>${esc(title)}</strong><p>${esc(detail)}</p>`;
}

export async function renderSchedule(ctx, home, alive) {
  const { api, esc } = ctx;
  const host = document.getElementById("ops-schedule");
  if (!host) return;
  const revision = (host.scheduleRevision || 0) + 1;
  host.scheduleRevision = revision;
  const pageAlive = alive;
  alive = () => pageAlive() && host.scheduleRevision === revision;
  const held = driftHeldNote(home, tr("定时任务", "Schedule"));
  if (held) {
    host.innerHTML = held;
    paintSchedulerStatus({ held: true }, esc);
    return;
  }
  let value;
  try {
    value = await api("/api/ops/schedule");
  } catch (err) {
    if (alive()) {
      host.innerHTML = `<p class="panel-note err">${tr("定时任务", "Schedule")}: ${esc(err.message)}</p>`;
      paintSchedulerStatus({ native: { error: err.message } }, esc);
    }
    return;
  }
  if (!alive()) return;
  paintSchedulerStatus(value, esc);
  const native = value.native || {};
  const writable = home.mode.ops_enabled;
  const disabled = writable ? "" : "disabled";
  const datasets = (value.backup_choices || []).map((name) => `<option value="${esc(name)}" ${(value.backup_datasets || []).includes(name) ? "selected" : ""}>${esc(dsWithCode(name))}</option>`).join("");
  // [[job.events.schedules]] in the config replace the single group/interval fields.
  const schedules = value.event_schedules || [];
  const describeSchedule = (item) => {
    const group = item.group ? esc(item.group) : tr("全部事件组", "all event groups");
    let text = tr(`${group} 每 ${esc(item.every_minutes)} 分钟`, `${group} every ${esc(item.every_minutes)} min`);
    if (item.jitter_seconds) text += tr(`，随机延后至多 ${esc(item.jitter_seconds)} 秒`, `, up to ${esc(item.jitter_seconds)}s later at random`);
    if (item.quiet_hours) text += tr(`，北京时间 ${esc(item.quiet_hours)} 点每 ${esc(Math.max(item.every_minutes, item.quiet_every_minutes))} 分钟`, `, every ${esc(Math.max(item.every_minutes, item.quiet_every_minutes))} min during ${esc(item.quiet_hours)} Beijing time`);
    return text;
  };
  const eventFields = schedules.length
    ? `<div class="panel-note">${tr("事件流计划（在配置的 [[job.events.schedules]] 中修改）：", "Event schedules (edit them in the config's [[job.events.schedules]]):")}<ul class="ops-list">${schedules.map((item) => `<li>${describeSchedule(item)}</li>`).join("")}</ul></div>`
    : null;
  const eventGroups = (value.events_groups || []).map((name) => `<option value="${esc(name)}" ${value.events_group === name ? "selected" : ""}>${esc(name)}</option>`).join("");
  const last = value.last_tick;
  const links = (last?.jobs || []).map((job) => `<a href="#/ops/jobs/${encodeURIComponent(job.job_id)}">${esc(job.job)} ${esc(job.session)}</a>`).join(" ");
  const status = native.error ? tr("系统状态未知", "System status unknown") : value.enabled ? tr("已启用", "Enabled") : tr("已暂停 / 未启用", "Paused / not enabled");
  const tickLabels = {
    idle: tr("已检查，当前无到期任务", "Checked; nothing due"),
    started: tr("已启动任务", "Started jobs"),
    waiting: tr("等待占用结束", "Waiting for the lake to free up"),
    error: tr("调度出错", "Scheduler error"),
    busy: tr("另一检查正在运行", "Another check is running"),
  };
  host.innerHTML = `<div class="panel-header"><h2>${tr("定时任务", "Schedule")}</h2><span class="status-pill">${esc(status)}</span></div>
    <p class="panel-note">${esc(value.backend)} ${esc(pick(value, "note"))}</p>
    ${native.error ? `<p class="panel-note err">${esc(native.error)}</p>` : ""}
    ${value.warning ? `<p class="panel-note err">${esc(value.warning)}</p>` : ""}
    ${value.timer_health === "stale" ? `<p class="panel-note err">${tr("超过 20 分钟未收到系统触发，请检查登录状态、休眠或 cron 服务，再刷新状态。", "No system trigger for over 20 minutes. Check the login session, sleep settings, or the cron service, then refresh the status.")}</p>` : ""}
    ${value.timer_health === "not_seen" ? `<p class="panel-note">${tr("已注册，等待首次系统检查（最多约 1 分钟）。", "Registered; waiting for the first system check (up to about 1 minute).")}</p>` : ""}
    ${native.installed && (!native.active || !native.matches) ? `<p class="panel-note err">${tr("系统任务未加载或定义与当前安装不符，请核对后重新设置。", "The system job is not loaded or does not match this installation. Check it and set the schedule again.")}</p>` : ""}
    ${last ? `<p class="panel-note">${tr(`最近检查 ${esc(tickText(last.at))}`, `Last check ${esc(tickText(last.at))}`)} ${esc(tickLabels[last.status] || last.status)} ${esc(last.message || "")} ${links}</p>` : `<p class="panel-note">${tr("尚无调度检查记录。", "No scheduler checks yet.")}</p>`}
    <form id="schedule-fields" class="ops-fields">
      <label class="ops-check"><input id="schedule-daily" type="checkbox" ${value.daily ? "checked" : ""} ${writable ? "" : "disabled"}> ${tr("自动日更（未选研究包时跑全部日更组与事件流）", "Automatic daily update (runs every daily update group and event stream when no research pack is selected)")}</label>
      <fieldset class="ops-fields">
        <legend>${tr("研究包（都不选则保持全部日更）", "Research packs (select none to keep the full daily update)")}</legend>
        <label class="ops-check"><input id="schedule-pack-market" type="checkbox" ${(value.daily_packs || []).includes("market") ? "checked" : ""} ${writable ? "" : "disabled"}> ${tr("行情", "Market data")}</label>
        <label class="ops-check"><input id="schedule-pack-fundamentals" type="checkbox" ${(value.daily_packs || []).includes("fundamentals") ? "checked" : ""} ${writable ? "" : "disabled"}> ${tr("基本面", "Fundamentals")}</label>
        <label class="ops-check"><input id="schedule-pack-universe" type="checkbox" ${(value.daily_packs || []).includes("universe") ? "checked" : ""} ${writable ? "" : "disabled"}> ${tr("股票池（无额外日更组）", "Universe (no extra daily update groups)")}</label>
      </fieldset>
      <label>${tr("日更时间（北京时间）", "Daily update time (Beijing time)")}<input id="schedule-daily-at" type="time" required value="${esc(value.daily_run_at)}" ${writable ? "" : "disabled"}></label>
      <label class="ops-check"><input id="schedule-stale" type="checkbox" ${value.stale ? "checked" : ""} ${writable ? "" : "disabled"}> ${tr("收尾补抓（落后的快照）", "Closing catch-up (lagging snapshots)")}</label>
      <label>${tr("补抓时间（北京时间）", "Catch-up time (Beijing time)")}<input id="schedule-stale-at" type="time" required value="${esc(value.stale_run_at)}" ${writable ? "" : "disabled"}></label>
      <label class="ops-check"><input id="schedule-backup" type="checkbox" ${value.backup ? "checked" : ""} ${disabled}> ${tr("每日数据备份（自然日）", "Daily data backup (calendar days)")}</label>
      <label>${tr("备份时间（北京时间）", "Backup time (Beijing time)")}<input id="schedule-backup-at" type="time" required value="${esc(value.backup_run_at || "23:00")}" ${disabled}></label>
      <label>${tr("备份数据集", "Backup datasets")}<select id="schedule-backup-datasets" multiple size="4" ${disabled}>${datasets}</select><small class="muted">${tr("只复制所选已发布数据及对应元数据，不包含配置、凭据或完整运行数据库。", "Copies only the selected published data and its metadata; excludes config, credentials, and the full run database.")}</small></label>
      <label>${tr("备份目录（绝对路径）", "Backup directory (absolute path)")}<input id="schedule-backup-root" value="${esc(value.backup_root)}" ${disabled}><small class="muted">${tr("建议选择外部磁盘。备份占用空间，不自动删除旧备份。", "An external disk is recommended. Backups take space; old backups are not deleted automatically.")}</small></label>
      <label class="ops-check"><input id="schedule-events" type="checkbox" ${value.events ? "checked" : ""} ${disabled}> ${tr("独立事件流（含周末和节假日）", "Standalone event stream (includes weekends and holidays)")}</label>
      ${eventFields || `<label>${tr("事件组", "Event group")}<select id="schedule-events-group" ${disabled}><option value="">${tr("请选择", "Select…")}</option>${eventGroups}</select></label>
      <label>${tr("事件流间隔（分钟）", "Event stream interval (minutes)")}<input id="schedule-events-interval" type="number" min="1" max="1440" required value="${esc(value.events_interval_minutes || 60)}" ${disabled}><small class="muted">${tr("按上次尝试开始时间计算间隔，任务占用时等待下一次检查。", "The interval counts from the last attempt's start; if the lake is busy, it waits for the next check.")}</small></label>`}
      <div class="action-row"><button class="button button-primary" type="submit" ${writable ? "" : "disabled"}>${tr("预览定时设置", "Preview schedule")}</button><button class="button button-ghost" id="schedule-refresh" type="button">${tr("刷新状态", "Refresh status")}</button></div>
    </form><div id="schedule-preview"></div>`;
  const form = document.getElementById("schedule-fields");
  const target = document.getElementById("schedule-preview");
  let previewGeneration = 0;
  form.oninput = () => {
    previewGeneration += 1;
    target.innerHTML = "";
  };
  document.getElementById("schedule-refresh").onclick = () => renderSchedule(ctx, home, pageAlive);
  form.onsubmit = async (event) => {
    event.preventDefault();
    const current = ++previewGeneration;
    target.innerHTML = `<p class="panel-note">${tr("正在预览…", "Previewing…")}</p>`;
    const settings = {
      daily: document.getElementById("schedule-daily").checked,
      stale: document.getElementById("schedule-stale").checked,
      daily_run_at: document.getElementById("schedule-daily-at").value,
      stale_run_at: document.getElementById("schedule-stale-at").value,
      backup: document.getElementById("schedule-backup").checked,
      backup_run_at: document.getElementById("schedule-backup-at").value || value.backup_run_at || "23:00",
      backup_datasets: [...(document.getElementById("schedule-backup-datasets").selectedOptions || [])].map((option) => option.value),
      backup_root: document.getElementById("schedule-backup-root").value.trim() || null,
      events: document.getElementById("schedule-events").checked,
      events_group: eventFields ? null : document.getElementById("schedule-events-group").value || null,
      events_interval_minutes: eventFields ? value.events_interval_minutes || 60 : Number(document.getElementById("schedule-events-interval").value || value.events_interval_minutes || 60),
      daily_packs: ["market", "fundamentals", "universe"].filter((name) => document.getElementById(`schedule-pack-${name}`).checked),
    };
    try {
      const preview = await api("/api/ops/schedule/preview", {
        method: "POST", headers: { "Content-Type": "application/json", "X-CNE-CSRF": home.csrf_token },
        body: JSON.stringify(settings),
      });
      if (!alive() || current !== previewGeneration) return;
      target.innerHTML = `<p class="panel-note">${esc(pick(preview, "action"))} ${esc(preview.backend)}${tr("，时间均为北京时间", ", Beijing time")}</p>
        <ul class="ops-list"><li>${tr("日更 ", "Daily update ")}${preview.daily ? esc(preview.daily_run_at) : tr("关闭", "off")}${preview.daily_packs?.length ? tr(`，研究包 ${esc(preview.daily_packs.join("、"))}`, `, packs ${esc(preview.daily_packs.join(", "))}`) : ""}${tr("；补抓 ", "; catch-up ")}${preview.stale ? esc(preview.stale_run_at) : tr("关闭", "off")}</li>
        <li>${tr("每日备份 ", "Daily backup ")}${preview.backup ? `${esc(preview.backup_run_at)} ${esc((preview.backup_datasets || []).map(dsWithCode).join(", "))} → ${esc(preview.backup_root)}` : tr("关闭", "off")}</li>
        <li>${tr("事件流 ", "Event stream ")}${!preview.events ? tr("关闭", "off") : schedules.length ? schedules.map(describeSchedule).join(tr("；", "; ")) : `${esc(preview.events_group)} ${tr(`每 ${esc(preview.events_interval_minutes)} 分钟`, `every ${esc(preview.events_interval_minutes)} min`)}`}</li></ul>
        <details><summary>${tr("查看系统任务定义", "View system job definition")}</summary><pre class="ops-log">${esc(preview.artifact || tr("移除本页面创建的定时触发。", "Removes the schedule triggers this page created."))}</pre></details>
        <p class="panel-note">${esc(pick(preview, "confirmation"))}</p>
        <div class="action-row"><button class="button button-primary" id="schedule-apply" type="button">${tr(`确认${esc(preview.action)}`, esc(pick(preview, "action")))}</button></div>
        <p class="panel-note" id="schedule-note"></p>`;
      const button = document.getElementById("schedule-apply");
      button.onclick = async () => {
        if (!alive() || current !== previewGeneration) return;
        button.disabled = true;
        try {
          await api("/api/ops/schedule/apply", {
            method: "POST", headers: { "Content-Type": "application/json", "X-CNE-CSRF": home.csrf_token },
            body: JSON.stringify({ token: preview.token, acknowledged: true }),
          });
          if (alive()) await renderSchedule(ctx, home, pageAlive);
        } catch (err) {
          if (alive()) document.getElementById("schedule-note").textContent = tr(`${err.message} 请刷新状态并重新预览。`, `${err.message} Refresh the status and preview again.`);
        }
      };
    } catch (err) {
      if (alive() && current === previewGeneration) target.innerHTML = `<p class="panel-note err">${esc(err.message)}</p>`;
    }
  };
}
