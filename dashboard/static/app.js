const REFRESH_MS = 5000;

const darkAxis = {
  axisLine: { lineStyle: { color: '#2a3a55' } },
  axisLabel: { color: '#8aa0bd' },
  splitLine: { lineStyle: { color: '#1f2b41' } },
};
const darkTooltip = { backgroundColor: '#1a2438', borderColor: '#2a3a55', textStyle: { color: '#e6edf3' } };

function initChart(id) {
  return echarts.init(document.getElementById(id), null, { renderer: 'canvas' });
}

const trendChart = initChart('chart-trend');
const cityChart = initChart('chart-city');
const ruleChart = initChart('chart-rules');
const latencyChart = initChart('chart-latency');

function updateStats(stats) {
  document.getElementById('c-total').textContent = stats.total;
  document.getElementById('c-5m').textContent = stats.last_5min;
  document.getElementById('c-1h').textContent = stats.last_1h;
  document.getElementById('c-rules').textContent = stats.rules;
  document.getElementById('updated').textContent = '更新于 ' + stats.updated_at;
}

function updateTrend(data) {
  trendChart.setOption({
    tooltip: Object.assign({ trigger: 'axis' }, darkTooltip),
    grid: { left: 40, right: 16, top: 20, bottom: 24 },
    xAxis: Object.assign({ type: 'category', data: data.labels, boundaryGap: false }, darkAxis),
    yAxis: Object.assign({ type: 'value', minInterval: 1 }, darkAxis),
    series: [{
      name: '告警数', type: 'line', smooth: true, symbol: 'circle', symbolSize: 5,
      data: data.values, areaStyle: { opacity: 0.25 },
      lineStyle: { color: '#4da3ff', width: 2 }, itemStyle: { color: '#4da3ff' },
    }],
  });
}

function updateCities(items) {
  cityChart.setOption({
    tooltip: Object.assign({ trigger: 'item' }, darkTooltip),
    legend: { textStyle: { color: '#8aa0bd' }, bottom: 0 },
    series: [{
      type: 'pie', radius: ['40%', '68%'], center: ['50%', '44%'],
      label: { color: '#c6d4e8' },
      data: items,
    }],
  });
}

function updateRules(items) {
  const names = items.map(i => i.rule_name + ' (' + i.rule_id + ')');
  const counts = items.map(i => i.count);
  ruleChart.setOption({
    tooltip: Object.assign({ trigger: 'axis' }, darkTooltip),
    grid: { left: 60, right: 20, top: 16, bottom: 30 },
    xAxis: Object.assign({ type: 'category', data: names }, darkAxis),
    yAxis: Object.assign({ type: 'value', minInterval: 1 }, darkAxis),
    series: [{
      name: '触发次数', type: 'bar', barWidth: '40%',
      data: counts, itemStyle: { color: '#5db9ff', borderRadius: [4, 4, 0, 0] },
    }],
  });
}

function updateTable(alerts) {
  const tbody = document.querySelector('#alert-table tbody');
  tbody.innerHTML = alerts.map(a => `
    <tr>
      <td>${a.trigger_time}</td>
      <td><span class="risk-tag">${a.rule_name}</span></td>
      <td>${a.user_id}</td>
      <td>${Number(a.total_amount).toFixed(2)}</td>
      <td style="color:#ffd08a;font-weight:600">${Number(a.risk_score).toFixed(2)}</td>
      <td>${a.city}</td>
      <td class="detail" title="${a.detail}">${a.detail}</td>
    </tr>`).join('');
}

function updateLatency(data) {
  const s = data.stats;
  const statText = s.count
    ? `均值 ${s.mean}s | P50 ${s.p50}s | P95 ${s.p95}s | P99 ${s.p99}s（样本 ${s.count}）`
    : '暂无告警样本';
  document.getElementById('latency-stats').textContent = statText;
  latencyChart.setOption({
    tooltip: Object.assign({ trigger: 'axis' }, darkTooltip),
    grid: { left: 40, right: 16, top: 20, bottom: 24 },
    xAxis: Object.assign({ type: 'category', data: data.labels, boundaryGap: false }, darkAxis),
    yAxis: Object.assign({ type: 'value' }, darkAxis),
    series: [{
      name: '平均延迟(s)', type: 'line', smooth: true, symbol: 'circle', symbolSize: 4,
      data: data.values, areaStyle: { opacity: 0.25 },
      lineStyle: { color: '#ffa05c', width: 2 }, itemStyle: { color: '#ffa05c' },
    }],
  });
}

// ---------- 规则管理 ----------
let rulesMap = {};            // rule_id -> rule（当前配置）
let sentRules = {};           // rule_id -> {rule, sentAt}（等待/已确认生效）
let selectedRuleId = null;

function fillRuleForm(rule) {
  document.getElementById('r-threshold').value = rule.threshold;
  document.getElementById('r-window').value = Math.round(rule.window_ms / 1000);
  document.getElementById('r-name').value = rule.rule_name;
  document.getElementById('r-weight').value = rule.weight;
  document.getElementById('r-enabled').checked = !!rule.enabled;
}

function populateRuleSelect() {
  const sel = document.getElementById('r-select');
  const ids = Object.keys(rulesMap);
  if (ids.length === 0) return;
  sel.innerHTML = ids.map(id =>
    `<option value="${id}">${id} · ${rulesMap[id].rule_name}</option>`).join('');
  if (!selectedRuleId || !rulesMap[selectedRuleId]) selectedRuleId = ids[0];
  sel.value = selectedRuleId;
  fillRuleForm(rulesMap[selectedRuleId]);
}

document.getElementById('r-select').addEventListener('change', (e) => {
  selectedRuleId = e.target.value;
  fillRuleForm(rulesMap[selectedRuleId]);
});

function renderRuleList() {
  const rows = Object.values(rulesMap).map(r => {
    const sent = sentRules[r.rule_id];
    let status, cls = '';
    if (sent && sent.applied) { status = '已生效 ✓'; cls = 'ok'; }
    else if (sent) { status = '已发送，等待生效…'; cls = 'wait'; }
    else { status = r.enabled ? '生效中' : '已停用'; cls = r.enabled ? 'ok' : 'off'; }
    return `<tr><td>${r.rule_id}</td><td>${r.rule_name}</td>
      <td>${r.threshold}</td><td>${Math.round(r.window_ms / 1000)}s</td>
      <td>${r.enabled ? '启用' : '停用'}</td><td>${r.weight}</td>
      <td class="${cls}">${status}</td></tr>`;
  }).join('');
  document.getElementById('rule-list').innerHTML =
    `<table><thead><tr><th>ID</th><th>规则名</th><th>阈值</th><th>窗口</th><th>状态</th><th>权重</th><th>广播状态</th></tr></thead><tbody>${rows}</tbody></table>`;
}

function markAppliedFromAlerts(alerts) {
  // 用最新告警确认广播已生效：出现 rule_id 相同且时间晚于发送时刻的告警即视为生效
  const now = new Date();
  for (const a of alerts) {
    const sent = sentRules[a.rule_id];
    if (!sent || sent.applied) continue;
    const t = new Date(a.trigger_time.replace(' ', 'T'));
    if (t >= sent.sentAt && (now - sent.sentAt) < 120000) {
      sent.applied = true;
    }
  }
  renderRuleList();
}

async function applyRule(overrides) {
  const id = selectedRuleId || (Object.keys(rulesMap)[0] || 'R001');
  const body = {
    rule_id: id,
    rule_name: document.getElementById('r-name').value || rulesMap[id]?.rule_name,
    rule_type: rulesMap[id]?.rule_type || 'CONSECUTIVE_HIGH_AMOUNT',
    threshold: parseFloat(document.getElementById('r-threshold').value) || 0,
    window_ms: (parseInt(document.getElementById('r-window').value, 10) || 30) * 1000,
    weight: parseFloat(document.getElementById('r-weight').value) || 0.5,
    enabled: document.getElementById('r-enabled').checked,
  };
  Object.assign(body, overrides);
  try {
    const res = await fetch('/api/rule', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (data.ok) {
      const r = data.rule;
      rulesMap[r.rule_id] = r;
      sentRules[r.rule_id] = { rule: r, sentAt: new Date(), applied: false };
      const state = r.enabled ? '启用' : '停用';
      document.getElementById('rule-status').textContent =
        `已下发 ${r.rule_name}(${r.rule_id}) v${r.version} 阈值=${r.threshold} ` +
        `窗口=${r.window_ms / 1000}s ${state} —— 等待 Flink 广播生效（约数秒后由新告警确认）`;
      renderRuleList();
    } else {
      document.getElementById('rule-status').textContent = '发送失败: ' + JSON.stringify(data);
    }
  } catch (e) {
    document.getElementById('rule-status').textContent = '发送异常: ' + e.message;
  }
}

document.getElementById('btn-apply').addEventListener('click', () => applyRule({}));
document.getElementById('btn-disable').addEventListener('click', () => applyRule({ enabled: false }));
document.getElementById('btn-enable').addEventListener('click', () => applyRule({ enabled: true }));

async function refresh() {
  try {
    const [stats, trend, cities, rules, latest, latency, rulesCur] = await Promise.all([
      fetch('/api/stats').then(r => r.json()),
      fetch('/api/trend').then(r => r.json()),
      fetch('/api/cities').then(r => r.json()),
      fetch('/api/rules').then(r => r.json()),
      fetch('/api/latest').then(r => r.json()),
      fetch('/api/latency').then(r => r.json()),
      fetch('/api/rules/current').then(r => r.json()),
    ]);
    updateStats(stats);
    updateTrend(trend);
    updateCities(cities);
    updateRules(rules);
    updateTable(latest);
    updateLatency(latency);

    // 规则面板：合并后端配置（含别处下发的版本）
    rulesCur.forEach(r => { rulesMap[r.rule_id] = r; });
    populateRuleSelect();
    markAppliedFromAlerts(latest);
  } catch (e) {
    console.error('刷新失败', e);
  }
}

refresh();
setInterval(refresh, REFRESH_MS);
window.addEventListener('resize', () => {
  trendChart.resize();
  cityChart.resize();
  ruleChart.resize();
  latencyChart.resize();
});