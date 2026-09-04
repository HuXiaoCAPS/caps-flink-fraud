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

async function applyRule(overrides) {
  const body = {
    rule_id: 'R001',
    rule_name: document.getElementById('r-name').value || '连续大额交易',
    rule_type: 'CONSECUTIVE_HIGH_AMOUNT',
    threshold: parseFloat(document.getElementById('r-threshold').value) || 900,
    window_ms: (parseInt(document.getElementById('r-window').value, 10) || 30) * 1000,
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
      const state = r.enabled ? '启用' : '停用';
      document.getElementById('rule-status').textContent =
        `已${state}：${r.rule_name} 阈值=${r.threshold}元 窗口=${r.window_ms / 1000}s` +
        ` v${r.version} @ ${new Date().toLocaleTimeString()}（等待广播生效...）`;
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
    const [stats, trend, cities, rules, latest, latency] = await Promise.all([
      fetch('/api/stats').then(r => r.json()),
      fetch('/api/trend').then(r => r.json()),
      fetch('/api/cities').then(r => r.json()),
      fetch('/api/rules').then(r => r.json()),
      fetch('/api/latest').then(r => r.json()),
      fetch('/api/latency').then(r => r.json()),
    ]);
    updateStats(stats);
    updateTrend(trend);
    updateCities(cities);
    updateRules(rules);
    updateTable(latest);
    updateLatency(latency);
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