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
      <td>${a.city}</td>
      <td class="detail" title="${a.detail}">${a.detail}</td>
    </tr>`).join('');
}

async function refresh() {
  try {
    const [stats, trend, cities, rules, latest] = await Promise.all([
      fetch('/api/stats').then(r => r.json()),
      fetch('/api/trend').then(r => r.json()),
      fetch('/api/cities').then(r => r.json()),
      fetch('/api/rules').then(r => r.json()),
      fetch('/api/latest').then(r => r.json()),
    ]);
    updateStats(stats);
    updateTrend(trend);
    updateCities(cities);
    updateRules(rules);
    updateTable(latest);
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
});