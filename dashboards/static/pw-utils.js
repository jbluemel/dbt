/* ============================================
   Purple Wave Dashboard Utilities
   Shared formatters, helpers, chart config
   ============================================ */

const PW = {
  GOAL: 10000,

  CHART_COLORS: [
    '#7C3AED', '#4F46E5', '#2563EB', '#0891B2',
    '#059669', '#D4A843', '#DB2777', '#7C3AED',
    '#EA580C', '#16A34A', '#E11D48', '#6366F1'
  ],

  // --- Formatters ---
  fmt(n) {
    return '$' + Math.round(n).toLocaleString('en-US');
  },

  fmtDec(n) {
    return '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  },

  fmtK(n) {
    if (n >= 1000000) return '$' + (n / 1000000).toFixed(1) + 'M';
    if (n >= 1000) return '$' + (n / 1000).toFixed(0) + 'k';
    return '$' + Math.round(n);
  },

  pct(n) {
    return (n * 100).toFixed(1) + '%';
  },

  num(n) {
    return n.toLocaleString('en-US');
  },

  // --- Status ---
  status(alv, goal) {
    goal = goal || PW.GOAL;
    if (alv >= goal) return 'hit';
    if (alv >= goal * 0.9) return 'near';
    return 'miss';
  },

  statusIcon(s) {
    if (s === 'hit') return '\u25b2';
    if (s === 'near') return '\u25cf';
    return '\u25bc';
  },

  gapPct(alv, goal) {
    goal = goal || PW.GOAL;
    return ((alv - goal) / goal * 100).toFixed(1);
  },

  gapLabel(alv, goal) {
    const g = parseFloat(PW.gapPct(alv, goal));
    return (g >= 0 ? '+' : '') + g + '% vs goal';
  },

  // --- Aggregation ---
  aggregate(rows) {
    const lots = rows.reduce((s, r) => s + r.lots, 0);
    const revenue = rows.reduce((s, r) => s + r.revenue, 0);
    return {
      lots,
      revenue,
      alv: lots > 0 ? revenue / lots : 0
    };
  },

  groupBy(rows, key) {
    const map = {};
    rows.forEach(r => {
      const k = r[key];
      if (!map[k]) map[k] = [];
      map[k].push(r);
    });
    return map;
  },

  // --- Chart.js Defaults (light theme) ---
  tooltipConfig() {
    return {
      backgroundColor: '#FFFFFF',
      titleColor: '#1F2937',
      bodyColor: '#6B7280',
      borderColor: '#E5E7EB',
      borderWidth: 1,
      padding: 12,
      titleFont: { family: 'DM Sans', weight: '600' },
      bodyFont: { family: 'JetBrains Mono', size: 12 }
    };
  },

  xAxisConfig() {
    return {
      grid: { display: false },
      ticks: { color: '#6B7280', font: { family: 'DM Sans', size: 11 } }
    };
  },

  yAxisConfig(formatter) {
    return {
      grid: { color: '#F3F4F6', drawBorder: false },
      ticks: {
        color: '#6B7280',
        font: { family: 'JetBrains Mono', size: 11 },
        callback: formatter || (v => PW.fmt(v))
      }
    };
  },

  doughnutLegendConfig() {
    return {
      position: 'bottom',
      labels: {
        color: '#6B7280',
        font: { family: 'DM Sans', size: 11 },
        padding: 12,
        usePointStyle: true,
        pointStyleWidth: 8
      }
    };
  },

  // --- Goal Line Plugin ---
  goalLinePlugin(goalValue, label) {
    goalValue = goalValue || PW.GOAL;
    label = label || PW.fmt(goalValue) + ' Goal';
    return {
      id: 'goalLine',
      afterDraw(chart) {
        const yScale = chart.scales.y;
        const y = yScale.getPixelForValue(goalValue);
        const ctx = chart.ctx;
        ctx.save();
        ctx.setLineDash([6, 4]);
        ctx.strokeStyle = '#D4A843';
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.moveTo(chart.chartArea.left, y);
        ctx.lineTo(chart.chartArea.right, y);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.fillStyle = '#D4A843';
        ctx.font = '600 10px "DM Sans"';
        ctx.textAlign = 'right';
        ctx.fillText(label, chart.chartArea.right, y - 6);
        ctx.restore();
      }
    };
  },

  // --- Bar Color by Goal ---
  barColorByGoal(values, goal) {
    goal = goal || PW.GOAL;
    return values.map(v =>
      v >= goal ? 'rgba(5, 150, 105, 0.75)' : 'rgba(220, 38, 38, 0.55)'
    );
  },

  // --- KPI Card HTML ---
  kpiCard(title, value, subtitle, statusClass) {
    const cls = statusClass ? ` ${statusClass}` : '';
    return `
      <div class="pw-kpi${cls}">
        <div class="pw-kpi-title">${title}</div>
        <div class="pw-kpi-value">${value}</div>
        <div class="pw-kpi-sub">${subtitle}</div>
      </div>`;
  },

  // --- Badge HTML ---
  badge(alv, goal) {
    const s = PW.status(alv, goal);
    return `<span class="pw-badge ${s}">${PW.statusIcon(s)} ${PW.gapLabel(alv, goal)}</span>`;
  },

  // --- ALV Cell with Dot ---
  alvCell(alv, goal) {
    const s = PW.status(alv, goal);
    return `<span class="pw-alv-cell"><span class="pw-dot ${s}"></span>${PW.fmtDec(alv)}</span>`;
  },

  // --- Revenue Bar ---
  revenueBar(value, max, status) {
    const pct = (value / max * 100).toFixed(0);
    const color = status === 'hit' ? 'var(--green)' : status === 'near' ? 'var(--amber)' : 'var(--chart-1)';
    return `<div class="pw-bar-inline">
      <div class="pw-bar-track"><div class="pw-bar-fill" style="width:${pct}%;background:${color}"></div></div>
    </div>`;
  }
};