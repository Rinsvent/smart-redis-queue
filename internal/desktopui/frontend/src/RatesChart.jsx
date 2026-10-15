const SERIES = [
  { key: 'get', label: 'get', color: '#1F6AA5' },
  { key: 'ack', label: 'ack', color: '#2E7D32' },
  { key: 'reject', label: 'reject', color: '#C62828' },
]

const TAG_COLORS = ['#6A1B9A', '#00838F', '#EF6C00', '#4527A0', '#00695C', '#AD1457']

/** Build continuous series with gap fill: rates→0, gauges→forward-fill. */
export function buildChartSeries(points, fromSec, toSec, watchSeries) {
  const bySec = new Map()
  for (const p of points || []) {
    bySec.set(p.sec, p)
  }
  const out = []
  let lastTags = {}
  for (const w of watchSeries || []) {
    for (const pt of w.points || []) {
      if (!lastTags[w.tag] && pt.n != null) lastTags[w.tag] = pt.n
    }
  }
  const watchBySec = {}
  for (const w of watchSeries || []) {
    watchBySec[w.tag] = new Map((w.points || []).map((p) => [p.sec, p.n]))
  }

  let ffTags = { ...lastTags }
  for (let sec = fromSec; sec <= toSec; sec++) {
    const p = bySec.get(sec)
    const row = {
      sec,
      get: p?.get || 0,
      ack: p?.ack || 0,
      reject: p?.reject || 0,
      tags: {},
    }
    if (p?.tags) {
      for (const [t, n] of Object.entries(p.tags)) {
        ffTags[t] = n
      }
    }
    for (const [t, n] of Object.entries(ffTags)) {
      row.tags[t] = n
    }
    for (const [tag, m] of Object.entries(watchBySec)) {
      if (m.has(sec)) {
        row.tags[tag] = m.get(sec)
        ffTags[tag] = m.get(sec)
      } else if (row.tags[tag] == null && ffTags[tag] != null) {
        row.tags[tag] = ffTags[tag]
      }
    }
    out.push(row)
  }
  return out
}

export default function RatesChart({ series, height = 140 }) {
  const w = 800
  const h = height
  const pad = { t: 8, r: 8, b: 18, l: 36 }
  const innerW = w - pad.l - pad.r
  const innerH = h - pad.t - pad.b
  const n = series.length
  if (n < 2) {
    return (
      <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center', opacity: 0.6 }}>
        No stats yet (enable worker SetStatsRetention / watch tags)
      </div>
    )
  }

  const tagKeys = []
  for (const row of series) {
    for (const t of Object.keys(row.tags || {})) {
      if (!tagKeys.includes(t)) tagKeys.push(t)
    }
  }

  let maxY = 1
  for (const row of series) {
    maxY = Math.max(maxY, row.get, row.ack, row.reject)
    for (const v of Object.values(row.tags || {})) maxY = Math.max(maxY, v)
  }

  const xAt = (i) => pad.l + (i / (n - 1)) * innerW
  const yAt = (v) => pad.t + innerH - (v / maxY) * innerH

  const line = (vals) =>
    vals
      .map((v, i) => `${i === 0 ? 'M' : 'L'}${xAt(i).toFixed(1)},${yAt(v).toFixed(1)}`)
      .join(' ')

  const legends = [
    ...SERIES.map((s) => ({ ...s, vals: series.map((r) => r[s.key] || 0) })),
    ...tagKeys.map((t, i) => ({
      key: `t:${t}`,
      label: t,
      color: TAG_COLORS[i % TAG_COLORS.length],
      vals: series.map((r) => r.tags?.[t] ?? 0),
    })),
  ]

  return (
    <div style={{ width: '100%' }}>
      <svg viewBox={`0 0 ${w} ${h}`} width="100%" height={height} preserveAspectRatio="none">
        <line
          x1={pad.l}
          y1={pad.t}
          x2={pad.l}
          y2={pad.t + innerH}
          stroke="currentColor"
          strokeOpacity="0.2"
        />
        <line
          x1={pad.l}
          y1={pad.t + innerH}
          x2={pad.l + innerW}
          y2={pad.t + innerH}
          stroke="currentColor"
          strokeOpacity="0.2"
        />
        <text x={4} y={pad.t + 10} fontSize="10" fill="currentColor" opacity="0.55">
          {maxY}
        </text>
        <text x={4} y={pad.t + innerH} fontSize="10" fill="currentColor" opacity="0.55">
          0
        </text>
        {legends.map((s) => (
          <path key={s.key} d={line(s.vals)} fill="none" stroke={s.color} strokeWidth="1.5" />
        ))}
      </svg>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '10px 14px', fontSize: 12, opacity: 0.85 }}>
        {legends.map((s) => (
          <span key={s.key} style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
            <span style={{ width: 10, height: 2, background: s.color, display: 'inline-block' }} />
            {s.label}
          </span>
        ))}
      </div>
    </div>
  )
}
