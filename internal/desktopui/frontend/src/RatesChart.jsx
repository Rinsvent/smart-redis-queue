import { useEffect, useMemo, useRef, useState } from 'react'

const SERIES = [
  { key: 'get', label: 'get', color: '#1F6AA5' },
  { key: 'ack', label: 'ack', color: '#2E7D32' },
  { key: 'reject', label: 'reject', color: '#C62828' },
]

const TAG_COLORS = ['#6A1B9A', '#00838F', '#EF6C00', '#4527A0', '#00695C', '#AD1457']

function normPoint(p) {
  if (!p || typeof p !== 'object') return null
  const sec = Number(p.sec ?? p.Sec)
  if (!Number.isFinite(sec)) return null
  const tagsIn = p.tags ?? p.Tags ?? {}
  const tags = {}
  for (const [k, v] of Object.entries(tagsIn)) {
    tags[k] = Number(v) || 0
  }
  return {
    sec,
    get: Number(p.get ?? p.Get) || 0,
    ack: Number(p.ack ?? p.Ack) || 0,
    reject: Number(p.reject ?? p.Reject) || 0,
    tags,
  }
}

function pointHasSignal(p) {
  if (!p) return false
  if (p.get || p.ack || p.reject) return true
  return Object.values(p.tags || {}).some((v) => v > 0)
}

/** Окно X: по реальным точкам, чтобы линии тянулись от левого края на всю ширину. */
export function resolveChartWindow(points, watchSeries, nowSec, maxWindowSec = 600) {
  let minSec = Infinity
  let maxSec = -Infinity
  for (const raw of points || []) {
    const p = normPoint(raw)
    if (!p || !pointHasSignal(p)) continue
    minSec = Math.min(minSec, p.sec)
    maxSec = Math.max(maxSec, p.sec)
  }
  for (const w of watchSeries || []) {
    for (const pt of w.points || []) {
      const s = Number(pt.sec)
      const n = Number(pt.n)
      if (!Number.isFinite(s) || !Number.isFinite(n)) continue
      minSec = Math.min(minSec, s)
      maxSec = Math.max(maxSec, s)
    }
  }
  if (!Number.isFinite(minSec)) {
    const to = nowSec
    return { from: to - Math.min(120, maxWindowSec), to }
  }
  const span = Math.max(1, maxSec - minSec)
  const pad = Math.max(3, Math.floor(span * 0.08))
  const live = nowSec - maxSec <= 45
  let to = live ? nowSec : maxSec + pad
  let from = minSec - pad
  if (to - from < 30) from = to - 30
  if (to - from > maxWindowSec) from = to - maxWindowSec
  return { from, to }
}

/** Build continuous series with gap fill: rates→0, gauges→forward-fill. */
export function buildChartSeries(points, fromSec, toSec, watchSeries) {
  const bySec = new Map()
  for (const raw of points || []) {
    const p = normPoint(raw)
    if (p) bySec.set(p.sec, p)
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
  const wrapRef = useRef(null)
  const [width, setWidth] = useState(0)
  // empty → show all; non-empty → only selected keys
  const [selected, setSelected] = useState(() => new Set())

  useEffect(() => {
    const el = wrapRef.current
    if (!el) return
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect?.width
      if (w && w > 0) setWidth(Math.floor(w))
    })
    ro.observe(el)
    setWidth(Math.floor(el.clientWidth) || 0)
    return () => ro.disconnect()
  }, [])

  const h = height
  const pad = { t: 8, r: 8, b: 18, l: 36 }
  const n = series.length

  const tagKeys = useMemo(() => {
    const keys = []
    for (const row of series) {
      for (const t of Object.keys(row.tags || {})) {
        if (!keys.includes(t)) keys.push(t)
      }
    }
    return keys
  }, [series])

  const legends = useMemo(() => {
    return [
      ...SERIES.map((s) => ({ ...s, vals: series.map((r) => r[s.key] || 0) })),
      ...tagKeys.map((t, i) => ({
        key: `t:${t}`,
        label: t,
        color: TAG_COLORS[i % TAG_COLORS.length],
        vals: series.map((r) => r.tags?.[t] ?? 0),
      })),
    ]
  }, [series, tagKeys])

  const visible = useMemo(() => {
    if (selected.size === 0) return legends
    return legends.filter((s) => selected.has(s.key))
  }, [legends, selected])

  const toggleLegend = (key) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  if (n < 2) {
    return (
      <div
        ref={wrapRef}
        style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center', opacity: 0.6 }}
      >
        No stats yet (enable worker SetStatsRetention / watch tags)
      </div>
    )
  }

  const w = Math.max(width || 0, 320)
  const innerW = w - pad.l - pad.r
  const innerH = h - pad.t - pad.b

  let maxY = 1
  for (const s of visible) {
    for (const v of s.vals) maxY = Math.max(maxY, v)
  }

  const xAt = (i) => pad.l + (i / (n - 1)) * innerW
  const yAt = (v) => pad.t + innerH - (v / maxY) * innerH

  const line = (vals) =>
    vals
      .map((v, i) => `${i === 0 ? 'M' : 'L'}${xAt(i).toFixed(1)},${yAt(v).toFixed(1)}`)
      .join(' ')

  return (
    <div ref={wrapRef} style={{ width: '100%', minWidth: 0 }}>
      <svg
        viewBox={`0 0 ${w} ${h}`}
        width="100%"
        height={height}
        style={{ display: 'block', width: '100%', height }}
        preserveAspectRatio="none"
      >
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
        {visible.map((s) => (
          <path key={s.key} d={line(s.vals)} fill="none" stroke={s.color} strokeWidth="1.5" />
        ))}
      </svg>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '10px 14px', fontSize: 12, marginTop: 4 }}>
        {legends.map((s) => {
          const on = selected.size === 0 || selected.has(s.key)
          return (
            <button
              key={s.key}
              type="button"
              onClick={() => toggleLegend(s.key)}
              title={
                selected.size === 0
                  ? 'Show only this series'
                  : on
                    ? 'Hide this series'
                    : 'Add this series'
              }
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 5,
                border: 'none',
                background: 'transparent',
                padding: '2px 4px',
                margin: 0,
                cursor: 'pointer',
                borderRadius: 4,
                opacity: on ? 1 : 0.35,
                color: 'inherit',
                font: 'inherit',
              }}
            >
              <span
                style={{
                  width: 12,
                  height: 3,
                  borderRadius: 1,
                  background: s.color,
                  display: 'inline-block',
                  opacity: on ? 1 : 0.4,
                }}
              />
              <span style={{ fontWeight: on && selected.size > 0 ? 600 : 400 }}>{s.label}</span>
            </button>
          )
        })}
      </div>
    </div>
  )
}
