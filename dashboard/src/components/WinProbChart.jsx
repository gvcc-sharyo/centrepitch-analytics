import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts'
import styles from './Panel.module.css'

const CustomTooltip = ({ active, payload, label, playerAId, playerBId }) => {
  if (!active || !payload?.length) return null
  const prob = payload[0]?.value ?? 0.5
  const ahead = prob >= 0.5
  return (
    <div className={styles.tooltip}>
      <div className={styles.tooltipTitle}>Point {label}</div>
      <div className={styles.tooltipRow} style={{ color: ahead ? 'var(--accent-a)' : 'var(--accent-b)' }}>
        {ahead ? playerAId : playerBId} — {((ahead ? prob : 1 - prob) * 100).toFixed(1)}%
      </div>
    </div>
  )
}

export function WinProbChart({ data, playerAId, playerBId }) {
  if (!data?.length) return (
    <div className={styles.card}>
      <div className={styles.title}>Win Probability</div>
      <div className={styles.empty}>No data</div>
    </div>
  )

  const last      = data[data.length - 1]
  const finalProb = last?.win_prob_a ?? 0.5
  const aAhead    = finalProb >= 0.5
  const leaderPct = ((aAhead ? finalProb : 1 - finalProb) * 100).toFixed(0)
  const leaderColor = aAhead ? 'var(--accent-a)' : 'var(--accent-b)'
  const leader    = aAhead ? playerAId : playerBId

  return (
    <div className={`${styles.card} animate-in`} style={{ animationDelay: '0.1s' }}>
      <div className={styles.header}>
        <div className={styles.title} style={{ marginBottom: 0 }}>Win Probability</div>
        <div style={{ textAlign: 'right', lineHeight: 1.2 }}>
          <div style={{ fontFamily: 'var(--font-playfair)', fontSize: 22, fontWeight: 800, color: leaderColor }}>
            {leaderPct}%
          </div>
          <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2 }}>{leader}</div>
        </div>
      </div>

      <ResponsiveContainer width="100%" height={200}>
        <AreaChart data={data} margin={{ top: 8, right: 12, left: -24, bottom: 0 }}>
          <defs>
            {/* Green above 50%, blue below — gradient stop fixed at 50% of chart height */}
            <linearGradient id="winGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%"  stopColor="var(--accent-a)" stopOpacity={0.35} />
              <stop offset="50%" stopColor="var(--accent-a)" stopOpacity={0.04} />
              <stop offset="50%" stopColor="var(--accent-b)" stopOpacity={0.04} />
              <stop offset="100%" stopColor="var(--accent-b)" stopOpacity={0.35} />
            </linearGradient>
          </defs>
          <XAxis
            dataKey="point_number"
            tick={{ fill: 'var(--text-muted)', fontSize: 10 }}
            axisLine={false} tickLine={false}
          />
          <YAxis
            domain={[0, 1]}
            ticks={[0, 0.25, 0.5, 0.75, 1]}
            tickFormatter={v => `${(v * 100).toFixed(0)}%`}
            tick={{ fill: 'var(--text-muted)', fontSize: 10 }}
            axisLine={false} tickLine={false}
          />
          <Tooltip content={<CustomTooltip playerAId={playerAId} playerBId={playerBId} />} />
          <ReferenceLine
            y={0.5}
            stroke="var(--bg-border)"
            strokeDasharray="4 3"
            label={{ value: '50%', position: 'insideRight', fill: 'var(--text-muted)', fontSize: 9 }}
          />
          <Area
            type="monotone"
            dataKey="win_prob_a"
            stroke={aAhead ? 'var(--accent-a)' : 'var(--accent-b)'}
            strokeWidth={2}
            fill="url(#winGrad)"
            dot={false}
            activeDot={{ r: 4, strokeWidth: 0 }}
          />
        </AreaChart>
      </ResponsiveContainer>

      <div className={styles.legend}>
        <span className={styles.dotA} />
        <span className={styles.legendLabel}>{playerAId}</span>
        <span className={styles.dotB} style={{ marginLeft: 12 }} />
        <span className={styles.legendLabel}>{playerBId}</span>
        <span style={{ marginLeft: 10, fontSize: 10, color: 'var(--text-muted)', fontStyle: 'italic' }}>
          · line = {playerAId} win prob
        </span>
      </div>
    </div>
  )
}
