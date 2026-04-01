import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from 'recharts'
import panelStyles from './Panel.module.css'
import styles from './ShotDonut.module.css'

const COLORS_A = ['#4ade80', '#22c55e', '#16a34a', '#15803d', '#166534', '#14532d']
const COLORS_B = ['#3b82f6', '#2563eb', '#1d4ed8', '#1e40af', '#1e3a8a', '#172554']

const CustomTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null
  const d = payload[0].payload
  return (
    <div className={panelStyles.tooltip}>
      <div className={panelStyles.tooltipTitle}>{d.name}</div>
      <div className={panelStyles.tooltipRow}>{d.count} shots · {d.pct}%</div>
      <div className={panelStyles.tooltipRow} style={{ color: 'var(--accent-warn)' }}>
        Win rate: {d.win_rate_pct}%
      </div>
    </div>
  )
}

function DonutCard({ label, shots, colors, accentVar }) {
  if (!shots?.length) return (
    <div className={styles.donutCard}>
      <div className={styles.playerName} style={{ color: `var(${accentVar})` }}>{label}</div>
      <div className={panelStyles.empty}>No shot data</div>
    </div>
  )

  const total = shots.reduce((s, d) => s + d.count, 0)
  const data = shots.map(s => ({
    name: s.shot_type.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
    value: s.count,
    count: s.count,
    pct: total > 0 ? ((s.count / total) * 100).toFixed(1) : 0,
    win_rate_pct: s.win_rate_pct,
  }))

  return (
    <div className={styles.donutCard}>
      <div className={styles.playerName} style={{ color: `var(${accentVar})` }}>{label}</div>
      <div className={styles.chartWrap}>
        <ResponsiveContainer width="100%" height={180}>
          <PieChart>
            <Pie
              data={data}
              cx="50%"
              cy="50%"
              innerRadius={52}
              outerRadius={80}
              paddingAngle={3}
              dataKey="value"
              strokeWidth={0}
            >
              {data.map((_, i) => (
                <Cell key={i} fill={colors[i % colors.length]} />
              ))}
            </Pie>
            <Tooltip content={<CustomTooltip />} />
          </PieChart>
        </ResponsiveContainer>
        <div className={styles.center}>
          <div className={styles.centerVal} style={{ color: `var(${accentVar})` }}>{total}</div>
          <div className={styles.centerLabel}>shots</div>
        </div>
      </div>
      <div className={styles.legend}>
        {data.slice(0, 5).map((d, i) => (
          <div key={d.name} className={styles.legendRow}>
            <span className={styles.legendDot} style={{ background: colors[i % colors.length] }} />
            <span className={styles.legendName}>{d.name}</span>
            <span className={styles.legendPct}>{d.pct}%</span>
            <span className={styles.legendWr}>{d.win_rate_pct}% wr</span>
          </div>
        ))}
      </div>
    </div>
  )
}

export function ShotDonut({ data, playerAId, playerBId }) {
  if (!data) return (
    <div className={panelStyles.card}>
      <div className={panelStyles.title}>Shot Distribution</div>
      <div className={panelStyles.empty}>No shot data</div>
    </div>
  )

  return (
    <div className={`${panelStyles.card} animate-in`} style={{ animationDelay: '0.22s' }}>
      <div className={panelStyles.title}>Shot Distribution</div>
      <div className={styles.grid}>
        <DonutCard label={playerAId} shots={data.player_a} colors={COLORS_A} accentVar="--accent-a" />
        <DonutCard label={playerBId} shots={data.player_b} colors={COLORS_B} accentVar="--accent-b" />
      </div>
    </div>
  )
}
