import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import styles from './Panel.module.css'

export function ShotStats({ data, playerAId, playerBId }) {
  if (!data) return <div className={styles.card}><div className={styles.title}>Shot Effectiveness</div><div className={styles.empty}>No shot data</div></div>

  const allShots = new Set([
    ...(data.player_a || []).map(s => s.shot_type),
    ...(data.player_b || []).map(s => s.shot_type),
  ])

  const aMap = Object.fromEntries((data.player_a || []).map(s => [s.shot_type, s]))
  const bMap = Object.fromEntries((data.player_b || []).map(s => [s.shot_type, s]))

  const chartData = [...allShots].map(shot => ({
    shot: shot.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
    [playerAId]: aMap[shot]?.count ?? 0,
    [playerBId]: bMap[shot]?.count ?? 0,
  })).sort((a, b) => (b[playerAId] + b[playerBId]) - (a[playerAId] + a[playerBId]))

  return (
    <div className={`${styles.card} animate-in`} style={{ animationDelay: '0.2s' }}>
      <div className={styles.header}>
        <div className={styles.title}>Shot Effectiveness</div>
        <div className={styles.legend}>
          <span className={styles.dotA} /><span className={styles.legendLabel}>{playerAId}</span>
          <span className={styles.dotB} /><span className={styles.legendLabel}>{playerBId}</span>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={chartData} margin={{ top: 8, right: 8, left: -20, bottom: 40 }} barGap={3}>
          <XAxis dataKey="shot" tick={{ fill: 'var(--text-secondary)', fontSize: 10 }} axisLine={false} tickLine={false} angle={-28} textAnchor="end" interval={0} />
          <YAxis tick={{ fill: 'var(--text-muted)', fontSize: 10 }} axisLine={false} tickLine={false} allowDecimals={false} />
          <Tooltip
            contentStyle={{ background: 'var(--bg-raised)', border: '1px solid var(--bg-border)', borderRadius: '6px' }}
            cursor={{ fill: 'rgba(0,0,0,0.03)' }}
          />
          <Bar dataKey={playerAId} fill="var(--accent-a)" radius={[4,4,0,0]} maxBarSize={24} />
          <Bar dataKey={playerBId} fill="var(--accent-b)" radius={[4,4,0,0]} maxBarSize={24} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
