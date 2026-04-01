import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import styles from './Panel.module.css'

export function EndingTypes({ data, playerAId, playerBId }) {
  if (!data?.length) return <div className={styles.card}><div className={styles.title}>How Points Ended</div><div className={styles.empty}>No data</div></div>

  const chartData = data.map(d => ({
    name: d.ending_type.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
    [playerAId]: d.a_wins,
    [playerBId]: d.b_wins,
    pct: d.pct_of_match,
  }))

  return (
    <div className={`${styles.card} animate-in`} style={{ animationDelay: '0.25s' }}>
      <div className={styles.header}>
        <div className={styles.title}>How Points Ended</div>
        <div className={styles.legend}>
          <span className={styles.dotA} /><span className={styles.legendLabel}>{playerAId}</span>
          <span className={styles.dotB} /><span className={styles.legendLabel}>{playerBId}</span>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={chartData} layout="vertical" margin={{ top: 4, right: 20, left: 8, bottom: 4 }} barGap={3}>
          <XAxis type="number" tick={{ fill: 'var(--text-muted)', fontSize: 10 }} axisLine={false} tickLine={false} allowDecimals={false} />
          <YAxis type="category" dataKey="name" tick={{ fill: 'var(--text-secondary)', fontSize: 10 }} axisLine={false} tickLine={false} width={100} />
          <Tooltip
            contentStyle={{ background: 'var(--bg-raised)', border: '1px solid var(--bg-border)', borderRadius: '6px' }}
            cursor={{ fill: 'rgba(0,0,0,0.03)' }}
          />
          <Bar dataKey={playerAId} fill="var(--accent-a)" radius={[0,4,4,0]} maxBarSize={16} />
          <Bar dataKey={playerBId} fill="var(--accent-b)" radius={[0,4,4,0]} maxBarSize={16} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
