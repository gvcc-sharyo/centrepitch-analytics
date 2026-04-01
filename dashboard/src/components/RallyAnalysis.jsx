import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import styles from './Panel.module.css'

export function RallyAnalysis({ data, playerAId, playerBId }) {
  if (!data) return <div className={styles.card}><div className={styles.title}>Rally Analysis</div><div className={styles.empty}>No data</div></div>

  const chartData = (data.by_bucket || []).map(b => ({
    name: b.bucket,
    [playerAId]: b.a_wins,
    [playerBId]: b.b_wins,
  }))

  return (
    <div className={`${styles.card} animate-in`} style={{ animationDelay: '0.3s' }}>
      <div className={styles.header}>
        <div className={styles.title}>Rally Analysis</div>
      </div>

      <div className={styles.statRow}>
        {[
          { label: 'Avg Shots',    value: data.avg_shots },
          { label: 'Avg Duration', value: `${data.avg_duration_sec}s` },
          { label: 'Longest Rally',value: data.max_rally },
          { label: 'Total Shots',  value: data.total_shots_played },
        ].map(s => (
          <div key={s.label} className={styles.miniStat}>
            <div className={styles.miniVal}>{s.value}</div>
            <div className={styles.miniLabel}>{s.label}</div>
          </div>
        ))}
      </div>

      <ResponsiveContainer width="100%" height={160}>
        <BarChart data={chartData} margin={{ top: 8, right: 8, left: -20, bottom: 0 }} barGap={3}>
          <XAxis dataKey="name" tick={{ fill: 'var(--text-secondary)', fontSize: 10 }} axisLine={false} tickLine={false} />
          <YAxis tick={{ fill: 'var(--text-muted)', fontSize: 10 }} axisLine={false} tickLine={false} allowDecimals={false} />
          <Tooltip
            contentStyle={{ background: 'var(--bg-raised)', border: '1px solid var(--bg-border)', borderRadius: '6px' }}
            cursor={{ fill: 'rgba(255,255,255,0.03)' }}
          />
          <Bar dataKey={playerAId} fill="var(--accent-a)" radius={[4,4,0,0]} maxBarSize={28} />
          <Bar dataKey={playerBId} fill="var(--accent-b)" radius={[4,4,0,0]} maxBarSize={28} />
        </BarChart>
      </ResponsiveContainer>
      <div className={styles.legend}>
        <span className={styles.dotA} /><span className={styles.legendLabel}>{playerAId} wins</span>
        <span className={styles.dotB} /><span className={styles.legendLabel}>{playerBId} wins</span>
      </div>
    </div>
  )
}
