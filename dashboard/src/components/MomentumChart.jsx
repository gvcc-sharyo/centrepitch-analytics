import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import styles from './Panel.module.css'

export function MomentumChart({ data, playerAId, playerBId }) {
  if (!data?.length) return <div className={styles.card}><div className={styles.title}>Momentum</div><div className={styles.empty}>No data</div></div>
  return (
    <div className={`${styles.card} animate-in`} style={{ animationDelay: '0.15s' }}>
      <div className={styles.title}>Momentum (Rolling 5 Points)</div>
      <ResponsiveContainer width="100%" height={220}>
        <AreaChart data={data} margin={{ top: 8, right: 12, left: -20, bottom: 0 }}>
          <defs>
            <linearGradient id="gradA" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%"  stopColor="var(--accent-a)" stopOpacity={0.3} />
              <stop offset="95%" stopColor="var(--accent-a)" stopOpacity={0} />
            </linearGradient>
            <linearGradient id="gradB" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%"  stopColor="var(--accent-b)" stopOpacity={0.3} />
              <stop offset="95%" stopColor="var(--accent-b)" stopOpacity={0} />
            </linearGradient>
          </defs>
          <XAxis dataKey="point_number" tick={{ fill: 'var(--text-muted)', fontSize: 10 }} axisLine={false} tickLine={false} />
          <YAxis domain={[0, 5]} tick={{ fill: 'var(--text-muted)', fontSize: 10 }} axisLine={false} tickLine={false} />
          <Tooltip
            contentStyle={{ background: 'var(--bg-raised)', border: '1px solid var(--bg-border)', borderRadius: '6px' }}
            labelStyle={{ color: 'var(--text-secondary)', fontSize: 11 }}
            itemStyle={{ fontSize: 12 }}
          />
          <Area type="monotone" dataKey="a_momentum" stroke="var(--accent-a)" fill="url(#gradA)" strokeWidth={2} name={playerAId} />
          <Area type="monotone" dataKey="b_momentum" stroke="var(--accent-b)" fill="url(#gradB)" strokeWidth={2} name={playerBId} />
        </AreaChart>
      </ResponsiveContainer>
      <div className={styles.legend}>
        <span className={styles.dotA} /><span className={styles.legendLabel}>{playerAId}</span>
        <span className={styles.dotB} /><span className={styles.legendLabel}>{playerBId}</span>
      </div>
    </div>
  )
}
