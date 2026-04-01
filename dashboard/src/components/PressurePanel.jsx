import styles from './Panel.module.css'

export function PressurePanel({ data, playerAId, playerBId }) {
  if (!data) return <div className={styles.card}><div className={styles.title}>Pressure Analysis</div><div className={styles.empty}>No data</div></div>

  const total = data.total_pressure_points
  const aShare = total > 0 ? Math.round((data.a_wins / total) * 100) : 50
  const bShare = 100 - aShare

  return (
    <div className={`${styles.card} animate-in`} style={{ animationDelay: '0.35s' }}>
      <div className={styles.title}>Pressure Analysis</div>
      <div className={styles.pressureDesc}>Points where score diff ≤ 2 or both players ≥ 15</div>

      <div className={styles.pressureStats}>
        <div className={styles.pressureStat}>
          <div className={styles.pressureVal} style={{ color: 'var(--accent-a)' }}>{data.a_wins}</div>
          <div className={styles.pressureLabel}>{playerAId} won</div>
          <div className={styles.pressurePct}>{data.a_win_pct}%</div>
        </div>
        <div className={styles.pressureTotal}>
          <div className={styles.pressureTotalVal}>{total}</div>
          <div className={styles.pressureTotalLabel}>Pressure Points</div>
        </div>
        <div className={`${styles.pressureStat} ${styles.right}`}>
          <div className={styles.pressureVal} style={{ color: 'var(--accent-b)' }}>{data.b_wins}</div>
          <div className={styles.pressureLabel}>{playerBId} won</div>
          <div className={styles.pressurePct}>{data.b_win_pct}%</div>
        </div>
      </div>

      <div className={styles.pressureBar}>
        <div style={{ width: `${aShare}%`, background: 'var(--accent-a)', height: '100%', borderRadius: '3px 0 0 3px', transition: 'width 0.6s' }} />
        <div style={{ width: `${bShare}%`, background: 'var(--accent-b)', height: '100%', borderRadius: '0 3px 3px 0', transition: 'width 0.6s' }} />
      </div>

      {data.points?.length > 0 && (
        <div className={styles.pressurePointsList}>
          <div className={styles.pressureListTitle}>Pressure Moments</div>
          {data.points.slice(0, 5).map((p, i) => (
            <div key={i} className={styles.pressurePoint}>
              <span className={styles.ppNum}>Pt {p.point_number}</span>
              <span className={styles.ppScore}>{p.score}</span>
              <span className={styles.ppEnding}>{p.ending_type?.replace(/_/g, ' ')}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
