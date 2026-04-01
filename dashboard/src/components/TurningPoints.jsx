import styles from './Panel.module.css'

export function TurningPoints({ data }) {
  if (!data?.length) return null

  return (
    <div className={`${styles.card} animate-in`} style={{ animationDelay: '0.4s' }}>
      <div className={styles.title}>Turning Points</div>
      <div className={styles.tpSubtitle}>Points where win probability shifted &gt;8%</div>
      <div className={styles.tpList}>
        {data.map((tp, i) => (
          <div key={i} className={styles.tpRow}>
            <div className={styles.tpRank}>#{i + 1}</div>
            <div className={styles.tpInfo}>
              <div className={styles.tpScore}>Point {tp.point_number} · {tp.score}</div>
              <div className={styles.tpEnding}>{tp.ending_type?.replace(/_/g, ' ') ?? '—'}</div>
            </div>
            <div className={styles.tpShift}>
              <span
                className={styles.tpDirection}
                style={{ color: tp.direction === 'a_gaining' ? 'var(--accent-a)' : 'var(--accent-b)' }}
              >
                {tp.direction === 'a_gaining' ? '↑ A' : '↑ B'}
              </span>
              <span className={styles.tpPct}>+{(tp.win_prob_shift * 100).toFixed(1)}%</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
