import styles from './ScoreSummary.module.css'

export function ScoreSummary({ summary, analytics }) {
  if (!summary) return null
  const { player_a_id, player_b_id, sets_won, winner_id, match_id, date } = summary
  const sets = summary.sets ?? []
  const aScore = sets_won?.a ?? 0
  const bScore = sets_won?.b ?? 0
  const total  = aScore + bScore
  const aShare = total > 0 ? Math.round((aScore / total) * 100) : 50
  const bShare = 100 - aShare

  const pressure = analytics?.pressure_analysis
  const serve    = analytics?.serve_analysis

  return (
    <div className={`${styles.card} animate-in`}>
      <div className={styles.meta}>
        <span className={styles.badge}>MATCH RESULT</span>
        <span className={styles.matchId}>{match_id}</span>
        <span className={styles.date}>{date ? new Date(date).toLocaleDateString() : ''}</span>
      </div>

      <div className={styles.scoreRow}>
        <div className={`${styles.player} ${winner_id === player_a_id ? styles.winner : ''}`}>
          <div className={styles.playerLabel}>{player_a_id}</div>
          <div className={styles.score} style={{ color: 'var(--accent-a)' }}>{aScore}</div>
          {winner_id === player_a_id && <div className={styles.winBadge} style={{ background: 'var(--accent-a)' }}>WIN</div>}
        </div>

        <div className={styles.divider}><span className={styles.vs}>VS</span></div>

        <div className={`${styles.player} ${styles.right} ${winner_id === player_b_id ? styles.winner : ''}`}>
          <div className={styles.playerLabel}>{player_b_id}</div>
          <div className={styles.score} style={{ color: 'var(--accent-b)' }}>{bScore}</div>
          {winner_id === player_b_id && <div className={styles.winBadge} style={{ background: 'var(--accent-b)' }}>WIN</div>}
        </div>
      </div>

      <div className={styles.dominanceWrap}>
        <span className={styles.domLabel} style={{ color: 'var(--accent-a)' }}>{aShare}%</span>
        <div className={styles.bar}>
          <div className={styles.fillA} style={{ width: `${aShare}%` }} />
          <div className={styles.fillB} style={{ width: `${bShare}%` }} />
        </div>
        <span className={styles.domLabel} style={{ color: 'var(--accent-b)' }}>{bShare}%</span>
      </div>
      <div className={styles.barLegend}>Sets Won</div>

      {sets.length > 0 && (
        <div className={styles.setsBreakdown}>
          {sets.map(s => {
            const aWon = s.winner_id === player_a_id
            return (
              <div key={s.set_number} className={styles.setChip}>
                <span className={styles.setChipLabel}>Set {s.set_number}{s.is_deuce ? ' ⚡' : ''}</span>
                <span style={{ color: aWon ? 'var(--accent-a)' : 'var(--text-muted)', fontWeight: aWon ? 700 : 400 }}>{s.score_a}</span>
                <span className={styles.setChipSep}>–</span>
                <span style={{ color: !aWon ? 'var(--accent-b)' : 'var(--text-muted)', fontWeight: !aWon ? 700 : 400 }}>{s.score_b}</span>
              </div>
            )
          })}
        </div>
      )}

      {(pressure || serve) && (
        <div className={styles.quickStats}>
          {pressure && (
            <div className={styles.quickStat}>
              <div className={styles.qLabel}>Pressure Points</div>
              <div className={styles.qVal}>
                <span style={{ color: 'var(--accent-a)' }}>{pressure.a_wins}</span>
                <span className={styles.qSep}>/</span>
                <span style={{ color: 'var(--accent-b)' }}>{pressure.b_wins}</span>
                <span className={styles.qTotal}> ({pressure.total_pressure_points} total)</span>
              </div>
            </div>
          )}
          {serve && (
            <>
              <div className={styles.quickStat}>
                <div className={styles.qLabel}>Serve Win% (A)</div>
                <div className={styles.qVal} style={{ color: 'var(--accent-a)' }}>
                  {serve.player_a_serving?.win_pct ?? '—'}%
                </div>
              </div>
              <div className={styles.quickStat}>
                <div className={styles.qLabel}>Serve Win% (B)</div>
                <div className={styles.qVal} style={{ color: 'var(--accent-b)' }}>
                  {serve.player_b_serving?.win_pct ?? '—'}%
                </div>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}
