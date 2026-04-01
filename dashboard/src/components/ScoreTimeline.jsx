import { useState } from 'react'
import styles from './ScoreTimeline.module.css'
import panelStyles from './Panel.module.css'

function Shuttlecock({ color }) {
  return (
    <svg viewBox="0 0 24 24" width="14" height="14" fill="none" xmlns="http://www.w3.org/2000/svg">
      {/* Cork base */}
      <ellipse cx="12" cy="17" rx="3.5" ry="4" fill={color} opacity="0.95" />
      {/* Feather skirt lines */}
      <line x1="12" y1="13" x2="6"  y2="4"  stroke={color} strokeWidth="1.3" strokeLinecap="round" opacity="0.8" />
      <line x1="12" y1="13" x2="9"  y2="3"  stroke={color} strokeWidth="1.3" strokeLinecap="round" opacity="0.8" />
      <line x1="12" y1="13" x2="12" y2="3"  stroke={color} strokeWidth="1.3" strokeLinecap="round" opacity="0.8" />
      <line x1="12" y1="13" x2="15" y2="3"  stroke={color} strokeWidth="1.3" strokeLinecap="round" opacity="0.8" />
      <line x1="12" y1="13" x2="18" y2="4"  stroke={color} strokeWidth="1.3" strokeLinecap="round" opacity="0.8" />
      {/* Feather rim arc */}
      <path d="M6 4 Q12 6 18 4" stroke={color} strokeWidth="1.1" fill="none" strokeLinecap="round" opacity="0.7" />
    </svg>
  )
}

export function ScoreTimeline({ data, playerAId, playerBId }) {
  const [hovered, setHovered] = useState(null)   // { setIdx, ptIdx }

  if (!data?.length) return (
    <div className={panelStyles.card}>
      <div className={panelStyles.title}>Score Timeline</div>
      <div className={panelStyles.empty}>No data</div>
    </div>
  )

  // Group points by set_number
  const setsMap = {}
  data.forEach(p => {
    const sn = p.set_number ?? 1
    if (!setsMap[sn]) setsMap[sn] = []
    setsMap[sn].push(p)
  })
  const setNumbers = Object.keys(setsMap).map(Number).sort((a, b) => a - b)

  // Resolve hovered point
  const hoveredPoint = hovered
    ? setsMap[hovered.setNum]?.[hovered.ptIdx]
    : null

  return (
    <div className={`${panelStyles.card} animate-in`} style={{ animationDelay: '0.1s' }}>
      <div className={styles.cardHeader}>
        <div className={panelStyles.title} style={{ marginBottom: 0 }}>Score Timeline</div>
        <div className={styles.hoverBox}>
          {hoveredPoint ? (
            <>
              <span className={styles.hoverPt}>Set {hoveredPoint.set_number} · Pt {hoveredPoint.point_number}</span>
              <span
                className={styles.hoverWinner}
                style={{ color: hoveredPoint.winner_id === playerAId ? 'var(--accent-a)' : 'var(--accent-b)' }}
              >
                {hoveredPoint.winner_id === playerAId ? playerAId : playerBId}
              </span>
              <span className={styles.hoverScore}>
                {hoveredPoint.set_score_a ?? hoveredPoint.score_a}
                {' – '}
                {hoveredPoint.set_score_b ?? hoveredPoint.score_b}
              </span>
              {hoveredPoint.ending_type && (
                <span className={styles.hoverEnding}>{hoveredPoint.ending_type.replace(/_/g, ' ')}</span>
              )}
            </>
          ) : (
            <span className={styles.hoverHint}>Hover a point to inspect</span>
          )}
        </div>
      </div>

      <div className={styles.sets}>
        {setNumbers.map(sn => {
          const pts = setsMap[sn]
          const last = pts[pts.length - 1]
          const sa = last?.set_score_a ?? last?.score_a ?? '?'
          const sb = last?.set_score_b ?? last?.score_b ?? '?'
          const setWonByA = (last?.set_score_a ?? 0) > (last?.set_score_b ?? 0)

          return (
            <div key={sn} className={styles.setBlock}>
              {/* Set header */}
              <div className={styles.setHeader}>
                <span className={styles.setLabel}>Set {sn}</span>
                <div className={styles.setDivider} />
                <span
                  className={styles.setScore}
                  style={{ color: setWonByA ? 'var(--accent-a)' : 'var(--accent-b)' }}
                >
                  {sa}
                  <span className={styles.setScoreSep}> – </span>
                  {sb}
                </span>
              </div>

              {/* Circle row — full width */}
              <div className={styles.row}>
                {pts.map((p, i) => {
                  const isA = p.winner_id === playerAId
                  const isHov = hovered?.setNum === sn && hovered?.ptIdx === i
                  return (
                    <div
                      key={i}
                      className={`${styles.circle} ${isHov ? styles.circleHov : ''}`}
                      style={{
                        background: isA
                          ? 'var(--accent-a-dim)'
                          : 'var(--accent-b-dim)',
                        borderColor: isA ? 'var(--accent-a)' : 'var(--accent-b)',
                      }}
                      onMouseEnter={() => setHovered({ setNum: sn, ptIdx: i })}
                      onMouseLeave={() => setHovered(null)}
                    >
                      <Shuttlecock color={isA ? 'var(--accent-a)' : 'var(--accent-b)'} />
                    </div>
                  )
                })}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
