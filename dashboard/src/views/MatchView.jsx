import { useEffect, useState, useCallback } from 'react'
import { api } from '../api.js'
import { SkeletonCard }   from '../components/SkeletonCard.jsx'
import { ScoreSummary }   from '../components/ScoreSummary.jsx'
import { WinProbChart }   from '../components/WinProbChart.jsx'
import { MomentumChart }  from '../components/MomentumChart.jsx'
import { TurningPoints }  from '../components/TurningPoints.jsx'
import { ShotStats }      from '../components/ShotStats.jsx'
import { EndingTypes }    from '../components/EndingTypes.jsx'
import { RallyAnalysis }  from '../components/RallyAnalysis.jsx'
import { PressurePanel }  from '../components/PressurePanel.jsx'
import { ScoreTimeline }  from '../components/ScoreTimeline.jsx'
import { ShotDonut }      from '../components/ShotDonut.jsx'
import styles from './MatchView.module.css'

// ── Set card ─────────────────────────────────────────────────────────────────

function SetCard({ set, playerAId, playerBId, isActive, isLoading, onClick }) {
  const wonByA = set.winner_id === playerAId
  return (
    <div
      className={`${styles.setCard} ${isActive ? styles.setCardActive : ''}`}
      onClick={onClick}
    >
      <div className={styles.setCardNum}>
        Set {set.set_number}
        {set.is_deuce && <span className={styles.setDeuce}>⚡ Deuce</span>}
      </div>
      <div className={styles.setCardScore}>
        <span style={{ color: 'var(--accent-a)', fontWeight: wonByA ? 800 : 400 }}>{set.score_a}</span>
        <span className={styles.setCardSep}> – </span>
        <span style={{ color: 'var(--accent-b)', fontWeight: !wonByA ? 800 : 400 }}>{set.score_b}</span>
      </div>
      <div
        className={styles.setCardWinner}
        style={{ background: wonByA ? 'var(--accent-a)' : 'var(--accent-b)' }}
      >
        {wonByA ? playerAId : playerBId}
      </div>
      <div className={styles.setCardCta}>
        {isLoading ? 'Loading…' : isActive ? 'Close ↑' : 'Analyse →'}
      </div>
    </div>
  )
}

// ── Analytics block (reused for both match and set views) ─────────────────────

function AnalyticsBlock({ analytics, playerAId, playerBId, label, animOffset = 0 }) {
  if (!analytics) return null

  const delay = (n) => ({ animationDelay: `${animOffset + n * 0.06}s` })

  return (
    <div className={styles.analyticsBlock}>
      {label && <div className={styles.sectionLabel}>{label}</div>}

      <ScoreTimeline
        data={analytics.score_progression}
        playerAId={playerAId}
        playerBId={playerBId}
      />

      <ShotDonut data={analytics.shot_effectiveness} playerAId={playerAId} playerBId={playerBId} />

      <div className={styles.row2} style={delay(2)}>
        <WinProbChart  data={analytics.win_probability}  playerAId={playerAId} playerBId={playerBId} />
        <MomentumChart data={analytics.momentum}         playerAId={playerAId} playerBId={playerBId} />
      </div>

      <div className={styles.row2} style={delay(3)}>
        <ShotStats   data={analytics.shot_effectiveness} playerAId={playerAId} playerBId={playerBId} />
        <EndingTypes data={analytics.ending_analysis}    playerAId={playerAId} playerBId={playerBId} />
      </div>

      <div className={styles.row2} style={delay(4)}>
        <RallyAnalysis data={analytics.rally_analysis}    playerAId={playerAId} playerBId={playerBId} />
        <PressurePanel data={analytics.pressure_analysis} playerAId={playerAId} playerBId={playerBId} />
      </div>

      <TurningPoints data={analytics.turning_points} />
    </div>
  )
}

// ── Main view ─────────────────────────────────────────────────────────────────

export function MatchView({ matchId }) {
  const [summary,      setSummary]      = useState(null)
  const [analytics,    setAnalytics]    = useState(null)
  const [loading,      setLoading]      = useState(true)
  const [error,        setError]        = useState(null)

  // Set-level state
  const [selectedSet,   setSelectedSet]   = useState(null)   // null = full match
  const [perSetData,    setPerSetData]    = useState({})     // { [setNum]: analyticsData }
  const [perSetLoading, setPerSetLoading] = useState({})     // { [setNum]: bool }

  useEffect(() => {
    setLoading(true)
    setSelectedSet(null)
    setPerSetData({})
    Promise.all([
      api.matches.summary(matchId),
      api.matches.fullAnalytics(matchId),
    ])
      .then(([s, a]) => { setSummary(s); setAnalytics(a) })
      .catch(() => setError('Could not load match analytics'))
      .finally(() => setLoading(false))
  }, [matchId])

  const handleSetClick = useCallback((setNum) => {
    // Toggle off
    if (selectedSet === setNum) {
      setSelectedSet(null)
      return
    }
    setSelectedSet(setNum)
    // Already fetched — no-op
    if (perSetData[setNum]) return
    // Fetch
    setPerSetLoading(prev => ({ ...prev, [setNum]: true }))
    api.matches.setAnalytics(matchId, setNum)
      .then(data => setPerSetData(prev => ({ ...prev, [setNum]: data })))
      .catch(() => setPerSetData(prev => ({ ...prev, [setNum]: null })))
      .finally(() => setPerSetLoading(prev => ({ ...prev, [setNum]: false })))
  }, [matchId, selectedSet, perSetData])

  if (loading) return (
    <div className={styles.skeletons}>
      <SkeletonCard height={160} />
      <div className={styles.row3}>
        <SkeletonCard height={120} /><SkeletonCard height={120} /><SkeletonCard height={120} />
      </div>
      <div className={styles.row2}><SkeletonCard height={280} /><SkeletonCard height={280} /></div>
      <div className={styles.row2}><SkeletonCard height={260} /><SkeletonCard height={260} /></div>
    </div>
  )

  if (error) return <div className={styles.error}>{error}</div>
  if (!analytics || Object.keys(analytics).length === 0)
    return <div className={styles.error}>No analytics found for this match. Re-run migration.</div>

  const playerAId = summary?.player_a_id
  const playerBId = summary?.player_b_id
  const sets      = summary?.sets ?? []
  const activeSetData = selectedSet ? perSetData[selectedSet] : null
  const activeSetInfo = sets.find(s => s.set_number === selectedSet)

  return (
    <div className={styles.view}>

      {/* ── Match result ── */}
      <ScoreSummary summary={summary} analytics={analytics} />

      {/* ── Set cards ── */}
      {sets.length > 0 && (
        <div className={styles.setCards}>
          {sets.map(s => (
            <SetCard
              key={s.set_number}
              set={s}
              playerAId={playerAId}
              playerBId={playerBId}
              isActive={selectedSet === s.set_number}
              isLoading={!!perSetLoading[s.set_number]}
              onClick={() => handleSetClick(s.set_number)}
            />
          ))}
        </div>
      )}

      {/* ── Set analytics (shown when a set is selected) ── */}
      {selectedSet !== null && (
        <div className={`${styles.setPanel} animate-in`}>
          <div className={styles.setPanelHeader}>
            <div className={styles.setPanelTitle}>
              Set {selectedSet} Analysis
              {activeSetInfo?.is_deuce && <span className={styles.setDeuce}>⚡ Deuce</span>}
            </div>
            {activeSetInfo && (
              <div className={styles.setPanelScore}>
                <span style={{ color: 'var(--accent-a)' }}>{activeSetInfo.score_a}</span>
                <span className={styles.setPanelSep}> – </span>
                <span style={{ color: 'var(--accent-b)' }}>{activeSetInfo.score_b}</span>
              </div>
            )}
            {sets.length > 1 && (
              <select
                className={styles.setDropdown}
                value={selectedSet}
                onChange={e => handleSetClick(Number(e.target.value))}
              >
                {sets.map(s => (
                  <option key={s.set_number} value={s.set_number}>Set {s.set_number}</option>
                ))}
              </select>
            )}
          </div>

          {perSetLoading[selectedSet] ? (
            <div className={styles.setLoadingBlock}>
              <SkeletonCard height={200} />
              <div className={styles.row2}><SkeletonCard height={220} /><SkeletonCard height={220} /></div>
            </div>
          ) : activeSetData ? (
            <AnalyticsBlock
              analytics={activeSetData}
              playerAId={playerAId}
              playerBId={playerBId}
            />
          ) : (
            <div className={styles.error}>Could not load set analytics.</div>
          )}
        </div>
      )}

      {/* ── Full match analytics (only when no set is selected) ── */}
      {selectedSet === null && (
        <AnalyticsBlock
          analytics={analytics}
          playerAId={playerAId}
          playerBId={playerBId}
          label="Match Analytics"
        />
      )}

    </div>
  )
}
