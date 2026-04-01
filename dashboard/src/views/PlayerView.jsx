import { useEffect, useState, useCallback, useMemo, memo } from 'react'
import { api } from '../api.js'
import { SkeletonCard } from '../components/SkeletonCard.jsx'
import styles from './PlayerView.module.css'

// ── Pressure helpers (shared across snapshot + advanced card) ─────────────────
function getPressureClassification(pr) {
  if (pr == null) return { label: '—', color: 'var(--text-muted)', tag: null }
  if (pr > 0.05)  return { label: 'Clutch',  color: 'var(--accent-a)', tag: 'CLUTCH'  }
  if (pr < -0.05) return { label: 'Fragile', color: 'var(--accent-b)', tag: 'FRAGILE' }
  return           { label: 'Steady',  color: 'var(--text-secondary)', tag: 'STEADY'  }
}

// ── Tier 1: Snapshot bar ──────────────────────────────────────────────────────
const SnapshotBar = memo(function SnapshotBar({ profile, fallbackTotal }) {
  if (!profile) return null

  const formPct  = profile.form_last5 != null ? `${Math.round(profile.form_last5 * 100)}%` : '—'
  const dir      = profile.form_direction
  const arrow    = dir === 'up' ? '↑' : dir === 'down' ? '↓' : '→'
  const dirColor = dir === 'up' ? 'var(--accent-a)' : dir === 'down' ? 'var(--accent-b)' : 'var(--text-muted)'

  const { label: prLabel, color: prColor } = getPressureClassification(profile.pressure_rating)
  const prDelta = profile.pressure_rating != null
    ? `${profile.pressure_rating >= 0 ? '+' : ''}${(profile.pressure_rating * 100).toFixed(1)}pp`
    : null

  const sig = profile.signature_shot
    ? profile.signature_shot.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
    : '—'

  return (
    <div className={styles.snapshot}>
      {/* Matches */}
      <div className={styles.snapItem}>
        <div className={styles.snapVal}>{profile.total_matches ?? fallbackTotal ?? '—'}</div>
        <div className={styles.snapLabel}>Matches</div>
      </div>

      {/* Win Rate */}
      <div className={styles.snapItem}>
        <div className={styles.snapVal} style={{ color: 'var(--accent-a)' }}>
          {profile.win_rate_overall != null ? `${Math.round(profile.win_rate_overall * 100)}%` : '—'}
        </div>
        <div className={styles.snapLabel}>Win Rate</div>
      </div>

      {/* Form */}
      <div className={styles.snapItem}>
        <div className={styles.snapVal} style={{ color: dirColor }}>
          {formPct}
          <span className={styles.snapBadge} style={{ color: dirColor }}>{arrow}</span>
        </div>
        <div className={styles.snapLabel}>Form (last 5)</div>
      </div>

      {/* Pressure — classification label, not a raw number */}
      <div className={styles.snapItem}>
        <div className={styles.snapPressureTag} style={{ color: prColor, borderColor: prColor }}>
          {prLabel}
        </div>
        {prDelta && <div className={styles.snapPressureDelta} style={{ color: prColor }}>{prDelta} vs avg</div>}
        <div className={styles.snapLabel}>Under Pressure</div>
      </div>

      {/* Signature Shot */}
      <div className={styles.snapItem}>
        <div className={styles.snapValSm} style={{ color: 'var(--text-secondary)' }}>{sig}</div>
        <div className={styles.snapLabel}>Signature Shot</div>
      </div>
    </div>
  )
})

// ── Tier 2a: Win rate windows ─────────────────────────────────────────────────
const WinRateWindows = memo(function WinRateWindows({ profile, matches }) {
  if (!profile) return null
  const windows = [
    { label: 'Overall', value: profile.win_rate_overall },
    { label: '30 days', value: profile.win_rate_30d },
    { label: '90 days', value: profile.win_rate_90d },
  ]

  const [mode, setMode] = useState('30d') // 7d | 30d | 90d | 180d | custom
  const [from, setFrom] = useState('')
  const [to,   setTo]   = useState('')

  const customStats = useMemo(() => {
    const list = Array.isArray(matches) ? matches : []
    const parseDate = (d) => {
      const dt = d ? new Date(d) : null
      return dt && !Number.isNaN(dt.getTime()) ? dt : null
    }

    const now = new Date()
    let start = null
    let end   = null

    if (mode === 'custom') {
      start = parseDate(from ? `${from}T00:00:00.000Z` : null) || null
      end   = parseDate(to   ? `${to}T23:59:59.999Z` : null) || null
    } else {
      const days = Number(String(mode).replace('d', ''))
      if (!Number.isFinite(days)) return { count: 0, wins: 0, winRate: null, label: 'Custom' }
      start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000)
      end   = now
    }

    const filtered = list.filter(m => {
      const dt = parseDate(m?.date)
      if (!dt) return false
      if (start && dt < start) return false
      if (end && dt > end) return false
      return true
    })

    const wins = filtered.filter(m => !!m?.won).length
    const count = filtered.length
    const winRate = count > 0 ? wins / count : null

    const label = mode === 'custom'
      ? (from && to ? `${from} → ${to}` : from ? `From ${from}` : to ? `Up to ${to}` : 'Custom range')
      : `Last ${String(mode).replace('d', '')} days`

    return { count, wins, winRate, label }
  }, [matches, mode, from, to])

  return (
    <>
      <div className={styles.rateCards}>
        {windows.map(w => {
          const pct   = w.value != null ? Math.round(w.value * 100) : null
          const color = pct == null ? 'var(--text-muted)' : pct >= 50 ? 'var(--accent-a)' : 'var(--accent-b)'
          return (
            <div key={w.label} className={styles.rateCard}>
              <div className={styles.rateVal} style={{ color }}>{pct != null ? `${pct}%` : '—'}</div>
              <div className={styles.rateLabel}>{w.label}</div>
              {pct != null && (
                <div className={styles.rateBar}>
                  <div style={{ width: `${pct}%`, background: color, height: '100%', borderRadius: '2px', transition: 'width 0.7s' }} />
                </div>
              )}
            </div>
          )
        })}
      </div>

      <div className={styles.windowPicker}>
        <div className={styles.windowPickerRow}>
          <div className={styles.windowPickerLabel}>Custom window</div>
          <select className={styles.windowPickerSelect} value={mode} onChange={(e) => setMode(e.target.value)}>
            <option value="7d">Last 7 days</option>
            <option value="30d">Last 30 days</option>
            <option value="90d">Last 90 days</option>
            <option value="180d">Last 180 days</option>
            <option value="custom">Pick dates</option>
          </select>
          {mode === 'custom' && (
            <div className={styles.windowPickerDates}>
              <label className={styles.windowPickerDate}>
                <span>From</span>
                <input type="date" value={from} onChange={(e) => setFrom(e.target.value)} />
              </label>
              <label className={styles.windowPickerDate}>
                <span>To</span>
                <input type="date" value={to} onChange={(e) => setTo(e.target.value)} />
              </label>
            </div>
          )}
        </div>

        <div className={styles.windowSummary}>
          <div className={styles.windowSummaryTop}>
            <div className={styles.windowSummaryTitle}>{customStats.label}</div>
            <div className={styles.windowSummaryMeta}>
              {customStats.count > 0 ? `${customStats.wins}W / ${customStats.count - customStats.wins}L (${customStats.count} matches)` : 'No matches in this window'}
            </div>
          </div>
          <div className={styles.windowSummaryVal}>
            {customStats.winRate != null ? `${Math.round(customStats.winRate * 100)}%` : '—'}
          </div>
        </div>
      </div>
    </>
  )
})

// ── Tier 2b: W/L history strip ───────────────────────────────────────────────
const HistoryStrip = memo(function HistoryStrip({ matches }) {
  const { last, wins } = useMemo(() => {
    const last = matches.slice(0, 10)
    return { last, wins: last.filter(m => m.won).length }
  }, [matches])

  return (
    <div className={styles.historyStrip}>
      <div className={styles.historyLabel}>Last {last.length}</div>
      <div className={styles.historyDots}>
        {last.map((m, i) => (
          <div key={i} className={m.won ? styles.dotWin : styles.dotLoss} title={m.match_id}>
            {m.won ? 'W' : 'L'}
          </div>
        ))}
      </div>
      <div className={styles.historyTally}>
        <span style={{ color: 'var(--accent-a)' }}>{wins}W</span>
        {' '}
        <span style={{ color: 'var(--accent-b)' }}>{last.length - wins}L</span>
      </div>
    </div>
  )
})

// ── Tier 3: H2H table ────────────────────────────────────────────────────────
const H2HTable = memo(function H2HTable({ h2h }) {
  if (!h2h?.length) return <div className={styles.empty}>No head-to-head data yet.</div>
  return (
    <div className={styles.h2hTable}>
      <div className={styles.h2hHeader}>
        <span>Opponent</span>
        <span>Record</span>
        <span>Win %</span>
        <span>Last</span>
      </div>
      {h2h.map(row => (
        <div key={row.opponent_id} className={styles.h2hRow}>
          <span className={styles.h2hOpp}>{row.opponent_id}</span>
          <span className={styles.h2hRecord}>
            <span style={{ color: 'var(--accent-a)' }}>{row.wins}</span>
            <span className={styles.h2hSep}>—</span>
            <span style={{ color: 'var(--accent-b)' }}>{row.losses}</span>
          </span>
          <span className={styles.h2hPct} style={{ color: row.win_pct >= 50 ? 'var(--accent-a)' : 'var(--accent-b)' }}>
            {row.win_pct}%
          </span>
          <span className={row.last_result === 'win' ? styles.winBadgeSm : styles.lossBadgeSm}>
            {row.last_result?.toUpperCase()}
          </span>
        </div>
      ))}
    </div>
  )
})

// ── Advanced: Pressure card ───────────────────────────────────────────────────
const PressureCard = memo(function PressureCard({ profile }) {
  const pr = profile?.pressure_rating
  const { color, tag } = getPressureClassification(pr)

  // Scale: ±30pp fills the full half of the bar
  const fillPct   = pr != null ? Math.min(Math.abs(pr) / 0.30 * 50, 50) : 0
  const isPositive = pr != null && pr >= 0

  const explanation = pr == null ? null
    : pr > 0.05  ? `Performs ${(pr * 100).toFixed(1)}% better when the score is tight`
    : pr < -0.05 ? `Performance drops ${Math.abs(pr * 100).toFixed(1)}% in close games`
    : 'Performance is consistent regardless of score pressure'

  return (
    <div className={styles.pressureCard}>
      <div className={styles.pressureCardTop}>
        <div className={styles.advCardLabel}>Under Pressure</div>
        {tag && (
          <span className={styles.pressureTagBig} style={{ color, borderColor: color }}>
            {tag}
          </span>
        )}
      </div>

      {/* Bi-directional bar */}
      <div className={styles.prBarWrap}>
        <span className={styles.prBarSideLabel} style={{ color: 'var(--accent-b)' }}>Fragile</span>
        <div className={styles.prTrack}>
          <div className={styles.prTrackCenter} />
          {pr != null && (
            <div
              className={styles.prFill}
              style={{
                width:      `${fillPct}%`,
                background: color,
                left:  isPositive ? '50%' : undefined,
                right: isPositive ? undefined : '50%',
              }}
            />
          )}
        </div>
        <span className={styles.prBarSideLabel} style={{ color: 'var(--accent-a)' }}>Clutch</span>
      </div>

      {explanation && (
        <div className={styles.pressureExplain}>{explanation}</div>
      )}
    </div>
  )
})

// ── Advanced: Consistency meter ───────────────────────────────────────────────
const ConsistencyMeter = memo(function ConsistencyMeter({ score }) {
  if (score == null) return null
  const pct   = Math.round(score * 100)
  const color = pct >= 70 ? 'var(--accent-a)' : pct >= 40 ? '#f59e0b' : 'var(--accent-b)'
  const label = pct >= 70 ? 'Consistent' : pct >= 40 ? 'Variable' : 'Inconsistent'
  return (
    <div className={styles.advCard}>
      <div className={styles.advCardLabel}>Consistency</div>
      <div className={styles.advCardVal} style={{ color }}>{pct}<span className={styles.advUnit}>/100</span></div>
      <div className={styles.advMeterTrack}>
        <div style={{ width: `${pct}%`, background: color, height: '100%', borderRadius: '3px', transition: 'width 0.7s' }} />
      </div>
      <div className={styles.advCardSub}>{label}</div>
    </div>
  )
})

// ── Advanced: Form breakdown ──────────────────────────────────────────────────
const FormBreakdown = memo(function FormBreakdown({ profile }) {
  const last5 = profile.form_last5 != null ? Math.round(profile.form_last5 * 100) : null
  const prev5 = profile.form_prev5 != null ? Math.round(profile.form_prev5 * 100) : null
  const delta = profile.form_delta
  const dir   = profile.form_direction

  if (last5 == null) return (
    <div className={styles.advCard}>
      <div className={styles.advCardLabel}>Form</div>
      <div className={styles.advCardSub}>Need 5+ matches</div>
    </div>
  )

  const dirColor = dir === 'up' ? 'var(--accent-a)' : dir === 'down' ? 'var(--accent-b)' : 'var(--text-muted)'
  const arrow    = dir === 'up' ? '↑' : dir === 'down' ? '↓' : '→'

  return (
    <div className={styles.advCard}>
      <div className={styles.advCardLabel}>Form Trend</div>
      <div className={styles.formRow}>
        <div className={styles.formWindow}>
          <div className={styles.formVal} style={{ color: last5 >= 50 ? 'var(--accent-a)' : 'var(--accent-b)' }}>{last5}%</div>
          <div className={styles.formWindowLabel}>Last 5</div>
        </div>
        <div className={styles.formArrow} style={{ color: dirColor }}>{arrow}</div>
        {prev5 != null && (
          <div className={styles.formWindow}>
            <div className={styles.formVal} style={{ color: prev5 >= 50 ? 'var(--accent-a)' : 'var(--accent-b)' }}>{prev5}%</div>
            <div className={styles.formWindowLabel}>Prev 5</div>
          </div>
        )}
      </div>
      {delta != null && (
        <div className={styles.advCardSub} style={{ color: dirColor }}>
          {delta >= 0 ? '+' : ''}{Math.round(delta * 100)}pp vs previous
        </div>
      )}
    </div>
  )
})

// ── Advanced: Shot win rates ──────────────────────────────────────────────────
const ShotWinRates = memo(function ShotWinRates({ trends }) {
  const shots = useMemo(() => {
    const shotMap = trends?.shot_win_rate_trend ?? {}
    return Object.entries(shotMap)
      .map(([shot, entries]) => ({
        shot: shot.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
        avg:  Math.round(entries.reduce((s, e) => s + e.win_rate, 0) / entries.length),
      }))
      .sort((a, b) => b.avg - a.avg)
  }, [trends])

  if (!shots.length) return (
    <div className={styles.advBlock}>
      <div className={styles.advBlockTitle}>Shot Win Rates</div>
      <div className={styles.empty}>Not enough shot data yet.</div>
    </div>
  )

  return (
    <div className={styles.advBlock}>
      <div className={styles.advBlockTitle}>
        Shot Win Rates <span className={styles.advBlockSub}>avg across all matches</span>
      </div>
      <div className={styles.shotSummary}>
        {shots.map(s => (
          <div key={s.shot} className={styles.shotRow}>
            <span className={styles.shotName}>{s.shot}</span>
            <div className={styles.shotBarWrap}>
              <div style={{ width: `${s.avg}%`, background: s.avg >= 50 ? 'var(--accent-a)' : 'var(--accent-b)', height: '100%', borderRadius: '2px', transition: 'width 0.7s' }} />
            </div>
            <span className={styles.shotPct} style={{ color: s.avg >= 50 ? 'var(--accent-a)' : 'var(--accent-b)' }}>{s.avg}%</span>
          </div>
        ))}
      </div>
    </div>
  )
})

// ── Advanced: Serve trend ─────────────────────────────────────────────────────
const ServeTrend = memo(function ServeTrend({ trends }) {
  const { avg, count } = useMemo(() => {
    const data = trends?.serve_win_rate_trend ?? []
    if (!data.length) return { avg: null, count: 0 }
    return {
      avg:   Math.round(data.reduce((s, e) => s + e.serve_win_pct, 0) / data.length),
      count: data.length,
    }
  }, [trends])

  if (avg == null) return null
  const color = avg >= 50 ? 'var(--accent-a)' : 'var(--accent-b)'
  return (
    <div className={styles.advCard}>
      <div className={styles.advCardLabel}>Serve Win Rate</div>
      <div className={styles.advCardVal} style={{ color }}>{avg}<span className={styles.advUnit}>%</span></div>
      <div className={styles.advMeterTrack}>
        <div style={{ width: `${avg}%`, background: color, height: '100%', borderRadius: '3px', transition: 'width 0.7s' }} />
      </div>
      <div className={styles.advCardSub}>avg over {count} matches</div>
    </div>
  )
})

// ── Advanced: Best winning shot ───────────────────────────────────────────────
const BestWinningShot = memo(function BestWinningShot({ profile }) {
  const shot = profile?.best_winning_shot
  if (!shot) return null
  return (
    <div className={styles.advCard}>
      <div className={styles.advCardLabel}>Best Winning Shot</div>
      <div className={styles.advCardVal} style={{ fontSize: 16, color: 'var(--accent-a)' }}>
        {shot.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}
      </div>
      <div className={styles.advCardSub}>highest winning shot count</div>
    </div>
  )
})

// ── Main view ─────────────────────────────────────────────────────────────────
export function PlayerView({ playerId, playerName, onSelectMatch }) {
  const [profile,       setProfile]       = useState(null)
  const [matches,       setMatches]       = useState([])
  const [h2h,           setH2h]           = useState(null)
  const [trends,        setTrends]        = useState(null)
  const [loading,       setLoading]       = useState(true)
  const [trendsLoading, setTrendsLoading] = useState(false)
  const [showAdvanced,  setShowAdvanced]  = useState(false)
  const [error,         setError]         = useState(null)

  useEffect(() => {
    setLoading(true)
    setProfile(null); setMatches([]); setH2h(null); setTrends(null); setShowAdvanced(false)
    Promise.all([
      api.players.profile(playerId).catch(() => null),
      api.players.matches(playerId).catch(() => []),
      api.players.h2h(playerId).catch(() => []),
    ])
      .then(([prof, mtchs, h2hData]) => { setProfile(prof); setMatches(mtchs); setH2h(h2hData) })
      .catch(() => setError('Could not load player data'))
      .finally(() => setLoading(false))
  }, [playerId])

  const handleToggleAdvanced = useCallback(() => {
    const opening = !showAdvanced
    setShowAdvanced(opening)
    if (opening && !trends && !trendsLoading) {
      setTrendsLoading(true)
      api.players.trends(playerId)
        .then(setTrends)
        .catch(() => setTrends({}))
        .finally(() => setTrendsLoading(false))
    }
  }, [showAdvanced, trends, trendsLoading, playerId])

  const totalMatches = useMemo(
    () => profile?.total_matches ?? matches.length,
    [profile, matches.length]
  )

  return (
    <div className={styles.view}>
      {/* Header */}
      <div className={styles.pageHeader}>
        <div className={styles.playerBadge}>{playerName[0]}</div>
        <div>
          <h1 className={styles.playerName}>{playerName}</h1>
          <div className={styles.playerId}>{playerId}</div>
        </div>
      </div>

      {error && <div className={styles.error}>{error}</div>}

      {loading ? (
        <div className={styles.skeletons}>
          <SkeletonCard height={80} />
          <SkeletonCard height={120} />
          <SkeletonCard height={200} />
        </div>
      ) : (
        <>
          {/* Tier 1: Snapshot */}
          <section className={`${styles.section} animate-in`}>
            <SnapshotBar profile={profile} fallbackTotal={matches.length} />
          </section>

          {/* Tier 2: Win rate windows + history strip */}
          <section className={`${styles.section} animate-in`} style={{ animationDelay: '0.08s' }}>
            <div className={styles.sectionTitle}>Performance Windows</div>
            <WinRateWindows profile={profile} matches={matches} />
            {matches.length > 0 && <HistoryStrip matches={matches} />}
          </section>

          {/* Tier 3: H2H */}
          <section className={`${styles.section} animate-in`} style={{ animationDelay: '0.16s' }}>
            <div className={styles.sectionTitle}>Head to Head</div>
            <H2HTable h2h={h2h} />
          </section>

          {/* Advanced Analytics toggle */}
          <section className={`${styles.section} animate-in`} style={{ animationDelay: '0.22s' }}>
            <button className={styles.advToggle} onClick={handleToggleAdvanced}>
              <span>Advanced Analytics</span>
              <span className={`${styles.advChevron} ${showAdvanced ? styles.advChevronOpen : ''}`}>▾</span>
            </button>

            {showAdvanced && (
              <div className={styles.advPanel}>
                {trendsLoading ? (
                  <>
                    <SkeletonCard height={90} />
                    <SkeletonCard height={160} />
                  </>
                ) : (
                  <>
                    {/* Pressure card — full width */}
                    <PressureCard profile={profile} />

                    {/* Smaller stat cards */}
                    <div className={styles.advCards}>
                      <ConsistencyMeter score={profile?.consistency_score} />
                      <FormBreakdown profile={profile ?? {}} />
                      <BestWinningShot profile={profile} />
                      <ServeTrend trends={trends} />
                    </div>

                    {/* Shot win rates */}
                    <ShotWinRates trends={trends} />

                    {totalMatches < 6 && (
                      <div className={styles.advNote}>
                        Some trend data requires 6+ matches to generate.
                      </div>
                    )}
                  </>
                )}
              </div>
            )}
          </section>

          {/* Match history */}
          <section className={`${styles.section} animate-in`} style={{ animationDelay: '0.28s' }}>
            <div className={styles.sectionTitle}>Match History ({matches.length})</div>
            {matches.length === 0 ? (
              <div className={styles.empty}>No matches found.</div>
            ) : (
              <div className={styles.matchList}>
                {matches.map((m, i) => (
                  <div
                    key={m.match_id}
                    className={styles.matchRow}
                    style={{ animationDelay: `${i * 0.03}s` }}
                    onClick={() => onSelectMatch(m.match_id)}
                  >
                    <span className={m.won ? styles.winBadge : styles.lossBadge}>{m.won ? 'W' : 'L'}</span>
                    <div className={styles.matchMain}>
                      <div className={styles.matchId}>{m.match_id}</div>
                      <div className={styles.matchDate}>{new Date(m.date).toLocaleDateString()}</div>
                    </div>
                    <div className={styles.matchScore}>
                      <span style={{ color: 'var(--accent-a)' }}>{m.sets_won_a}</span>
                      <span className={styles.scoreSep}>–</span>
                      <span style={{ color: 'var(--accent-b)' }}>{m.sets_won_b}</span>
                    </div>
                    <div className={styles.matchCta}>→</div>
                  </div>
                ))}
              </div>
            )}
          </section>
        </>
      )}
    </div>
  )
}
