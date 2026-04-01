import { useEffect, useState } from 'react'
import { api } from '../api.js'
import { SkeletonCard } from '../components/SkeletonCard.jsx'
import styles from './PlayerSelectView.module.css'

const AVATARS = ['A','B','C','D','E','F','G','H']

export function PlayerSelectView({ onSelectPlayer }) {
  const [players, setPlayers] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)

  useEffect(() => {
    api.players.list()
      .then(setPlayers)
      .catch(() => setError('Could not load players'))
      .finally(() => setLoading(false))
  }, [])

  return (
    <div className={styles.view}>
      <div className={styles.pageHeader}>
        <h1 className={styles.pageTitle}>Select Player</h1>
        <p className={styles.pageSubtitle}>Choose a player to view their match history and analytics</p>
      </div>

      {error && <div className={styles.error}>{error}</div>}

      {loading ? (
        <div className={styles.grid}>
          {[...Array(6)].map((_, i) => <SkeletonCard key={i} height={140} />)}
        </div>
      ) : (
        <div className={styles.grid}>
          {players.map((p, i) => (
            <div key={p.player_id} className={`${styles.card} animate-in`}
              style={{ animationDelay: `${i * 0.05}s` }}
              onClick={() => onSelectPlayer(p.player_id, p.name)}
            >
              <div className={styles.avatar}>{AVATARS[i] ?? p.name[0]}</div>
              <div className={styles.name}>{p.name}</div>
              <div className={styles.sport}>{p.sport_id}</div>
              <div className={styles.cta}>View profile →</div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
