import logo from '../assets/cp-logo.png'
import styles from './Header.module.css'

export function Header({ nav, onGoPlayers, onGoPlayer }) {
  return (
    <header className={styles.header}>
      <div className={styles.brand}>
        <img src={logo} alt="CentrePitch" className={styles.logo} />
        <div>
          <div className={styles.title}>Centre-Pitch</div>
          <div className={styles.subtitle}>Analytics Platform</div>
        </div>
      </div>

      <nav className={styles.breadcrumb}>
        <span className={styles.crumb} onClick={onGoPlayers}>Players</span>
        {(nav.view === 'player' || nav.view === 'match') && (
          <>
            <span className={styles.sep}>›</span>
            <span
              className={`${styles.crumb} ${nav.view === 'player' ? styles.active : ''}`}
              onClick={onGoPlayer}
            >
              {nav.playerName}
            </span>
          </>
        )}
        {nav.view === 'match' && (
          <>
            <span className={styles.sep}>›</span>
            <span className={`${styles.crumb} ${styles.active}`}>{nav.matchId}</span>
          </>
        )}
      </nav>
    </header>
  )
}
