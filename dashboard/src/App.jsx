import { useState } from 'react'
import { Header } from './components/Header.jsx'
import { PlayerSelectView } from './views/PlayerSelectView.jsx'
import { PlayerView } from './views/PlayerView.jsx'
import { MatchView } from './views/MatchView.jsx'
import styles from './App.module.css'

export default function App() {
  const [nav, setNav] = useState({ view: 'players', playerId: null, playerName: null, matchId: null })

  const goPlayers = () => setNav({ view: 'players', playerId: null, playerName: null, matchId: null })
  const goPlayer  = (playerId, playerName) => setNav({ view: 'player', playerId, playerName, matchId: null })
  const goMatch   = (matchId) => setNav(n => ({ ...n, view: 'match', matchId }))

  return (
    <div className={styles.layout}>
      <Header nav={nav} onGoPlayers={goPlayers} onGoPlayer={() => goPlayer(nav.playerId, nav.playerName)} />
      <main className={styles.main}>
        {nav.view === 'players' && <PlayerSelectView onSelectPlayer={goPlayer} />}
        {nav.view === 'player'  && <PlayerView playerId={nav.playerId} playerName={nav.playerName} onSelectMatch={goMatch} />}
        {nav.view === 'match'   && <MatchView matchId={nav.matchId} />}
      </main>
    </div>
  )
}
