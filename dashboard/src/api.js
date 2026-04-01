import axios from 'axios'

const http = axios.create({ baseURL: 'http://localhost:8001' })

export const api = {
  players: {
    list:    ()   => http.get('/players').then(r => r.data),
    profile: (id) => http.get(`/players/${id}/profile`).then(r => r.data),
    matches: (id, limit = 50, offset = 0) =>
      http.get(`/players/${id}/matches`, { params: { limit, offset } })
          .then(r => r.data),   // keep paginated envelope
    trends:  (id) => http.get(`/players/${id}/trends`).then(r => r.data),
    h2h:     (id) => http.get(`/players/${id}/h2h`).then(r => r.data),
  },
  matches: {
    summary:      (id)           => http.get(`/matches/${id}/summary`).then(r => r.data),
    fullAnalytics:(id)           => http.get(`/matches/${id}/full-analytics`).then(r => r.data),
    setAnalytics: (id, setNum)   => http.get(`/matches/${id}/sets/${setNum}/analytics`).then(r => r.data),
  },
}
