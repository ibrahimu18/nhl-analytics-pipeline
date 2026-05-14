import { useQuery } from '@tanstack/react-query'

const fetchJson = async (filename) => {
  const res = await fetch(`/data/${filename}.json`)
  return res.json()
}

export const useSeasonOverview = () =>
  useQuery({ queryKey: ['seasonOverview'], queryFn: () => fetchJson('season_overview') })

export const usePlayerLeader = () =>
  useQuery({ queryKey: ['playerLeader'], queryFn: () => fetchJson('regular_player_leader') })

export const useDecadeLeader = () =>
  useQuery({ queryKey: ['decadeLeader'], queryFn: () => fetchJson('regular_player_decade_leader') })

export const usePlayoffTeam = () =>
  useQuery({ queryKey: ['playoffTeam'], queryFn: () => fetchJson('playoff_team_summary') })

export const usePlayoffOpponent = () =>
  useQuery({ queryKey: ['playoffOpponent'], queryFn: () => fetchJson('playoff_opponent_summary') })

export const useOpponentRecord = () =>
  useQuery({ queryKey: ['opponentRecord'], queryFn: () => fetchJson('opponent_record') })

export const useHomeAway = () =>
  useQuery({ queryKey: ['homeAway'], queryFn: () => fetchJson('home_away_summary') })

export const useMonthlySummary = () =>
  useQuery({ queryKey: ['monthlySummary'], queryFn: () => fetchJson('monthly_summary') })