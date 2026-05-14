import { useState } from 'react'
import { usePlayerLeader, useDecadeLeader } from '../hooks/useData'
import useSeasonStore from '../store/useSeasonStore'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from 'recharts'

function Players() {
  const { data: seasonData, isLoading: seasonLoading } = usePlayerLeader()
  const { data: decadeData, isLoading: decadeLoading } = useDecadeLeader()
  const { selectedSeason } = useSeasonStore()
  const [playerA, setPlayerA] = useState('Sidney Crosby')
  const [playerB, setPlayerB] = useState('Evgeni Malkin')

  if (seasonLoading || decadeLoading) return <div className="text-white p-8">Loading...</div>

  // Decade skaters + goalies
  const decadeSkaters = [...decadeData]
    .filter(d => d.position_group !== 'GOALIE')
    .sort((a, b) => b.points - a.points)

  const decadeGoalies = [...decadeData]
    .filter(d => d.position_group === 'GOALIE')
    .sort((a, b) => b.wins - a.wins)

  // Decade KPIs
  const totalGoals = decadeSkaters.reduce((sum, d) => sum + d.goals, 0)
  const totalAssists = decadeSkaters.reduce((sum, d) => sum + d.assists, 0)
  const totalPoints = decadeSkaters.reduce((sum, d) => sum + d.points, 0)
  const topScorer = decadeSkaters[0]
  const topGoalie = decadeGoalies[0]

  // All unique skater names for picker
  const skaterNames = [...new Set(
    seasonData
      .filter(d => d.position_group !== 'GOALIE')
      .map(d => d.player_name)
  )].sort()

  // Per season data for comparison chart
  const allSeasons = ['2010-11','2011-12','2012-13','2013-14','2014-15','2015-16','2016-17','2017-18','2018-19','2019-20']

  const chartData = allSeasons.map(season => {
    const aRow = seasonData.find(d => d.player_name === playerA && d.season_label === season)
    const bRow = seasonData.find(d => d.player_name === playerB && d.season_label === season)
    return {
      season,
      [playerA]: aRow ? aRow.points : null,
      [playerB]: bRow ? bRow.points : null,
    }
  })

  return (
    <div className="space-y-8">

      {/* KPI Cards */}
      <div className="grid grid-cols-5 gap-4">
        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 flex flex-col items-center justify-center">
          <span className="text-gray-400 text-xs font-semibold uppercase tracking-widest mb-2">Decade Goals</span>
          <span className="text-4xl font-black text-white">{totalGoals}</span>
        </div>
        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 flex flex-col items-center justify-center">
          <span className="text-gray-400 text-xs font-semibold uppercase tracking-widest mb-2">Decade Assists</span>
          <span className="text-4xl font-black text-white">{totalAssists}</span>
        </div>
        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 flex flex-col items-center justify-center">
          <span className="text-gray-400 text-xs font-semibold uppercase tracking-widest mb-2">Decade Points</span>
          <span className="text-4xl font-black text-white">{totalPoints}</span>
        </div>
        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 flex flex-col items-center justify-center">
          <span className="text-gray-400 text-xs font-semibold uppercase tracking-widest mb-2">Top Scorer</span>
          <span className="text-lg font-black text-[#FFB81C] text-center">{topScorer?.player_name}</span>
          <span className="text-sm text-gray-400">{topScorer?.points} pts</span>
        </div>
        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 flex flex-col items-center justify-center">
          <span className="text-gray-400 text-xs font-semibold uppercase tracking-widest mb-2">Top Goalie</span>
          <span className="text-lg font-black text-[#FFB81C] text-center">{topGoalie?.player_name}</span>
          <span className="text-sm text-gray-400">{topGoalie?.wins} wins</span>
        </div>
      </div>

      {/* Decade Tables */}
      <div className="grid grid-cols-2 gap-6">

        {/* Skater Leaders */}
        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-6">
          <h2 className="text-[#FFB81C] font-bold text-lg mb-4">Decade Skater Leaders</h2>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[#FFB81C]">
                {['Player', 'GP', 'G', 'A', 'P', '+/-'].map(col => (
                  <th key={col} className="text-[#FFB81C] font-bold px-2 py-2 text-left text-xs">{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {decadeSkaters.slice(0, 10).map((row, i) => (
                <tr key={row.player_name} className={`border-b border-[#2a2a2a] ${i % 2 === 0 ? 'bg-[#1a1a1a]' : 'bg-[#161616]'}`}>
                  <td className="px-2 py-1.5 text-gray-300 font-medium">{row.player_name}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.games_played}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.goals}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.assists}</td>
                  <td className="px-2 py-1.5 font-bold text-white">{row.points}</td>
                  <td className={`px-2 py-1.5 font-medium ${row.plus_minus >= 0 ? 'text-[#FFB81C]' : 'text-red-400'}`}>
                    {row.plus_minus > 0 ? '+' : ''}{row.plus_minus}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Goalie Leaders */}
        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-6">
          <h2 className="text-[#FFB81C] font-bold text-lg mb-4">Decade Goalie Leaders</h2>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[#FFB81C]">
                {['Player', 'GP', 'W', 'L', 'SV%', 'GAA'].map(col => (
                  <th key={col} className="text-[#FFB81C] font-bold px-2 py-2 text-left text-xs">{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {decadeGoalies.slice(0, 10).map((row, i) => (
                <tr key={row.player_name} className={`border-b border-[#2a2a2a] ${i % 2 === 0 ? 'bg-[#1a1a1a]' : 'bg-[#161616]'}`}>
                  <td className="px-2 py-1.5 text-gray-300 font-medium">{row.player_name}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.games_played}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.wins}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.losses}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.save_pct ? (row.save_pct * 100).toFixed(1) + '%' : '-'}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.gaa ? row.gaa.toFixed(2) : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

      </div>

      {/* Player Comparison */}
      <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-6">
        <h2 className="text-[#FFB81C] font-bold text-lg mb-4">Player Points Comparison</h2>

        {/* Pickers */}
        <div className="flex gap-4 mb-6">
          <div className="flex flex-col gap-1">
            <label className="text-xs text-gray-400 uppercase tracking-wider">Player A</label>
            <select
              value={playerA}
              onChange={e => setPlayerA(e.target.value)}
              className="bg-[#111] border border-[#FFB81C] text-white rounded px-3 py-1.5 text-sm"
            >
              {skaterNames.map(n => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>
          <div className="flex flex-col gap-1">
            <label className="text-xs text-gray-400 uppercase tracking-wider">Player B</label>
            <select
              value={playerB}
              onChange={e => setPlayerB(e.target.value)}
              className="bg-[#111] border border-[#FFB81C] text-white rounded px-3 py-1.5 text-sm"
            >
              {skaterNames.map(n => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>
        </div>

        <ResponsiveContainer width="100%" height={250}>
          <LineChart data={chartData} margin={{ left: 10, right: 30, top: 10, bottom: 10 }}>
            <XAxis dataKey="season" stroke="#666" tick={{ fill: '#999', fontSize: 12 }} />
            <YAxis stroke="#666" tick={{ fill: '#999', fontSize: 12 }} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1a1a1a', border: '1px solid #FFB81C', borderRadius: '8px' }}
              labelStyle={{ color: '#FFB81C' }}
              itemStyle={{ color: '#fff' }}
            />
            <Legend wrapperStyle={{ color: '#999' }} />
            <Line type="monotone" dataKey={playerA} stroke="#FFB81C" strokeWidth={2} dot={{ fill: '#FFB81C' }} connectNulls />
            <Line type="monotone" dataKey={playerB} stroke="#6b7280" strokeWidth={2} dot={{ fill: '#6b7280' }} connectNulls />
          </LineChart>
        </ResponsiveContainer>
      </div>

    </div>
  )
}

export default Players