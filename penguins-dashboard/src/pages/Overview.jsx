import KpiCard from '../components/KpiCard'
import { useSeasonOverview, useDecadeLeader } from '../hooks/useData'
import useSeasonStore from '../store/useSeasonStore'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'

function Overview() {
  const { data, isLoading } = useSeasonOverview()
  const { data: decadeData } = useDecadeLeader()
  const { selectedSeason } = useSeasonStore()

  if (isLoading) return <div className="text-white p-8">Loading...</div>

  const filtered = selectedSeason === 'all'
    ? data
    : data.filter(d => d.season_label === selectedSeason)

  const totalGP = filtered.reduce((sum, d) => sum + d.gp, 0)
  const totalW = filtered.reduce((sum, d) => sum + d.w, 0)
  const totalL = filtered.reduce((sum, d) => sum + d.l, 0)
  const totalOL = filtered.reduce((sum, d) => sum + d.ol, 0)
  const cups = filtered.filter(d => d.playoff_finish === 'Won Stanley Cup').length

  const decadeSkaters = decadeData
    ? [...decadeData]
        .filter(d => d.position_group !== 'GOALIE')
        .sort((a, b) => b.points - a.points)
    : []

  return (
    <div className="space-y-8">

      {/* KPI Cards */}
      <div className="grid grid-cols-5 gap-4">
        <KpiCard label="Stanley Cups" value={cups} highlight />
        <KpiCard label="Games Played" value={totalGP} />
        <KpiCard label="Wins" value={totalW} />
        <KpiCard label="Losses" value={totalL} />
        <KpiCard label="OT Losses" value={totalOL} />
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-3 gap-6">

        {/* Wins by Season */}
        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-6">
          <h2 className="text-[#FFB81C] font-bold text-lg mb-4">Wins by Season</h2>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={filtered} margin={{ left: 10, right: 10, top: 10, bottom: 20 }}>
              <XAxis dataKey="season_label" stroke="#666" tick={{ fill: '#999', fontSize: 11 }} angle={-45} textAnchor="end" />
              <YAxis stroke="#666" tick={{ fill: '#999', fontSize: 12 }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1a1a1a', border: '1px solid #FFB81C', borderRadius: '8px' }}
                labelStyle={{ color: '#FFB81C' }}
                itemStyle={{ color: '#fff' }}
              />
              <Bar dataKey="w" name="Wins" radius={[4, 4, 0, 0]}>
                {filtered.map((entry) => (
                  <Cell
                    key={entry.season_label}
                    fill={entry.playoff_finish === 'Won Stanley Cup' ? '#FFB81C' : '#6b7280'}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Decade Top Scorers */}
        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-6">
          <h2 className="text-[#FFB81C] font-bold text-lg mb-4 text-center">Decade Top Scorers</h2>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[#FFB81C]">
                {['Player', 'GP', 'G', 'A', 'P', '+/-'].map(col => (
                  <th key={col} className="text-[#FFB81C] font-bold px-2 py-2 text-left text-xs">
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {decadeSkaters.slice(0, 5).map((row, i) => (
                <tr key={row.player_name} className={`border-b border-[#2a2a2a] ${i % 2 === 0 ? 'bg-[#1a1a1a]' : 'bg-[#161616]'}`}>
                  <td className="px-2 py-2 text-gray-300 font-medium">{row.player_name}</td>
                  <td className="px-2 py-2 text-gray-300">{row.games_played}</td>
                  <td className="px-2 py-2 text-gray-300">{row.goals}</td>
                  <td className="px-2 py-2 text-gray-300">{row.assists}</td>
                  <td className="px-2 py-2 text-gray-300 font-bold text-white">{row.points}</td>
                  <td className={`px-2 py-2 font-medium ${row.plus_minus >= 0 ? 'text-[#FFB81C]' : 'text-red-400'}`}>
                    {row.plus_minus > 0 ? '+' : ''}{row.plus_minus}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Goal Differential by Season */}
        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-6">
          <h2 className="text-[#FFB81C] font-bold text-lg mb-4">Goal Differential by Season</h2>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={filtered} margin={{ left: 10, right: 10, top: 10, bottom: 20 }}>
              <XAxis dataKey="season_label" stroke="#666" tick={{ fill: '#999', fontSize: 11 }} angle={-45} textAnchor="end" />
              <YAxis stroke="#666" tick={{ fill: '#999', fontSize: 12 }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1a1a1a', border: '1px solid #FFB81C', borderRadius: '8px' }}
                labelStyle={{ color: '#FFB81C' }}
                itemStyle={{ color: '#fff' }}
              />
              <Bar dataKey={(d) => d.gf - d.ga} name="Goal Diff" radius={[4, 4, 0, 0]}>
                {filtered.map((entry) => (
                  <Cell
                    key={entry.season_label}
                    fill={
                      entry.playoff_finish === 'Won Stanley Cup'
                        ? '#FFB81C'
                        : (entry.gf - entry.ga) >= 0
                        ? '#6b7280'
                        : '#ef4444'
                    }
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

      </div>

      {/* Season Summary Table */}
      <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[#FFB81C]">
              {['Season', 'GP', 'W', 'L', 'OL', 'P', 'GF', 'GA', 'W%', 'Top Scorer (Points)', 'Top Goalie (Wins)', 'Playoffs'].map(col => (
                <th key={col} className="text-[#FFB81C] font-bold px-4 py-2 text-left text-xs uppercase tracking-wider">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map((row, i) => {
              const isCup = row.playoff_finish === 'Won Stanley Cup'
              return (
                <tr
                  key={row.season_label}
                  className={`border-b border-[#2a2a2a] transition-colors hover:bg-[#222] ${i % 2 === 0 ? 'bg-[#1a1a1a]' : 'bg-[#161616]'}`}
                >
                  <td className="px-4 py-1.5 font-semibold text-[#FFB81C]">{row.season_label}</td>
                  <td className="px-4 py-1.5 text-gray-300">{row.gp}</td>
                  <td className="px-4 py-1.5 text-gray-300">{row.w}</td>
                  <td className="px-4 py-1.5 text-gray-300">{row.l}</td>
                  <td className="px-4 py-1.5 text-gray-300">{row.ol}</td>
                  <td className="px-4 py-1.5 text-gray-300">{row.p}</td>
                  <td className="px-4 py-1.5 text-gray-300">{row.gf}</td>
                  <td className="px-4 py-1.5 text-gray-300">{row.ga}</td>
                  <td className="px-4 py-1.5 text-gray-300">{(row.win_pct * 100).toFixed(1)}%</td>
                  <td className="px-4 py-1.5 text-gray-300">{row.top_scorer_display}</td>
                  <td className="px-4 py-1.5 text-gray-300">{row.top_goalie_display}</td>
                  <td className={`px-4 py-1.5 font-semibold ${isCup ? 'text-[#FFB81C]' : 'text-gray-400'}`}>
                    {row.playoff_finish}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

    </div>
  )
}

export default Overview