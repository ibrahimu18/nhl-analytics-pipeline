import { useEffect, useRef } from 'react'
import { usePlayoffTeam, usePlayoffOpponent } from '../hooks/useData'
import * as d3 from 'd3'

function ForceGraph({ opponents }) {
  const svgRef = useRef()

  useEffect(() => {
    if (!opponents || opponents.length === 0) return

    const width = 600
    const height = 400

    d3.select(svgRef.current).selectAll('*').remove()

    const svg = d3.select(svgRef.current)
      .attr('viewBox', `0 0 ${width} ${height}`)
      .attr('width', '100%')
      .attr('height', '100%')

    const nodes = [
      { id: 'PIT', isCenter: true },
      ...opponents.map(d => ({ id: d.opponent, isCenter: false, data: d }))
    ]

    const links = opponents.map(d => ({
      source: 'PIT',
      target: d.opponent,
      seriesWin: d.series_wins > d.series_losses,
      games: d.games_played
    }))

    const simulation = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links).id(d => d.id).distance(150))
      .force('charge', d3.forceManyBody().strength(-300))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collision', d3.forceCollide().radius(40))

    const link = svg.append('g')
      .selectAll('line')
      .data(links)
      .join('line')
      .attr('stroke', d => d.seriesWin ? '#FFB81C' : '#ef4444')
      .attr('stroke-width', d => Math.max(1, d.games / 2))
      .attr('stroke-opacity', 0.7)

    const node = svg.append('g')
      .selectAll('g')
      .data(nodes)
      .join('g')
      .call(d3.drag()
        .on('start', (event, d) => {
          if (!event.active) simulation.alphaTarget(0.3).restart()
          d.fx = d.x; d.fy = d.y
        })
        .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y })
        .on('end', (event, d) => {
          if (!event.active) simulation.alphaTarget(0)
          d.fx = null; d.fy = null
        })
      )

    node.append('circle')
      .attr('r', d => d.isCenter ? 28 : 20)
      .attr('fill', d => d.isCenter ? '#FFB81C' : '#1a1a1a')
      .attr('stroke', d => d.isCenter ? '#FFB81C' : '#6b7280')
      .attr('stroke-width', 2)

    node.append('text')
      .text(d => d.id)
      .attr('text-anchor', 'middle')
      .attr('dominant-baseline', 'central')
      .attr('fill', d => d.isCenter ? '#000' : '#fff')
      .attr('font-size', d => d.isCenter ? '11px' : '9px')
      .attr('font-weight', 'bold')

    simulation.on('tick', () => {
      link
        .attr('x1', d => d.source.x)
        .attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x)
        .attr('y2', d => d.target.y)

      node.attr('transform', d => `translate(${d.x},${d.y})`)
    })

    return () => simulation.stop()
  }, [opponents])

  return <svg ref={svgRef} style={{ width: '100%', height: '400px' }} />
}

function Playoffs() {
  const { data: teamData, isLoading: teamLoading } = usePlayoffTeam()
  const { data: oppData, isLoading: oppLoading } = usePlayoffOpponent()

  if (teamLoading || oppLoading) return <div className="text-white p-8">Loading...</div>

  const playoffSeasons = teamData.filter(d => d.playoff_games > 0)

  const totalGames = playoffSeasons.reduce((sum, d) => sum + d.playoff_games, 0)
  const totalWins = playoffSeasons.reduce((sum, d) => sum + d.playoff_wins, 0)
  const totalLosses = playoffSeasons.reduce((sum, d) => sum + d.playoff_losses, 0)
  const totalSeriesWins = playoffSeasons.reduce((sum, d) => sum + d.series_wins, 0)
  const totalSeriesLosses = playoffSeasons.reduce((sum, d) => sum + d.series_losses, 0)

  return (
    <div className="space-y-8">

      {/* KPI Cards */}
      <div className="grid grid-cols-5 gap-4">
        {[
          { label: 'Playoff Games', value: totalGames },
          { label: 'Playoff Wins', value: totalWins },
          { label: 'Playoff Losses', value: totalLosses },
          { label: 'Series Wins', value: totalSeriesWins },
          { label: 'Series Losses', value: totalSeriesLosses },
        ].map(({ label, value }) => (
          <div key={label} className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 flex flex-col items-center justify-center">
            <span className="text-gray-400 text-xs font-semibold uppercase tracking-widest mb-2">{label}</span>
            <span className="text-4xl font-black text-white">{value}</span>
          </div>
        ))}
      </div>

      {/* Tables Row */}
      <div className="grid grid-cols-2 gap-6">

        {/* Playoff Finish by Season */}
        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-6">
          <h2 className="text-[#FFB81C] font-bold text-lg mb-4">Playoff Finish by Season</h2>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[#FFB81C]">
                {['Season', 'GP', 'W', 'L', 'Series W', 'Series L', 'Result'].map(col => (
                  <th key={col} className="text-[#FFB81C] font-bold px-2 py-2 text-left text-xs">{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {teamData.map((row, i) => {
                const isCup = row.playoff_finish === 'Won Stanley Cup'
                const missed = row.playoff_finish === 'Missed playoffs'
                return (
                  <tr key={row.season} className={`border-b border-[#2a2a2a] ${i % 2 === 0 ? 'bg-[#1a1a1a]' : 'bg-[#161616]'}`}>
                    <td className="px-2 py-1.5 text-[#FFB81C] font-semibold">{row.season_label}</td>
                    <td className="px-2 py-1.5 text-gray-300">{row.playoff_games || '-'}</td>
                    <td className="px-2 py-1.5 text-gray-300">{row.playoff_wins || '-'}</td>
                    <td className="px-2 py-1.5 text-gray-300">{row.playoff_losses || '-'}</td>
                    <td className="px-2 py-1.5 text-gray-300">{row.series_wins ?? '-'}</td>
                    <td className="px-2 py-1.5 text-gray-300">{row.series_losses ?? '-'}</td>
                    <td className={`px-2 py-1.5 font-semibold text-xs ${isCup ? 'text-[#FFB81C]' : missed ? 'text-gray-600' : 'text-gray-400'}`}>
                      {row.playoff_finish}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>

        {/* Opponent Head-to-Head */}
        <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-6">
          <h2 className="text-[#FFB81C] font-bold text-lg mb-4">Playoff Opponent Record</h2>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[#FFB81C]">
                {['Opponent', 'Series', 'W', 'L', 'GP', 'GF', 'GA', 'Diff'].map(col => (
                  <th key={col} className="text-[#FFB81C] font-bold px-2 py-2 text-left text-xs">{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {oppData.map((row, i) => (
                <tr key={row.opponent} className={`border-b border-[#2a2a2a] ${i % 2 === 0 ? 'bg-[#1a1a1a]' : 'bg-[#161616]'}`}>
                  <td className="px-2 py-1.5 text-gray-300 font-medium">{row.opponent}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.series_head_to_head}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.wins}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.losses}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.games_played}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.goals_for}</td>
                  <td className="px-2 py-1.5 text-gray-300">{row.goals_against}</td>
                  <td className={`px-2 py-1.5 font-medium ${row.goal_diff >= 0 ? 'text-[#FFB81C]' : 'text-red-400'}`}>
                    {row.goal_diff > 0 ? '+' : ''}{row.goal_diff}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

      </div>

      {/* D3 Force Graph */}
      <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-6">
        <h2 className="text-[#FFB81C] font-bold text-lg mb-2">Playoff Opponent Network</h2>
        <p className="text-gray-400 text-xs mb-4">Gold lines = series win · Red lines = series loss · Line thickness = games played · Nodes are draggable</p>
        <ForceGraph opponents={oppData} />
      </div>

    </div>
  )
}

export default Playoffs