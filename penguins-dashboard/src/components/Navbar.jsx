import { Link, useLocation } from 'react-router-dom'
import useSeasonStore from '../store/useSeasonStore'

const seasons = [
  'all', '2010-11', '2011-12', '2012-13', '2013-14', '2014-15',
  '2015-16', '2016-17', '2017-18', '2018-19', '2019-20'
]

function Navbar() {
  const location = useLocation()
  const { selectedSeason, setSelectedSeason } = useSeasonStore()

  const navLinks = [
    { path: '/', label: 'Overview' },
    { path: '/players', label: 'Players' },
    { path: '/playoffs', label: 'Playoffs' },
  ]

  return (
    <nav className="bg-black border-b border-[#FFB81C] px-6 py-4 flex items-center justify-between">
      <div className="flex items-center gap-2">
        <span className="text-[#FFB81C] font-black text-xl tracking-tight">PIT</span>
        <span className="text-white font-semibold text-lg">Penguins Dashboard</span>
      </div>

      <div className="flex items-center gap-6">
        {navLinks.map(({ path, label }) => (
          <Link
            key={path}
            to={path}
            className={`text-sm font-semibold transition-colors ${
              location.pathname === path
                ? 'text-[#FFB81C]'
                : 'text-gray-400 hover:text-white'
            }`}
          >
            {label}
          </Link>
        ))}
      </div>

      <select
        value={selectedSeason}
        onChange={(e) => setSelectedSeason(e.target.value)}
        className="bg-[#1a1a1a] text-white border border-[#FFB81C] rounded px-3 py-1.5 text-sm"
      >
        {seasons.map((s) => (
          <option key={s} value={s}>
            {s === 'all' ? 'All Seasons' : s}
          </option>
        ))}
      </select>
    </nav>
  )
}

export default Navbar