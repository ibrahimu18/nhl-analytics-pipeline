import { Link, useLocation } from 'react-router-dom'

function Navbar() {
  const location = useLocation()

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
    </nav>
  )
}

export default Navbar