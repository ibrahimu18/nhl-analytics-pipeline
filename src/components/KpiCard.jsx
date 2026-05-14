function KpiCard({ label, value, highlight = false }) {
  return (
    <div className="bg-[#1a1a1a] border border-[#2a2a2a] rounded-lg p-4 flex flex-col items-center justify-center">
      <span className="text-gray-400 text-xs font-semibold uppercase tracking-widest mb-2">
        {label}
      </span>
      <span className={`text-4xl font-black ${highlight ? 'text-[#FFB81C]' : 'text-white'}`}>
        {value}
      </span>
    </div>
  )
}

export default KpiCard