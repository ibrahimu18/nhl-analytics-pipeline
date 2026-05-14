import { create } from 'zustand'

const useSeasonStore = create((set) => ({
  selectedSeason: 'all',
  setSelectedSeason: (season) => set({ selectedSeason: season }),
}))

export default useSeasonStore