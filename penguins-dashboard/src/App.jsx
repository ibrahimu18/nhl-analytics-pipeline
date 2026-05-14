import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import Navbar from './components/Navbar'
import Overview from './pages/Overview'
import Players from './pages/Players'
import Playoffs from './pages/Playoffs'

const queryClient = new QueryClient()

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <div className="min-h-screen bg-[#111111]">
          <Navbar />
          <main className="p-6">
            <Routes>
              <Route path="/" element={<Overview />} />
              <Route path="/players" element={<Players />} />
              <Route path="/playoffs" element={<Playoffs />} />
            </Routes>
          </main>
        </div>
      </BrowserRouter>
    </QueryClientProvider>
  )
}

export default App