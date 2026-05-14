/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        gold: {
          400: '#FFB81C',
          500: '#FFB81C',
          600: '#e6a619',
        },
        penguin: {
          black: '#000000',
          dark: '#111111',
          card: '#1a1a1a',
          border: '#2a2a2a',
        }
      },
    },
  },
  plugins: [],
}