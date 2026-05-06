import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
  ],
})

// //debugging
// export default defineConfig(({ mode }) => {
//   // Load env file from the current directory
//   const env = loadEnv(mode, process.cwd(), '');
//   console.log('--- VITE DEBUG ---');
//   console.log('VITE_QUERY_URL:', env.VITE_QUERY_URL);
//   console.log('------------------');

//   return {
//     plugins: [react(), tailwindcss()],
//   }
// })
