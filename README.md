# Backend

Standalone PostgreSQL-backed backend for Real Matka.

## Run

```powershell
cd "C:\Users\SDT-WEDDING\Desktop\realmatka app\Backend"
npm install
npm start
```

## Verification

```powershell
cd "C:\Users\SDT-WEDDING\Desktop\realmatka app\Backend"
npm run check:syntax
```

Health endpoint:

- `http://localhost:3000/health`

## Environment

Create `.env.local` from `.env.example` and set your local PostgreSQL connection.

Important variables:

- `DATABASE_PROVIDER=postgres`
- `DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/realmatka`
- `EXPO_PUBLIC_API_BASE_URL=http://localhost:3000`
- `EXPO_PUBLIC_APP_URL=http://localhost:8083`
- `ADMIN_DOMAIN=http://localhost:5501`

## Production notes

- Use `https://` origins for deployed web/mobile clients
- Keep PostgreSQL schema imported in the `realmatka` database
- Restart the backend after env or schema changes
