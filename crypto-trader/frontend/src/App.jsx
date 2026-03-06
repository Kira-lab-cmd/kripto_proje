import { useState, useEffect } from 'react';
import axios from 'axios';
import {
  Container, Typography, Grid, Card, CardContent, Button,
  Table, TableBody, TableCell, TableHead, TableRow, Alert, Chip, Box
} from '@mui/material';
import { Refresh, Warning } from '@mui/icons-material';

// API Adresi (İleride .env dosyasına taşınabilir)
const API_URL = (import.meta.env.VITE_API_URL || 'http://localhost:8000').replace(/\/$/, '');

function App() {
  const [positions, setPositions] = useState([]);
  const [trades, setTrades] = useState([]);
  const [summary, setSummary] = useState({ open_positions: 0, realized_pnl: 0, total_pnl: 0 });
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const summaryPnl = summary.realized_pnl ?? summary.total_pnl ?? 0;

  const fetchData = async () => {
    setLoading(true);
    try {
      // Promise.all ile paralel istek atıyoruz (Daha hızlı)
      const [posRes, tradeRes, sumRes] = await Promise.all([
        axios.get(`${API_URL}/dashboard/positions`),
        axios.get(`${API_URL}/dashboard/trades`),
        axios.get(`${API_URL}/dashboard/summary`)
      ]);

      setPositions(posRes.data);
      setTrades(tradeRes.data);
      setSummary(sumRes.data);
      setError(null);
    } catch (err) {
      console.error(err);
      setError('Backend bağlantısı başarısız! Bot çalışıyor mu?');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000); // 5 saniyede bir canlı veri
    return () => clearInterval(interval);
  }, []);

  const handleForceClose = async (symbol) => {
    // FastAPI route uses {symbol:path}. Keep the original (e.g. "ETH/USDT") and URL-encode it -> "ETH%2FUSDT".
    const encSymbol = encodeURIComponent(symbol);

    if (!window.confirm(`${symbol} pozisyonunu ACİL KAPATMAK istiyor musunuz?`)) return;

    try {
      await axios.post(`${API_URL}/trade/close/${encSymbol}`);
      alert('Satış emri gönderildi!');
      fetchData(); // Tabloyu yenile
    } catch (err) {
      alert('Hata: ' + (err.response?.data?.detail || err.message));
    }
  };

  return (
    <Container maxWidth="xl" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4" component="h1" fontWeight="bold">
          🤖 AI Crypto Dashboard
        </Typography>
        <Button startIcon={<Refresh />} variant="outlined" onClick={fetchData} disabled={loading}>
          Yenile
        </Button>
      </Box>

      {error && <Alert severity="error" sx={{ mb: 3 }}>{error}</Alert>}

      <Grid container spacing={3}>
        {/* ÖZET KARTLARI */}
        <Grid item xs={12} md={4}>
          <Card sx={{ bgcolor: '#e3f2fd' }}>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>Açık Pozisyonlar</Typography>
              <Typography variant="h3">{summary.open_positions}</Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={4}>
          <Card sx={{ bgcolor: summaryPnl >= 0 ? '#e8f5e9' : '#ffebee' }}>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>Toplam Kâr (Realized PnL)</Typography>
              <Typography variant="h3" color={summaryPnl >= 0 ? 'success.main' : 'error.main'}>
                {summaryPnl?.toFixed(2)}$
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        {/* AÇIK POZİSYONLAR TABLOSU */}
        <Grid item xs={12} lg={6}>
          <Card elevation={3}>
            <CardContent>
              <Typography variant="h6" gutterBottom>⚡ Aktif İşlemler</Typography>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Coin</TableCell>
                    <TableCell>Giriş</TableCell>
                    <TableCell>Güncel</TableCell>
                    <TableCell>PnL (Anlık)</TableCell>
                    <TableCell>İşlem</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {positions.length === 0 ? (
                    <TableRow><TableCell colSpan={5} align="center">Açık işlem yok</TableCell></TableRow>
                  ) : (
                    positions.map((pos) => (
                      <TableRow key={pos.symbol}>
                        <TableCell fontWeight="bold">{pos.symbol}</TableCell>
                        <TableCell>{pos.entry_price?.toFixed(4)}</TableCell>
                        <TableCell>{pos.current_price?.toFixed(4)}</TableCell>
                        <TableCell>
                          <Chip
                            label={`${pos.unrealized_pnl?.toFixed(2)}$`}
                            color={pos.unrealized_pnl >= 0 ? "success" : "error"}
                            size="small"
                          />
                        </TableCell>
                        <TableCell>
                          <Button
                            variant="contained"
                            color="error"
                            size="small"
                            startIcon={<Warning />}
                            onClick={() => handleForceClose(pos.symbol)}
                          >
                            Kapat
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </Grid>

        {/* GEÇMİŞ İŞLEMLER */}
        <Grid item xs={12} lg={6}>
          <Card elevation={3}>
            <CardContent>
              <Typography variant="h6" gutterBottom>📜 İşlem Geçmişi</Typography>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Zaman</TableCell>
                    <TableCell>Coin</TableCell>
                    <TableCell>Yön</TableCell>
                    <TableCell>Fiyat</TableCell>
                    <TableCell>Miktar</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {trades.map((trade, i) => (
                    <TableRow key={i}>
                      <TableCell>{new Date(trade.timestamp).toLocaleTimeString()}</TableCell>
                      <TableCell>{trade.symbol}</TableCell>
                      <TableCell>
                        <span style={{ color: trade.side === 'BUY' ? 'green' : 'red', fontWeight: 'bold' }}>
                          {trade.side}
                        </span>
                      </TableCell>
                      <TableCell>{trade.price?.toFixed(4)}</TableCell>
                      <TableCell>{trade.amount?.toFixed(4)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Container>
  );
}

export default App;
