import { useState, useEffect, useCallback, useRef } from 'react';
import axios from 'axios';
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer,
  BarChart, Bar, Cell, PieChart, Pie
} from 'recharts';

const API = (import.meta.env.VITE_API_URL || 'http://localhost:8000').replace(/\/$/, '');

const C = {
  bg:'#0a0e1a', surface:'#0f1629', border:'#1a2540',
  accent:'#00d4ff', green:'#00ff88', red:'#ff4466',
  yellow:'#ffcc00', muted:'#4a5a7a', text:'#c8d8f0', dim:'#6a7a9a',
};

const fmt = {
  usd:  (v) => v == null ? '—' : `$${Number(v).toFixed(2)}`,
  price:(v) => v == null ? '—' : Number(v) > 1000
    ? `$${Number(v).toLocaleString('en',{minimumFractionDigits:2,maximumFractionDigits:2})}`
    : `$${Number(v).toFixed(4)}`,
  qty:  (v) => v == null ? '—' : Number(v).toFixed(6),
  time: (v) => {
    if (!v) return '—';
    const d = new Date(typeof v==='number' ? v*1000 : v);
    return d.toLocaleTimeString('tr-TR',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
  },
  age:  (v) => {
    if (!v) return '—';
    const d = new Date(typeof v==='number' ? v*1000 : v);
    const ms = Date.now()-d.getTime();
    const h = Math.floor(ms/3600000), m = Math.floor((ms%3600000)/60000);
    return h>0 ? `${h}s ${m}d` : `${m}d`;
  }
};

const pnlColor = (v) => Number(v)>=0 ? C.green : C.red;
const regimeColor = (r) => ({TREND:C.yellow,CHOP:C.accent,HIGH_VOL:C.red,UNKNOWN:C.muted})[r]||C.muted;

const Dot = ({color}) => <span style={{display:'inline-block',width:8,height:8,borderRadius:'50%',background:color,marginRight:6}}/>;

const Badge = ({label,color}) => (
  <span style={{display:'inline-flex',alignItems:'center',padding:'2px 10px',borderRadius:12,
    fontSize:11,fontWeight:700,letterSpacing:'0.05em',background:`${color}22`,color,border:`1px solid ${color}44`}}>
    {label}
  </span>
);

const Stat = ({label,value,color,sub}) => (
  <div style={{padding:'16px 20px'}}>
    <div style={{fontSize:11,color:C.dim,letterSpacing:'0.08em',textTransform:'uppercase',marginBottom:4}}>{label}</div>
    <div style={{fontSize:28,fontWeight:800,color:color||C.text,fontFamily:'monospace',lineHeight:1}}>{value}</div>
    {sub && <div style={{fontSize:12,color:C.muted,marginTop:4}}>{sub}</div>}
  </div>
);

const Card = ({children,style={}}) => (
  <div style={{background:C.surface,border:`1px solid ${C.border}`,borderRadius:12,overflow:'hidden',...style}}>
    {children}
  </div>
);

const CardHeader = ({title,badge,right}) => (
  <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',
    padding:'14px 20px',borderBottom:`1px solid ${C.border}`}}>
    <div style={{display:'flex',alignItems:'center',gap:10}}>
      <span style={{fontSize:13,fontWeight:700,color:C.text,letterSpacing:'0.04em'}}>{title}</span>
      {badge}
    </div>
    {right}
  </div>
);

const Btn = ({label,onClick,color=C.accent,small,danger}) => (
  <button onClick={onClick} style={{
    background:danger?`${C.red}22`:`${color}15`,
    border:`1px solid ${danger?C.red:color}44`,
    color:danger?C.red:color,borderRadius:8,
    padding:small?'4px 12px':'8px 18px',
    fontSize:small?11:13,fontWeight:700,cursor:'pointer',letterSpacing:'0.04em',
  }}>{label}</button>
);

export default function App() {
  const [positions,setPositions] = useState([]);
  const [trades,setTrades]       = useState([]);
  const [summary,setSummary]     = useState({});
  const [botState,setBotState]   = useState({});
  const [error,setError]         = useState(null);
  const [lastUpdate,setLastUpdate] = useState(null);
  const [equityHistory,setEquityHistory] = useState([]);
  const [tab,setTab]             = useState('overview');
  const equityRef = useRef([]);

  const fetchAll = useCallback(async () => {
    try {
      const [posR,trR,sumR,stR] = await Promise.all([
        axios.get(`${API}/dashboard/positions`),
        axios.get(`${API}/dashboard/trades?limit=50`),
        axios.get(`${API}/dashboard/summary`),
        axios.get(`${API}/bot/state`),
      ]);
      setPositions(posR.data||[]);
      setTrades(trR.data||[]);
      setSummary(sumR.data||{});
      setBotState(stR.data||{});
      setError(null);
      const eq = stR.data?.paper_equity_usdt;
      if (eq!=null) {
        equityRef.current = [...equityRef.current.slice(-59),{t:Date.now(),v:eq}];
        setEquityHistory([...equityRef.current]);
      }
      setLastUpdate(new Date());
    } catch(e) { setError('Backend bağlantısı yok'); }
  },[]);

  useEffect(()=>{ fetchAll(); const id=setInterval(fetchAll,5000); return ()=>clearInterval(id); },[fetchAll]);

  const forceClose = async (symbol) => {
    if (!window.confirm(`${symbol} pozisyonu kapatılsın mı?`)) return;
    try { await axios.post(`${API}/trade/close/${encodeURIComponent(symbol)}`); fetchAll(); }
    catch(e) { alert('Hata: '+(e.response?.data?.detail||e.message)); }
  };

  const toggleBot = async () => {
    await axios.post(`${API}${botState.enabled?'/control/stop':'/control/start'}`);
    fetchAll();
  };

  const panicClose = async () => {
    if (!window.confirm('TÜM pozisyonları kapat?')) return;
    await axios.post(`${API}/control/panic_close_all`); fetchAll();
  };

  const regimeDist = botState.regime_distribution_last_n?.dist||{};
  const regimeTotal = Object.values(regimeDist).reduce((a,b)=>a+b,0)||1;
  const regimePie = Object.entries(regimeDist).map(([name,value])=>({name,value,pct:((value/regimeTotal)*100).toFixed(1)}));

  const realizedPnl = summary.realized_pnl??0;
  const equity = botState.paper_equity_usdt??1000;
  const unrealized = positions.reduce((s,p)=>s+(p.unrealized_pnl||0),0);
  const closedTrades = trades.filter(t=>['SELL','sell'].includes(t.side));
  const winTrades = closedTrades.filter(t=>(t.realized_pnl??0)>0);
  const winRate = closedTrades.length>0 ? (winTrades.length/closedTrades.length*100).toFixed(0) : '—';

  const thStyle = {padding:'9px 14px',textAlign:'left',fontSize:11,color:C.dim,fontWeight:600,letterSpacing:'0.06em'};
  const tdStyle = {padding:'8px 14px'};

  return (
    <div style={{minHeight:'100vh',background:C.bg,color:C.text,
      fontFamily:"'JetBrains Mono','Fira Code','Cascadia Code',monospace",fontSize:13}}>

      {/* HEADER */}
      <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',
        padding:'16px 28px',borderBottom:`1px solid ${C.border}`,
        background:`${C.surface}cc`,backdropFilter:'blur(10px)',
        position:'sticky',top:0,zIndex:100}}>
        <div style={{display:'flex',alignItems:'center',gap:16}}>
          <div style={{fontSize:18,fontWeight:900,letterSpacing:'-0.02em',color:C.accent}}>◈ GRID BOT</div>
          <Badge label={botState.enabled?'● ÇALIŞIYOR':'○ DURDURULDU'} color={botState.enabled?C.green:C.red}/>
          {error && <Badge label="⚠ BAĞLANTI YOK" color={C.red}/>}
        </div>
        <div style={{display:'flex',alignItems:'center',gap:10}}>
          <span style={{fontSize:11,color:C.dim}}>{lastUpdate?`güncellendi ${fmt.time(lastUpdate)}`:'...'}</span>
          <Btn label={botState.enabled?'⏸ Durdur':'▶ Başlat'} onClick={toggleBot}
            color={botState.enabled?C.yellow:C.green} small/>
          <Btn label="⚠ Tümünü Kapat" onClick={panicClose} danger small/>
          <Btn label="↻" onClick={fetchAll} small/>
        </div>
      </div>

      {/* SEKMELER */}
      <div style={{display:'flex',padding:'0 28px',borderBottom:`1px solid ${C.border}`}}>
        {[['overview','⊞ Genel Bakış'],['positions','⟁ Pozisyonlar'],['trades','≡ Geçmiş']].map(([id,label])=>(
          <button key={id} onClick={()=>setTab(id)} style={{
            background:'none',border:'none',padding:'12px 20px',
            color:tab===id?C.accent:C.dim,
            borderBottom:tab===id?`2px solid ${C.accent}`:'2px solid transparent',
            cursor:'pointer',fontSize:12,fontWeight:700,letterSpacing:'0.05em',
          }}>{label}</button>
        ))}
      </div>

      <div style={{padding:'24px 28px',maxWidth:1400}}>

        {/* ── GENEL BAKIŞ ── */}
        {tab==='overview' && (
          <div style={{display:'flex',flexDirection:'column',gap:20}}>

            <div style={{display:'grid',gridTemplateColumns:'repeat(5,1fr)',gap:12}}>
              {[
                {label:'Equity',value:fmt.usd(equity),color:C.accent},
                {label:'Realized PnL',value:fmt.usd(realizedPnl),color:pnlColor(realizedPnl)},
                {label:'Unrealized PnL',value:fmt.usd(unrealized),color:pnlColor(unrealized)},
                {label:'Açık Pozisyon',value:positions.length,color:C.text},
                {label:'Win Rate',value:winRate==='—'?'—':`${winRate}%`,
                  color:winRate!=='—'&&Number(winRate)>=50?C.green:winRate==='—'?C.muted:C.yellow,
                  sub:`${closedTrades.length} kapalı işlem`},
              ].map(s=><Card key={s.label}><Stat {...s}/></Card>)}
            </div>

            <div style={{display:'grid',gridTemplateColumns:'2fr 1fr',gap:20}}>
              <Card>
                <CardHeader title="Equity Seyri" badge={
                  <Badge label={`$${equity?.toFixed(2)}`} color={equity>=1000?C.green:C.red}/>
                }/>
                <div style={{padding:'16px 8px 8px'}}>
                  {equityHistory.length<2
                    ? <div style={{height:180,display:'flex',alignItems:'center',justifyContent:'center',color:C.dim}}>Veri biriktirilyor...</div>
                    : <ResponsiveContainer width="100%" height={180}>
                        <AreaChart data={equityHistory}>
                          <defs>
                            <linearGradient id="eq" x1="0" y1="0" x2="0" y2="1">
                              <stop offset="5%" stopColor={C.accent} stopOpacity={0.3}/>
                              <stop offset="95%" stopColor={C.accent} stopOpacity={0}/>
                            </linearGradient>
                          </defs>
                          <XAxis dataKey="t" hide/>
                          <YAxis domain={['auto','auto']} hide/>
                          <Tooltip contentStyle={{background:C.surface,border:`1px solid ${C.border}`,borderRadius:8,fontSize:11}}
                            formatter={(v)=>[`$${v.toFixed(4)}`,'Equity']} labelFormatter={()=>''}/>
                          <Area type="monotone" dataKey="v" stroke={C.accent} fill="url(#eq)" strokeWidth={2} dot={false}/>
                        </AreaChart>
                      </ResponsiveContainer>
                  }
                </div>
              </Card>

              <Card>
                <CardHeader title="Regime Dağılımı" badge={
                  <Badge label={`son ${botState.regime_distribution_last_n?.n||0}`} color={C.muted}/>
                }/>
                <div style={{padding:'8px',display:'flex',flexDirection:'column',alignItems:'center'}}>
                  {regimePie.length===0
                    ? <div style={{height:160,display:'flex',alignItems:'center',color:C.dim}}>Veri yok</div>
                    : <>
                        <ResponsiveContainer width="100%" height={140}>
                          <PieChart>
                            <Pie data={regimePie} dataKey="value" cx="50%" cy="50%" innerRadius={40} outerRadius={60} paddingAngle={3}>
                              {regimePie.map(e=><Cell key={e.name} fill={regimeColor(e.name)}/>)}
                            </Pie>
                            <Tooltip contentStyle={{background:C.surface,border:`1px solid ${C.border}`,borderRadius:8,fontSize:11}}
                              formatter={(v,n,p)=>[`${p.payload.pct}%`,p.payload.name]}/>
                          </PieChart>
                        </ResponsiveContainer>
                        <div style={{display:'flex',gap:12,flexWrap:'wrap',justifyContent:'center',marginTop:4}}>
                          {regimePie.map(r=>(
                            <div key={r.name} style={{display:'flex',alignItems:'center',gap:4,fontSize:11}}>
                              <Dot color={regimeColor(r.name)}/>
                              <span style={{color:C.dim}}>{r.name}</span>
                              <span style={{color:regimeColor(r.name),fontWeight:700}}>{r.pct}%</span>
                            </div>
                          ))}
                        </div>
                      </>
                  }
                </div>
              </Card>
            </div>

            <Card>
              <CardHeader title="Aktif Grid Pozisyonları"
                badge={<Badge label={positions.length} color={positions.length>0?C.accent:C.muted}/>}/>
              {positions.length===0
                ? <div style={{padding:'32px',textAlign:'center',color:C.dim}}>Açık pozisyon yok</div>
                : <div style={{padding:'12px 16px',display:'flex',flexWrap:'wrap',gap:12}}>
                    {positions.map((pos,i)=>{
                      const pnlPct = pos.entry_price>0 ? ((pos.current_price-pos.entry_price)/pos.entry_price*100) : 0;
                      return (
                        <div key={i} style={{background:C.bg,border:`1px solid ${C.border}`,
                          borderRadius:10,padding:'12px 16px',minWidth:200,flex:'1 1 200px'}}>
                          <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:8}}>
                            <span style={{fontWeight:800,color:C.text}}>{pos.symbol}</span>
                            <Badge label={`${pnlPct>=0?'+':''}${pnlPct.toFixed(2)}%`} color={pnlColor(pnlPct)}/>
                          </div>
                          <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:'4px 12px',fontSize:11}}>
                            {[['Giriş',fmt.price(pos.entry_price)],['Güncel',fmt.price(pos.current_price)],
                              ['Miktar',fmt.qty(pos.entry_amount)],['PnL',fmt.usd(pos.unrealized_pnl)],
                              ['Yaş',fmt.age(pos.entry_time)]].map(([k,v])=>(
                              <><span style={{color:C.dim}}>{k}</span>
                              <span style={{textAlign:'right',color:k==='PnL'?pnlColor(pos.unrealized_pnl):C.text}}>{v}</span></>
                            ))}
                          </div>
                          <div style={{marginTop:10}}>
                            <Btn label="Kapat" onClick={()=>forceClose(pos.symbol)} danger small/>
                          </div>
                        </div>
                      );
                    })}
                  </div>
              }
            </Card>

            <Card>
              <CardHeader title="Son İşlemler" right={<Btn label="Tümünü Gör →" onClick={()=>setTab('trades')} small/>}/>
              <table style={{width:'100%',borderCollapse:'collapse'}}>
                <thead><tr style={{borderBottom:`1px solid ${C.border}`}}>
                  {['Zaman','Coin','Yön','Fiyat','Miktar','PnL','Neden'].map(h=><th key={h} style={thStyle}>{h}</th>)}
                </tr></thead>
                <tbody>{trades.slice(0,5).map((t,i)=>(
                  <tr key={i} style={{borderBottom:`1px solid ${C.border}22`}}>
                    <td style={{...tdStyle,color:C.dim,fontSize:11}}>{fmt.time(t.timestamp)}</td>
                    <td style={{...tdStyle,fontWeight:700}}>{t.symbol}</td>
                    <td style={tdStyle}><Badge label={t.side?.toUpperCase()} color={['BUY','buy'].includes(t.side)?C.green:C.red}/></td>
                    <td style={tdStyle}>{fmt.price(t.price)}</td>
                    <td style={{...tdStyle,color:C.dim}}>{fmt.qty(t.amount)}</td>
                    <td style={{...tdStyle,color:t.realized_pnl!=null?pnlColor(t.realized_pnl):C.muted}}>
                      {t.realized_pnl!=null?fmt.usd(t.realized_pnl):'—'}
                    </td>
                    <td style={{...tdStyle,color:C.dim,fontSize:11,maxWidth:180,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>
                      {t.reason||t.entry_reason||'—'}
                    </td>
                  </tr>
                ))}</tbody>
              </table>
            </Card>
          </div>
        )}

        {/* ── POZİSYONLAR ── */}
        {tab==='positions' && (
          <Card>
            <CardHeader title="Tüm Açık Pozisyonlar"
              badge={<Badge label={positions.length} color={C.accent}/>}
              right={<Btn label="↻ Yenile" onClick={fetchAll} small/>}/>
            <table style={{width:'100%',borderCollapse:'collapse'}}>
              <thead><tr style={{borderBottom:`1px solid ${C.border}`}}>
                {['Coin','Giriş','Güncel','Miktar','Unrealized PnL','PnL %','Yaş','Stop Loss','Take Profit','İşlem'].map(h=><th key={h} style={thStyle}>{h}</th>)}
              </tr></thead>
              <tbody>
                {positions.length===0 && <tr><td colSpan={10} style={{padding:'32px',textAlign:'center',color:C.dim}}>Açık pozisyon yok</td></tr>}
                {positions.map((pos,i)=>{
                  const pct = pos.entry_price>0 ? ((pos.current_price-pos.entry_price)/pos.entry_price*100) : 0;
                  return <tr key={i} style={{borderBottom:`1px solid ${C.border}22`}}>
                    <td style={{...tdStyle,fontWeight:800,color:C.accent}}>{pos.symbol}</td>
                    <td style={tdStyle}>{fmt.price(pos.entry_price)}</td>
                    <td style={{...tdStyle,color:pnlColor(pct)}}>{fmt.price(pos.current_price)}</td>
                    <td style={{...tdStyle,color:C.dim}}>{fmt.qty(pos.entry_amount)}</td>
                    <td style={{...tdStyle,color:pnlColor(pos.unrealized_pnl)}}>{fmt.usd(pos.unrealized_pnl)}</td>
                    <td style={tdStyle}><Badge label={`${pct>=0?'+':''}${pct.toFixed(3)}%`} color={pnlColor(pct)}/></td>
                    <td style={{...tdStyle,color:C.dim}}>{fmt.age(pos.entry_time)}</td>
                    <td style={{...tdStyle,color:C.red,fontSize:11}}>{pos.stop_loss?fmt.price(pos.stop_loss):'—'}</td>
                    <td style={{...tdStyle,color:C.green,fontSize:11}}>{pos.take_profit?fmt.price(pos.take_profit):'—'}</td>
                    <td style={tdStyle}><Btn label="✕ Kapat" onClick={()=>forceClose(pos.symbol)} danger small/></td>
                  </tr>;
                })}
              </tbody>
            </table>
          </Card>
        )}

        {/* ── İŞLEM GEÇMİŞİ ── */}
        {tab==='trades' && (
          <div style={{display:'flex',flexDirection:'column',gap:20}}>
            <div style={{display:'grid',gridTemplateColumns:'repeat(4,1fr)',gap:12}}>
              {[
                {label:'Toplam İşlem',value:trades.length},
                {label:'Kârlı Satış',value:winTrades.length,color:C.green},
                {label:'Zararlı Satış',value:closedTrades.length-winTrades.length,color:C.red},
                {label:'Win Rate',value:winRate==='—'?'—':`${winRate}%`,
                  color:winRate!=='—'&&Number(winRate)>=50?C.green:C.yellow},
              ].map(s=><Card key={s.label}><Stat {...s}/></Card>)}
            </div>

            {closedTrades.length>0 && (
              <Card>
                <CardHeader title="Realized PnL — Kapalı İşlemler"/>
                <div style={{padding:'16px 8px 8px'}}>
                  <ResponsiveContainer width="100%" height={160}>
                    <BarChart data={closedTrades.slice(-20).map((t,i)=>({
                      i:i+1, pnl:+(t.realized_pnl||0).toFixed(4),
                      sym:t.symbol?.replace('/USDT',''),
                    }))}>
                      <XAxis dataKey="sym" tick={{fontSize:10,fill:C.dim}}/>
                      <YAxis tick={{fontSize:10,fill:C.dim}}/>
                      <Tooltip contentStyle={{background:C.surface,border:`1px solid ${C.border}`,borderRadius:8,fontSize:11}}
                        formatter={(v)=>[`$${v}`,'PnL']}/>
                      <Bar dataKey="pnl" radius={[4,4,0,0]}>
                        {closedTrades.slice(-20).map((t,i)=>(
                          <Cell key={i} fill={(t.realized_pnl||0)>=0?C.green:C.red}/>
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </Card>
            )}

            <Card>
              <CardHeader title="İşlem Geçmişi" badge={<Badge label={`${trades.length} kayıt`} color={C.muted}/>}/>
              <table style={{width:'100%',borderCollapse:'collapse'}}>
                <thead><tr style={{borderBottom:`1px solid ${C.border}`}}>
                  {['#','Zaman','Coin','Yön','Fiyat','Miktar','Real. PnL','Brüt PnL','Komis.','Neden'].map(h=><th key={h} style={thStyle}>{h}</th>)}
                </tr></thead>
                <tbody>{trades.map((t,i)=>(
                  <tr key={i} style={{borderBottom:`1px solid ${C.border}22`,background:i%2===0?'transparent':`${C.surface}55`}}>
                    <td style={{...tdStyle,color:C.muted,fontSize:11}}>{trades.length-i}</td>
                    <td style={{...tdStyle,color:C.dim,fontSize:11}}>{fmt.time(t.timestamp)}</td>
                    <td style={{...tdStyle,fontWeight:700}}>{t.symbol}</td>
                    <td style={tdStyle}><Badge label={t.side?.toUpperCase()} color={['BUY','buy'].includes(t.side)?C.green:C.red}/></td>
                    <td style={tdStyle}>{fmt.price(t.price)}</td>
                    <td style={{...tdStyle,color:C.dim}}>{fmt.qty(t.amount)}</td>
                    <td style={{...tdStyle,color:t.realized_pnl!=null?pnlColor(t.realized_pnl):C.muted}}>
                      {t.realized_pnl!=null?fmt.usd(t.realized_pnl):'—'}
                    </td>
                    <td style={{...tdStyle,color:C.dim,fontSize:11}}>{t.gross_pnl!=null?fmt.usd(t.gross_pnl):'—'}</td>
                    <td style={{...tdStyle,color:C.red,fontSize:11}}>{t.fee!=null?fmt.usd(t.fee):'—'}</td>
                    <td style={{...tdStyle,color:C.dim,fontSize:11,maxWidth:200,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>
                      {t.reason||t.entry_reason||'—'}
                    </td>
                  </tr>
                ))}</tbody>
              </table>
            </Card>
          </div>
        )}
      </div>

      <div style={{borderTop:`1px solid ${C.border}`,padding:'10px 28px',
        display:'flex',justifyContent:'space-between',alignItems:'center',color:C.muted,fontSize:11}}>
        <span>Grid Bot Dashboard v2.0 — Paper Trading</span>
        <span>API: {API} · Risk: {botState.risk_multiplier?.toFixed(2)??'—'}x</span>
      </div>
    </div>
  );
}
