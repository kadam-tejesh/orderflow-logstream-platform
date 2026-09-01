import { useEffect, useMemo, useRef, useState } from "react";
import * as echarts from "echarts";

const API = {
  search: import.meta.env.VITE_SEARCH_API_URL || "http://localhost:8081/api/search",
  aggregate: import.meta.env.VITE_AGGREGATION_API_URL || "http://localhost:8081/api/analytics/log-volume",
  alerts: import.meta.env.VITE_ALERT_API_URL || "http://localhost:8082/api/alerts",
  liveTail: import.meta.env.VITE_LIVE_TAIL_URL || "ws://localhost:8083/ws/logs",
};

const mockLogs = [
  { timestamp:"16:00:21.412", level:"ERROR", service:"billing-api", message:"Payment authorization timed out", response_time:1842 },
  { timestamp:"16:00:20.947", level:"WARN", service:"orders-api", message:"Retrying downstream inventory request", response_time:821 },
  { timestamp:"16:00:20.204", level:"INFO", service:"auth-service", message:"User session refreshed successfully", response_time:83 },
  { timestamp:"16:00:19.882", level:"ERROR", service:"billing-api", message:"Database connection pool exhausted", response_time:2211 },
  { timestamp:"15:59:58.613", level:"INFO", service:"orders-api", message:"Order 84192 completed", response_time:129 },
  { timestamp:"15:59:54.301", level:"DEBUG", service:"gateway", message:"Upstream response received", response_time:41 },
  { timestamp:"15:59:52.994", level:"WARN", service:"search-api", message:"Slow Lucene query detected", response_time:1123 },
];

function App() {
  const [query, setQuery] = useState("level:ERROR");
  const [service, setService] = useState("All services");
  const [level, setLevel] = useState("All levels");
  const [logs, setLogs] = useState(mockLogs);
  const [alerts, setAlerts] = useState([]);
  const [tail, setTail] = useState([]);
  const [loading, setLoading] = useState(false);
  const [tailConnected, setTailConnected] = useState(false);
  const [notice, setNotice] = useState("");
  const chartRef = useRef(null);
  const wsRef = useRef(null);

  const filtered = useMemo(() => logs.filter(l =>
    (service === "All services" || l.service === service) &&
    (level === "All levels" || l.level === level)
  ), [logs, service, level]);

  async function runSearch() {
    setLoading(true); setNotice("");
    const params = new URLSearchParams();
    if (query.trim()) params.set("q", query.trim());
    if (service !== "All services") params.set("service", service);
    if (level !== "All levels") params.set("level", level);
    try {
      const response = await fetch(`${API.search}?${params.toString()}`);
      if (!response.ok) throw new Error(`Search API ${response.status}`);
      const data = await response.json();
      setLogs(data.results || data.logs || data || []);
      setNotice("Search completed");
    } catch {
      setNotice("Search API unavailable — showing demo data");
    } finally { setLoading(false); }
  }

  async function loadAlerts() {
    try {
      const response = await fetch(API.alerts);
      if (!response.ok) throw new Error();
      const data = await response.json();
      setAlerts(Array.isArray(data) ? data : data.alerts || []);
    } catch {
      setAlerts([
        { id:1, query:"level:ERROR AND service:billing-api", threshold:100, window:"5m", status:"ACTIVE" },
        { id:2, query:"response_time:>1000", threshold:20, window:"10m", status:"TRIGGERED" }
      ]);
    }
  }

  useEffect(() => { loadAlerts(); }, []);

  useEffect(() => {
    const chart = echarts.init(chartRef.current);
    const labels = Array.from({length:60}, (_,i)=>`${String(15+Math.floor(i/60)).padStart(2,'0')}:${String(i).padStart(2,'0')}`);
    const values = labels.map((_,i)=> 8 + Math.round(14*Math.abs(Math.sin(i/5))) + (i%7===0?18:0));
    chart.setOption({
      backgroundColor: "transparent",
      animationDuration: 500,
      grid: { left: 42, right: 18, top: 22, bottom: 34 },
      tooltip: { trigger:"axis", backgroundColor:"#111827", borderColor:"#25344a", textStyle:{color:"#e5edf7"} },
      xAxis: { type:"category", data:labels, boundaryGap:false, axisLabel:{color:"#71839a",fontSize:9,interval:9}, axisLine:{lineStyle:{color:"#24364d"}} },
      yAxis: { type:"value", splitLine:{lineStyle:{color:"rgba(120,140,165,.12)"}}, axisLabel:{color:"#71839a",fontSize:9} },
      series:[{ name:"Logs", type:"line", smooth:true, symbol:"none", data:values, lineStyle:{color:"#5ea9ff",width:2}, areaStyle:{color:"rgba(59,130,246,.12)"} }]
    });
    const onResize=()=>chart.resize(); window.addEventListener("resize",onResize);
    return ()=>{window.removeEventListener("resize",onResize);chart.dispose()};
  }, []);

  function connectTail() {
    if (wsRef.current) wsRef.current.close();
    try {
      const ws = new WebSocket(API.liveTail);
      wsRef.current = ws;
      ws.onopen=()=>{setTailConnected(true); setNotice("Live Tail connected")};
      ws.onmessage=e=>{
        try {
          const item=JSON.parse(e.data);
          setTail(p=>[item,...p].slice(0,100));
        } catch {
          setTail(p=>[{timestamp:new Date().toLocaleTimeString(),level:"INFO",service:"live-tail",message:e.data},...p].slice(0,100));
        }
      };
      ws.onerror=()=>setNotice("Live Tail unavailable — demo stream available");
      ws.onclose=()=>setTailConnected(false);
    } catch { setNotice("WebSocket unavailable"); }
  }

  function demoTail() {
    const levels=["INFO","INFO","WARN","ERROR"];
    const services=["billing-api","orders-api","gateway","auth-service"];
    const messages=["request completed","cache miss","slow downstream dependency","database timeout"];
    setTail(p=>[{
      timestamp:new Date().toLocaleTimeString(),
      level:levels[Math.floor(Math.random()*levels.length)],
      service:services[Math.floor(Math.random()*services.length)],
      message:messages[Math.floor(Math.random()*messages.length)]
    },...p].slice(0,100));
  }

  async function createAlert() {
    const body={ query: query || "level:ERROR", threshold:100, timeWindow:"5m" };
    try {
      const r=await fetch(API.alerts,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
      if (!r.ok) throw new Error();
      await loadAlerts(); setNotice("Alert rule created");
    } catch {
      setAlerts(p=>[{id:Date.now(),query:body.query,threshold:100,window:"5m",status:"ACTIVE"},...p]);
      setNotice("Alert added locally (Alert API unavailable)");
    }
  }

  const services=["All services","billing-api","orders-api","gateway","auth-service","search-api"];
  const levels=["All levels","ERROR","WARN","INFO","DEBUG"];

  return <div className="app">
    <header className="topbar">
      <div className="brand"><div className="logo">LS</div><div><div className="eyebrow">DISTRIBUTED OBSERVABILITY</div><h1>LogStream</h1><p>Analytics & Alerting Platform</p></div></div>
      <div className="top-actions"><span className="pill green">● API HEALTHY</span><span className={`pill ${tailConnected?"green":"gray"}`}>● LIVE TAIL {tailConnected?"ON":"OFF"}</span><button className="profile">NS</button></div>
    </header>

    <main>
      <section className="searchbar panel">
        <div className="search-main"><span className="search-icon">⌕</span><input value={query} onChange={e=>setQuery(e.target.value)} onKeyDown={e=>e.key==="Enter"&&runSearch()} placeholder="Search logs: level:ERROR AND service:billing-api"/></div>
        <select value={service} onChange={e=>setService(e.target.value)}>{services.map(s=><option key={s}>{s}</option>)}</select>
        <select value={level} onChange={e=>setLevel(e.target.value)}>{levels.map(s=><option key={s}>{s}</option>)}</select>
        <button className="primary" onClick={runSearch}>{loading?"Searching…":"Search"}</button>
      </section>

      {notice && <div className="notice">{notice}</div>}

      <section className="stats">
        <div className="stat panel"><span>Total logs</span><b>1.24M</b><small>last 60 min</small></div>
        <div className="stat panel"><span>Error rate</span><b className="danger">3.8%</b><small>+0.6% vs previous hour</small></div>
        <div className="stat panel"><span>Ingestion</span><b>10.0K/s</b><small>current throughput</small></div>
        <div className="stat panel"><span>Active alerts</span><b className="warn">{alerts.filter(a=>a.status==="ACTIVE").length}</b><small>rules configured</small></div>
      </section>

      <section className="grid-two">
        <div className="panel chart-panel"><div className="section-head"><div><span className="kicker">ANALYTICS</span><h2>Log volume over time</h2></div><select className="compact"><option>Last 60 minutes</option><option>Last 24 hours</option></select></div><div ref={chartRef} className="chart"/></div>
        <div className="panel service-panel"><div className="section-head"><div><span className="kicker">SERVICES</span><h2>Service health</h2></div></div>
          {["billing-api","orders-api","gateway","auth-service","search-api"].map((s,i)=><div className="service" key={s}><span className={`service-dot ${i===0?"bad":i===1?"warn-dot":"ok"}`}/><b>{s}</b><span className="service-spacer"/><span>{[97.2,99.8,99.9,100,98.7][i]}%</span><small>{[1842,821,41,83,1123][i]} ms p95</small></div>)}
        </div>
      </section>

      <section className="panel results"><div className="section-head"><div><span className="kicker">SEARCH RESULTS</span><h2>{filtered.length} matching log entries</h2></div><span className="muted">Page 1 · 50 rows</span></div>
        <div className="table"><div className="thead"><span>TIME</span><span>LEVEL</span><span>SERVICE</span><span>MESSAGE</span><span>RESPONSE</span></div>
        {filtered.map((l,i)=><div className="tr" key={i}><span>{l.timestamp}</span><span className={`level ${String(l.level).toLowerCase()}`}>{l.level}</span><span className="mono">{l.service}</span><span>{l.message}</span><span>{l.response_time ?? "—"} ms</span></div>)}</div>
      </section>

      <section className="grid-two">
        <div className="panel tail"><div className="section-head"><div><span className="kicker">LIVE TAIL</span><h2>Streaming logs</h2></div><div className="btns"><button className="ghost" onClick={connectTail}>Connect</button><button className="ghost" onClick={demoTail}>Demo event</button></div></div>
          <div className="terminal">{tail.length===0?<div className="empty">Connect to the Live Tail WebSocket or generate a demo event.</div>:tail.map((l,i)=><div className="line" key={i}><span>{l.timestamp}</span><span className={`level ${String(l.level||"INFO").toLowerCase()}`}>{l.level||"INFO"}</span><b>{l.service||"service"}</b><span>{l.message}</span></div>)}</div>
        </div>
        <div className="panel alerts"><div className="section-head"><div><span className="kicker">ALERTING</span><h2>Alert rules</h2></div><button className="primary small" onClick={createAlert}>+ New rule</button></div>
          {alerts.map(a=><div className="alert" key={a.id}><div><b>{a.query}</b><small>Threshold {a.threshold} · Window {a.window||a.timeWindow||"5m"}</small></div><span className={`status ${String(a.status).toLowerCase()}`}>{a.status}</span></div>)}
        </div>
      </section>
    </main>

    <footer>LogStream Analytics Dashboard <span>React · Apache ECharts · REST + WebSocket</span></footer>
  </div>
}
export default App;
