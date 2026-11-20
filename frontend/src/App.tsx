import { NavLink, Route, Routes } from "react-router-dom";
import Overview from "./pages/Overview";
import Pipeline from "./pages/Pipeline";
import Explorer from "./pages/Explorer";
import Quality from "./pages/Quality";
import Lineage from "./pages/Lineage";
import Catalog from "./pages/Catalog";
import { useLang, useT } from "./i18n";
import { tzLabel } from "./api";

const NAV = [["/", "Overview"], ["/pipeline", "Pipeline"], ["/explorer", "Data Explorer"], ["/quality", "Data Quality"], ["/lineage", "Data Lineage"], ["/catalog", "Data Catalog"]] as const;

export default function App() {
  const t = useT();
  const [lang, setLang] = useLang();
  return (
    <div className="shell">
      <aside className="side">
        <div className="brand"><span className="logo">P</span> PaperForge</div>
        <nav>{NAV.map(([to, label]) => <NavLink key={to} to={to} end={to === "/"} className={({ isActive }) => (isActive ? "on" : "")}>{t(label)}</NavLink>)}</nav>
        <div className="lang" role="group" aria-label={t("Language")}>
          <button className={lang === "en" ? "on" : ""} onClick={() => setLang("en")}>EN</button>
          <button className={lang === "zh" ? "on" : ""} onClick={() => setLang("zh")}>中文</button>
        </div>
        <div className="side-f">{t("Scholarly data engineering")}<br />Crossref · Kafka · PostgreSQL · dbt<br />{t("Times shown in")} {tzLabel()}</div>
      </aside>
      <main className="main">
        <Routes>
          <Route path="/" element={<Overview />} />
          <Route path="/pipeline" element={<Pipeline />} />
          <Route path="/explorer" element={<Explorer />} />
          <Route path="/quality" element={<Quality />} />
          <Route path="/lineage" element={<Lineage />} />
          <Route path="/catalog" element={<Catalog />} />
          <Route path="*" element={<div className="pad">{t("Not found")}</div>} />
        </Routes>
      </main>
    </div>
  );
}
