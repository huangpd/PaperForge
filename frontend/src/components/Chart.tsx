import { useEffect, useRef } from "react";
import * as echarts from "echarts/core";
import { BarChart, GraphChart, LineChart } from "echarts/charts";
import { GridComponent, LegendComponent, TooltipComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

echarts.use([BarChart, LineChart, GraphChart, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer]);

export const PALETTE = ["#2f6fed", "#12a594", "#f5a524", "#e5484d", "#8e4ec6", "#6e7b8b"];

export default function Chart({ option, height = 280, onClick }: { option: echarts.EChartsCoreOption; height?: number; onClick?: (p: { name: string; dataType?: string; data?: unknown }) => void }) {
  const el = useRef<HTMLDivElement>(null);
  const chart = useRef<echarts.ECharts>();
  useEffect(() => {
    chart.current = echarts.init(el.current!);
    const ro = new ResizeObserver(() => chart.current?.resize());
    ro.observe(el.current!);
    return () => { ro.disconnect(); chart.current?.dispose(); };
  }, []);
  useEffect(() => {
    chart.current?.setOption({ color: PALETTE, textStyle: { fontFamily: "inherit" }, ...option }, true);
    chart.current?.off("click");
    if (onClick) chart.current?.on("click", (p) => onClick(p as never));
  }, [option, onClick]);
  return <div ref={el} style={{ height, width: "100%" }} />;
}
