import { findMarketBySlug, getChartRecord, listMarkets } from "../db.mjs";
import { corsPreflight, fail, ok } from "../http.mjs";

export function options(request) {
  return corsPreflight(request);
}

export async function list(request) {
  return ok(await listMarkets(), request);
}

export async function detail(request, params) {
  const market = await findMarketBySlug(params.slug);
  if (!market) {
    return fail("Market not found", 404, request);
  }
  return ok(market, request);
}

export async function chart(request, params) {
  const chartType = new URL(request.url).searchParams.get("type") === "panna" ? "panna" : "jodi";
  const chart = await getChartRecord(params.slug, chartType);
  if (!chart) {
    return fail("Chart not found", 404, request);
  }
  return ok(chart, request);
}
