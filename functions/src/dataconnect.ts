import {getDataConnect} from "firebase-admin/data-connect";

export interface MarketStatusRow {
  ticker_count?: number | string | null;
  history_rows?: number | string | null;
  latest_date?: string | null;
}

interface MarketStatusResponse {
  status?: MarketStatusRow | null;
}

function enabled(): boolean {
  return ["1", "true", "yes"].includes(String(process.env.MONEYMAKER_USE_DATA_CONNECT ?? "false").toLowerCase());
}

function location(): string {
  return String(process.env.MONEYMAKER_DATACONNECT_LOCATION ?? "australia-southeast1");
}

function serviceId(): string {
  return String(process.env.MONEYMAKER_DATACONNECT_SERVICE ?? "moneymaker");
}

export async function readMarketStatusViaDataConnect(market: string): Promise<MarketStatusRow | null> {
  if (!enabled()) return null;
  const dc = getDataConnect({location: location(), serviceId: serviceId()});
  const response = await dc.executeGraphqlRead<MarketStatusResponse, {market: string}>(
    `
    query GetMarketStatus($market: String!) {
      status: _selectFirst(
        sql: """
          SELECT COUNT(DISTINCT ticker) AS ticker_count,
                 COUNT(*) AS history_rows,
                 MAX(price_date)::text AS latest_date
          FROM price_history
          WHERE market = $1
        """,
        params: [$market]
      )
    }
    `,
    {variables: {market}}
  );
  return response.data.status ?? null;
}
