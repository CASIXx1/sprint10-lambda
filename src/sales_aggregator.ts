import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { parse } from "csv-parse/sync";
import { Handler } from "aws-lambda";

import { AggregatedResult, ProductSales } from "./types";

let s3Client: S3Client | null = null;

const getS3Client = (): S3Client => {
    if (!s3Client) {
        s3Client = new S3Client({});
    }
    return s3Client;
};

interface SalesRow {
    date: string;
    product_id: string;
    product_name: string;
    category: string;
    quantity: number;
    unit_price: number;
    total_price: number;
}

const readSalesCsvFromS3 = async (bucket: string, key: string): Promise<SalesRow[]> => {
    const command = new GetObjectCommand({
        Bucket: bucket,
        Key: key,
    });
    const response = await getS3Client().send(command);
    const body = await response.Body?.transformToString("utf-8");
    if (!body) {
        throw new Error("S3 objectが空です");
    }

    return parse(body, {
        columns: true,
        skip_empty_lines: true,
        cast: true,
    });
};

const aggregateSales = (rows: SalesRow[]): AggregatedResult => {
    const salesByCategory: Record<string, number> = {};
    const salesByDate: Record<string, number> = {};
    const salesByProduct: Record<string, number> = {};
    let grandTotal = 0;

    for (const row of rows) {
        const date = row.date;
        const category = row.category;
        const productName = row.product_name;
        const totalPrice = row.total_price;

        salesByCategory[category] = (salesByCategory[category] || 0) + totalPrice;
        salesByDate[date] = (salesByDate[date] || 0) + totalPrice;
        
        salesByProduct[productName] = (salesByProduct[productName] || 0) + totalPrice;
        
        grandTotal += totalPrice;
    }

    const sortObjectByKey = (obj: Record<string, number>) => {
        return Object.fromEntries(
            Object.entries(obj).sort(([a], [b]) => a.localeCompare(b))
        );
    };

    const topProducts: ProductSales[] = Object.entries(salesByProduct)
        .map(([name, total_sales]) => ({
            product_name: name,
            total_sales: total_sales,
        }))
        .sort((a, b) => b.total_sales - a.total_sales)
        .slice(0, 3);

    return {
        sales_by_category: sortObjectByKey(salesByCategory),
        sales_by_date: sortObjectByKey(salesByDate),
        top_products: topProducts,
        grand_total: grandTotal,
    };
};

export const handler: Handler = async (event) => {
    const bucket = process.env.SALES_BUCKET;
    const key = process.env.SALES_KEY || "sales_202401.csv";

    if (!bucket) {
        throw new Error("環境変数 SALES_BUCKET が必要です");
    }

    const rows = await readSalesCsvFromS3(bucket, key);
    const aggregatedSales = aggregateSales(rows);

    return {
        bucket,
        key,
        record_count: rows.length,
        aggregated: aggregatedSales,
    };
};
