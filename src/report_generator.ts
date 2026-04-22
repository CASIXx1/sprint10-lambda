import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { Handler } from "aws-lambda";
import { AggregatedResult } from "./types";

let s3Client: S3Client | null = null;

const getS3Client = (): S3Client => {
    if (!s3Client) {
        s3Client = new S3Client({});
    }
    return s3Client;
};

const formatCurrency = (amount: number): string => {
    return new Intl.NumberFormat("ja-JP", { style: "currency", currency: "JPY" }).format(amount).replace("￥", "¥");
};

const formatDate = (date: Date): string => {
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, '0');
    const d = String(date.getDate()).padStart(2, '0');
    const h = String(date.getHours()).padStart(2, '0');
    const min = String(date.getMinutes()).padStart(2, '0');
    const s = String(date.getSeconds()).padStart(2, '0');
    return `${y}-${m}-${d} ${h}:${min}:${s}`;
};

export const handler: Handler = async (event: any) => {
    console.log("集計後のJSON:", JSON.stringify(event, null, 2));

    const data: AggregatedResult = event.aggregated || event;

    if (!data.sales_by_category) {
        throw new Error("集計データ (sales_by_category) がイベントに含まれていません");
    }

    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const dateStr = `${year}${month}${day}`;
    
    let report = "=====================================\n";
    report += "        月次売上レポート\n";
    report += `        対象期間: ${year}年${Number(month)}月\n`;
    report += "=====================================\n\n";

    report += "【総売上】\n";
    report += `  合計金額: ${formatCurrency(data.grand_total)}\n\n`;

    report += "【カテゴリ別売上】\n";
    for (const [category, total] of Object.entries(data.sales_by_category)) {
        report += `  ${category}: ${formatCurrency(total)}\n`;
    }
    report += "\n";

    report += "【日別売上】\n";
    for (const [date, total] of Object.entries(data.sales_by_date)) {
        report += `  ${date}: ${formatCurrency(total)}\n`;
    }
    report += "\n";

    report += "【トップ3商品】\n";
    for (let i = 0; i < data.top_products.length; i++) {
        if (i >= 3) break;
        const product = data.top_products[i];
        report += `${i + 1}. ${product.product_name}: ${formatCurrency(product.total_sales)}\n`;
    }
    report += "\n";

    report += "=====================================\n";
    report += `        レポート作成日時: ${formatDate(now)}\n`;
    report += "=====================================\n";

    const bucket = process.env.SALES_BUCKET;
    if (!bucket) {
        throw new Error("環境変数 SALES_BUCKET が必要です");
    }

    const key = `${dateStr}_report.txt`;

    const command = new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: report,
        ContentType: "text/plain; charset=utf-8",
    });

    await getS3Client().send(command);

    return {
        report_key: key,
        report_content: report
    };
};
