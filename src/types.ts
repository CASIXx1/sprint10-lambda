export interface ProductSales {
    product_name: string;
    total_sales: number;
}

export interface AggregatedResult {
    sales_by_category: Record<string, number>;
    sales_by_date: Record<string, number>;
    top_products: ProductSales[];
    grand_total: number;
}
