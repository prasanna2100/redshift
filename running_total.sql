# Query to Calculate Running Total with Self-Join

SELECT 
  s1.sale_id,
  s1.sale_date,
  s1.sales_amount,
  SUM(s2.sales_amount) AS running_total
FROM sales s1
JOIN sales s2
  ON s2.sale_date <= s1.sale_date
GROUP BY s1.sale_id, s1.sale_date, s1.sales_amount
ORDER BY s1.sale_date;

SELECT 
  sale_id,
  sale_date,
  sales_amount,
  SUM(sales_amount) OVER (ORDER BY sale_date) AS running_total
FROM sales
ORDER BY sale_date;
